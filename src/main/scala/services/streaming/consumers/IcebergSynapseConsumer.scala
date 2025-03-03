package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.consumers

import models.app.MicrosoftSynapseLinkStreamContext
import models.app.streaming.{SourceCleanupRequest, SourceCleanupResult}
import services.data_providers.microsoft_synapse_link.DataStreamElement
import services.streaming.consumers.DataStreamElementExtensions.given_MetadataEnrichedRowStreamElement_DataStreamElement

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.*
import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings.{OptimizeSettings, OrphanFilesExpirationSettings, SnapshotExpirationSettings}
import com.sneaksanddata.arcane.framework.models.{ArcaneSchema, DataRow, MergeKeyField}
import com.sneaksanddata.arcane.framework.services.app.base.StreamLifetimeService
import com.sneaksanddata.arcane.framework.services.base.{BatchArchivationResult, SchemaProvider}
import com.sneaksanddata.arcane.framework.services.consumers.{ArchiveableBatch, MergeableBatch, StagedVersionedBatch, SynapseLinkMergeBatch}
import com.sneaksanddata.arcane.framework.services.lakehouse.base.IcebergCatalogSettings
import com.sneaksanddata.arcane.framework.services.lakehouse.given_Conversion_ArcaneSchema_Schema
import com.sneaksanddata.arcane.framework.services.merging.models.{JdbcOptimizationRequest, JdbcOrphanFilesExpirationRequest, JdbcSnapshotExpirationRequest}
import com.sneaksanddata.arcane.framework.services.streaming.base.*
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.{ArchivationProcessor, MergeBatchProcessor}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.{IndexedStagedBatches, StagingProcessor}
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import org.apache.zookeeper.proto.DeleteRequest
import zio.stream.{ZPipeline, ZSink}
import zio.{Chunk, Schedule, Task, ZIO, ZLayer}

import java.time.format.DateTimeFormatter
import java.time.{Duration, ZoneOffset, ZonedDateTime}
import java.util.UUID

type IncomingBatch = Chunk[DataStreamElement]
type InFlightBatch = ((Iterable[StagedVersionedBatch], Seq[SourceCleanupRequest]), Long)
type CompletedBatch = (Iterable[BatchArchivationResult], Seq[SourceCleanupRequest])
type PipelineResult = (Iterable[BatchArchivationResult], Seq[SourceCleanupResult])

class IcebergSynapseConsumer(stagingProcessor: StagingProcessor,
                             mergeProcessor: MergeBatchProcessor,
                             archivationProcessor: ArchivationProcessor,
                             streamLifetimeService: StreamLifetimeService)
  extends BatchConsumer[IncomingBatch]:


  private val retryPolicy = Schedule.exponential(Duration.ofSeconds(1)) && Schedule.recurs(10)
  /**
   * Returns the sink that consumes the batch.
   *
   * @return ZSink (stream sink for the stream graph).
   */
  override def consume: ZSink[Any, Throwable, IncomingBatch, Any, Unit] =
    stagingProcessor.process(toInFlightBatch) >>> mergeProcessor.process >>> archivationProcessor.process >>>  lifetimeGuard >>> logResults

  private def lifetimeGuard: ZPipeline[Any, Throwable, ArchivationProcessor#BatchType, ArchivationProcessor#BatchType] =
    ZPipeline.takeUntil(_ => streamLifetimeService.cancelled)

  private def logResults: ZSink[Any, Throwable, ArchivationProcessor#BatchType, Any, Unit] = ZSink.foreach(result => zlog(s"Processing completed: $result"))

  def toInFlightBatch(batches: Iterable[StagedVersionedBatch & MergeableBatch & ArchiveableBatch], index: Long, others: Any): MergeBatchProcessor#BatchType =
    new IndexedStagedBatchesImpl(batches, index)

object IcebergSynapseConsumer:

  def apply(stagingProcessor: StagingProcessor,
            mergeProcessor: MergeBatchProcessor,
            archivationProcessor: ArchivationProcessor,
            streamLifetimeService: StreamLifetimeService): IcebergSynapseConsumer =
    new IcebergSynapseConsumer(stagingProcessor, mergeProcessor, archivationProcessor, streamLifetimeService)

  /**
   * The required environment for the IcebergConsumer.
   */
  type Environment = SchemaProvider[ArcaneSchema]
    & StagingProcessor
    & MergeBatchProcessor
    & ArchivationProcessor
    & StreamLifetimeService

  /**
   * The ZLayer that creates the IcebergConsumer.
   */
  val layer: ZLayer[Environment, Nothing, IcebergSynapseConsumer] =
    ZLayer {
      for
        stageProcessor <- ZIO.service[StagingProcessor]
        mergeProcessor <- ZIO.service[MergeBatchProcessor]
        archivationProcessor <- ZIO.service[ArchivationProcessor]
        streamLifetimeService <- ZIO.service[StreamLifetimeService]
      yield IcebergSynapseConsumer(stageProcessor, mergeProcessor, archivationProcessor, streamLifetimeService)
    }
