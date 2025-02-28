package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.consumers

import models.app.streaming.{SourceCleanupRequest, SourceCleanupResult}
import models.app.{MicrosoftSynapseLinkStreamContext, TargetTableSettings}
import services.clients.BatchArchivationResult
import services.data_providers.microsoft_synapse_link.DataStreamElement

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.*
import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.{ArcaneSchema, DataRow, MergeKeyField}
import com.sneaksanddata.arcane.framework.services.app.base.StreamLifetimeService
import com.sneaksanddata.arcane.framework.services.base.SchemaProvider
import com.sneaksanddata.arcane.framework.services.consumers.{StagedVersionedBatch, SynapseLinkMergeBatch}
import com.sneaksanddata.arcane.framework.services.lakehouse.base.IcebergCatalogSettings
import com.sneaksanddata.arcane.framework.services.lakehouse.{CatalogWriter, given_Conversion_ArcaneSchema_Schema}
import com.sneaksanddata.arcane.framework.services.streaming.base.{BatchConsumer, BatchProcessor}
import com.sneaksanddata.arcane.framework.services.streaming.consumers.{IcebergStreamingConsumer, StreamingConsumer}
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import org.apache.zookeeper.proto.DeleteRequest
import zio.stream.{ZPipeline, ZSink}
import zio.{Chunk, Schedule, Task, ZIO, ZLayer}

import java.time.format.DateTimeFormatter
import java.time.{Duration, ZoneOffset, ZonedDateTime}
import java.util.UUID

type IncomingBatch = (Chunk[DataStreamElement], String)
type InFlightBatch = ((Iterable[StagedVersionedBatch], Seq[SourceCleanupRequest]), Long)
type CompletedBatch = (Iterable[BatchArchivationResult], Seq[SourceCleanupRequest])
type PipelineResult = (Iterable[BatchArchivationResult], Seq[SourceCleanupResult])

class IcebergSynapseConsumer(stageProcessor: BatchProcessor[IncomingBatch, InFlightBatch],
                             mergeProcessor: BatchProcessor[InFlightBatch, InFlightBatch],
                             archivationProcessor: BatchProcessor[InFlightBatch, CompletedBatch],
                             streamLifetimeService: StreamLifetimeService,
                             sourceCleanupProcessor: BatchProcessor[CompletedBatch, PipelineResult])
  extends BatchConsumer[IncomingBatch]:


  private val retryPolicy = Schedule.exponential(Duration.ofSeconds(1)) && Schedule.recurs(10)
  /**
   * Returns the sink that consumes the batch.
   *
   * @return ZSink (stream sink for the stream graph).
   */
  override def consume: ZSink[Any, Throwable, IncomingBatch, Any, Unit] =
    stageProcessor.process >>> mergeProcessor.process >>> archivationProcessor.process >>> sourceCleanupProcessor.process >>> lifetimeGuard >>> logResults

  private def lifetimeGuard: ZPipeline[Any, Throwable, PipelineResult, PipelineResult] = ZPipeline.takeUntil(_ => streamLifetimeService.cancelled)

  private def logResults: ZSink[Any, Throwable, PipelineResult, Any, Unit] = ZSink.foreach {
    case (arch, results) =>
      zlog(s"Processing completed: $arch") *>
        ZIO.foreach(results)(src => ZIO.log(s"Marked prefix for deletion: ${src.blobName} with marker ${src.deleteMarker}"))
  }


object IcebergSynapseConsumer:

  def apply(stageProcessor: BatchProcessor[IncomingBatch, InFlightBatch],
            mergeProcessor: BatchProcessor[InFlightBatch, InFlightBatch],
            archivationProcessor: BatchProcessor[InFlightBatch, CompletedBatch],
            streamLifetimeService: StreamLifetimeService,
            sourceCleanupProcessor: BatchProcessor[CompletedBatch, PipelineResult]): IcebergSynapseConsumer =
    new IcebergSynapseConsumer(stageProcessor, mergeProcessor, archivationProcessor, streamLifetimeService, sourceCleanupProcessor)

  /**
   * The required environment for the IcebergConsumer.
   */
  type Environment = SchemaProvider[ArcaneSchema]
    & BatchProcessor[IncomingBatch, InFlightBatch]
    & BatchProcessor[InFlightBatch, InFlightBatch]
    & BatchProcessor[InFlightBatch, CompletedBatch]
    & BatchProcessor[CompletedBatch, PipelineResult]
    & StreamLifetimeService

  /**
   * The ZLayer that creates the IcebergConsumer.
   */
  val layer: ZLayer[Environment, Nothing, IcebergSynapseConsumer] =
    ZLayer {
      for
        stageProcessor <- ZIO.service[BatchProcessor[IncomingBatch, InFlightBatch]]
        mergeProcessor <- ZIO.service[BatchProcessor[InFlightBatch, InFlightBatch]]
        archivationProcessor <- ZIO.service[BatchProcessor[InFlightBatch, CompletedBatch]]
        sourceCleanupProcessor <- ZIO.service[BatchProcessor[CompletedBatch, PipelineResult]]
        streamLifetimeService <- ZIO.service[StreamLifetimeService]
      yield IcebergSynapseConsumer(stageProcessor, mergeProcessor, archivationProcessor, streamLifetimeService, sourceCleanupProcessor)
    }
