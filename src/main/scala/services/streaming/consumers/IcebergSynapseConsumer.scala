package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.consumers

import models.app.streaming.{SourceCleanupRequest, SourceCleanupResult}
import services.data_providers.microsoft_synapse_link.DataStreamElement
import services.streaming.consumers.DataStreamElementExtensions.given_MetadataEnrichedRowStreamElement_DataStreamElement

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.*
import com.sneaksanddata.arcane.framework.models.ArcaneSchema
import com.sneaksanddata.arcane.framework.services.app.base.StreamLifetimeService
import com.sneaksanddata.arcane.framework.services.base.SchemaProvider
import com.sneaksanddata.arcane.framework.services.consumers.{MergeableBatch, StagedVersionedBatch}
import com.sneaksanddata.arcane.framework.services.streaming.base.*
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.{DisposeBatchProcessor, MergeBatchProcessor}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.StagingProcessor
import zio.stream.{ZPipeline, ZSink}
import zio.{Chunk, Schedule, ZIO, ZLayer}

import java.time.Duration

type IncomingBatch = Chunk[DataStreamElement]
type InFlightBatch = ((Iterable[StagedVersionedBatch], Seq[SourceCleanupRequest]), Long)

class IcebergSynapseConsumer(stagingProcessor: StagingProcessor,
                             mergeProcessor: MergeBatchProcessor,
                             disposeBatchProcessor: DisposeBatchProcessor,
                             streamLifetimeService: StreamLifetimeService)
  extends BatchConsumer[IncomingBatch]:


  private val retryPolicy = Schedule.exponential(Duration.ofSeconds(1)) && Schedule.recurs(10)
  /**
   * Returns the sink that consumes the batch.
   *
   * @return ZSink (stream sink for the stream graph).
   */
  override def consume: ZSink[Any, Throwable, IncomingBatch, Any, Unit] =
    stagingProcessor.process(toInFlightBatch).filter(_.groupedBySchema.nonEmpty)  >>> mergeProcessor.process >>> disposeBatchProcessor.process >>> lifetimeGuard >>> logResults

  private def lifetimeGuard: ZPipeline[Any, Throwable, MergeBatchProcessor#BatchType, MergeBatchProcessor#BatchType] =
    ZPipeline.takeUntil(_ => streamLifetimeService.cancelled)

  private def logResults: ZSink[Any, Throwable, MergeBatchProcessor#BatchType, Any, Unit] = ZSink.foreach(result => zlog(s"Processing completed: $result"))

  def toInFlightBatch(batches: Iterable[StagedVersionedBatch & MergeableBatch], index: Long, others: Any): MergeBatchProcessor#BatchType =
    new IndexedStagedBatchesImpl(batches, index)

object IcebergSynapseConsumer:

  def apply(stagingProcessor: StagingProcessor,
            mergeProcessor: MergeBatchProcessor,
            disposeBatchProcessor: DisposeBatchProcessor,
            streamLifetimeService: StreamLifetimeService): IcebergSynapseConsumer =
    new IcebergSynapseConsumer(stagingProcessor, mergeProcessor, disposeBatchProcessor, streamLifetimeService)

  /**
   * The required environment for the IcebergConsumer.
   */
  type Environment = SchemaProvider[ArcaneSchema]
    & StagingProcessor
    & MergeBatchProcessor
    & StreamLifetimeService
    & DisposeBatchProcessor

  /**
   * The ZLayer that creates the IcebergConsumer.
   */
  val layer: ZLayer[Environment, Nothing, IcebergSynapseConsumer] =
    ZLayer {
      for
        stageProcessor <- ZIO.service[StagingProcessor]
        mergeProcessor <- ZIO.service[MergeBatchProcessor]
        streamLifetimeService <- ZIO.service[StreamLifetimeService]
        disposeBatchProcessor <- ZIO.service[DisposeBatchProcessor]
      yield IcebergSynapseConsumer(stageProcessor, mergeProcessor, disposeBatchProcessor, streamLifetimeService)
    }
