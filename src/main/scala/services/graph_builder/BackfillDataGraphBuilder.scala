package com.sneaksanddata.arcane.microsoft_synapse_link
package services.graph_builder

import com.sneaksanddata.arcane.framework.models.DataRow
import com.sneaksanddata.arcane.framework.services.app.base.StreamLifetimeService
import com.sneaksanddata.arcane.framework.services.streaming.base.{BackfillDataProvider, BatchProcessor, StreamGraphBuilder}
import com.sneaksanddata.arcane.framework.services.streaming.consumers.BackfillConsumer
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.*
import com.sneaksanddata.arcane.microsoft_synapse_link.services.data_providers.microsoft_synapse_link.DataStreamElement
import com.sneaksanddata.arcane.microsoft_synapse_link.services.streaming.consumers.IcebergSynapseConsumer
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, ZIO, ZLayer}

class BackfillDataGraphBuilder(backfillDataProvider: BackfillDataProvider,
                               streamLifetimeService: StreamLifetimeService,
                               batchProcessor: BatchProcessor[DataStreamElement, Chunk[DataStreamElement]],
                               batchConsumer: IcebergSynapseConsumer)
  extends StreamGraphBuilder:


  override type StreamElementType = Chunk[DataStreamElement]

  override def create: ZStream[Any, Throwable, StreamElementType] =
    ZStream.fromZIO(backfillDataProvider.requestBackfill)
      .takeUntil(_ => streamLifetimeService.cancelled)
      .flatMap(batch => ZStream.fromIterable(batch.read))
      .via(batchProcessor.process)

  override def consume: ZSink[Any, Throwable, StreamElementType, Any, Unit] = batchConsumer.consume

/**
 * The companion object for the VersionedDataGraphBuilder class.
 */
object BackfillDataGraphBuilder:
  type Environment = BackfillDataProvider
    & StreamLifetimeService
    & BatchProcessor[DataStreamElement, Chunk[DataStreamElement]]
    & IcebergSynapseConsumer

  /**
   * Creates a new instance of the BackfillDataGraphBuilder class.
   *
   * @param backfillDataProvider  The backfill data provider.
   * @param streamLifetimeService The stream lifetime service.
   * @param batchProcessor        The batch processor.
   * @return A new instance of the BackfillDataGraphBuilder class.
   */
  def apply(backfillDataProvider: BackfillDataProvider,
            streamLifetimeService: StreamLifetimeService,
            batchProcessor: BatchProcessor[DataStreamElement, Chunk[DataStreamElement]],
            batchConsumer: IcebergSynapseConsumer): BackfillDataGraphBuilder =
    new BackfillDataGraphBuilder(backfillDataProvider, streamLifetimeService, batchProcessor, batchConsumer)

  /**
   * Creates a new instance of the BackfillDataGraphBuilder using services provided by ZIO Environment.
   *
   * @return A new instance of the BackfillDataGraphBuilder class.
   */
  def layer: ZLayer[Environment, Nothing, BackfillDataGraphBuilder] =
  ZLayer {
    for
      _ <- zlog("Running in backfill mode")
      dp <- ZIO.service[BackfillDataProvider]
      ls <- ZIO.service[StreamLifetimeService]
      bp <- ZIO.service[BatchProcessor[DataStreamElement, Chunk[DataStreamElement]]]
      bc <- ZIO.service[IcebergSynapseConsumer]
    yield BackfillDataGraphBuilder(dp, ls, bp, bc)
  }

