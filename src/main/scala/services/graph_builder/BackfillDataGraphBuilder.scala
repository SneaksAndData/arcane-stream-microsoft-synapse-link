package com.sneaksanddata.arcane.microsoft_synapse_link
package services.graph_builder

import services.data_providers.microsoft_synapse_link.{BackfillBatchInFlight, DataStreamElement, MicrosoftSynapseLinkDataProvider}
import services.streaming.consumers.IcebergSynapseBackfillConsumer

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.*
import com.sneaksanddata.arcane.framework.services.app.base.StreamLifetimeService
import com.sneaksanddata.arcane.framework.services.streaming.base.{BatchProcessor, StreamGraphBuilder}
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, ZIO, ZLayer}

class BackfillDataGraphBuilder(backfillDataProvider: MicrosoftSynapseLinkDataProvider,
                               streamLifetimeService: StreamLifetimeService,
                               batchProcessor: BatchProcessor[DataStreamElement, Chunk[DataStreamElement]],
                               batchConsumer: IcebergSynapseBackfillConsumer)
  extends StreamGraphBuilder:


  override type StreamElementType = BackfillBatchInFlight

  override def create: ZStream[Any, Throwable, StreamElementType] = ZStream.fromZIO(backfillDataProvider.requestBackfill)

  override def consume: ZSink[Any, Throwable, StreamElementType, Any, Unit] = batchConsumer.consume

/**
 * The companion object for the VersionedDataGraphBuilder class.
 */
object BackfillDataGraphBuilder:
  type Environment = MicrosoftSynapseLinkDataProvider
    & StreamLifetimeService
    & BatchProcessor[DataStreamElement, Chunk[DataStreamElement]]
    & IcebergSynapseBackfillConsumer

  /**
   * Creates a new instance of the BackfillDataGraphBuilder class.
   *
   * @param backfillDataProvider  The backfill data provider.
   * @param streamLifetimeService The stream lifetime service.
   * @param batchProcessor        The batch processor.
   * @return A new instance of the BackfillDataGraphBuilder class.
   */
  def apply(backfillDataProvider: MicrosoftSynapseLinkDataProvider,
            streamLifetimeService: StreamLifetimeService,
            batchProcessor: BatchProcessor[DataStreamElement, Chunk[DataStreamElement]],
            batchConsumer: IcebergSynapseBackfillConsumer): BackfillDataGraphBuilder =
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
      dp <- ZIO.service[MicrosoftSynapseLinkDataProvider]
      ls <- ZIO.service[StreamLifetimeService]
      bp <- ZIO.service[BatchProcessor[DataStreamElement, Chunk[DataStreamElement]]]
      bc <- ZIO.service[IcebergSynapseBackfillConsumer]
    yield BackfillDataGraphBuilder(dp, ls, bp, bc)
  }

