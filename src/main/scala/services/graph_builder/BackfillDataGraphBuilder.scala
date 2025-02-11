package com.sneaksanddata.arcane.microsoft_synapse_link
package services.graph_builder

import com.sneaksanddata.arcane.framework.models.DataRow
import com.sneaksanddata.arcane.framework.services.app.base.StreamLifetimeService
import com.sneaksanddata.arcane.framework.services.streaming.base.{BackfillDataProvider, BatchProcessor, StreamGraphBuilder}
import com.sneaksanddata.arcane.framework.services.streaming.consumers.BackfillConsumer
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.*

import zio.stream.{ZSink, ZStream}
import zio.{Chunk, ZIO}

class BackfillDataGraphBuilder(backfillDataProvider: BackfillDataProvider,
                               streamLifetimeService: StreamLifetimeService,
                               batchProcessor: BatchProcessor[DataRow, Chunk[DataRow]],
                               batchConsumer: BackfillConsumer)
  extends StreamGraphBuilder:


  override type StreamElementType = Chunk[DataRow]

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
    & BatchProcessor[DataRow, Chunk[DataRow]]
    & BackfillConsumer

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
            batchProcessor: BatchProcessor[DataRow, Chunk[DataRow]],
            batchConsumer: BackfillConsumer): BackfillDataGraphBuilder =
    new BackfillDataGraphBuilder(backfillDataProvider, streamLifetimeService, batchProcessor, batchConsumer)

  /**
   * Creates a new instance of the BackfillDataGraphBuilder using services provided by ZIO Environment.
   *
   * @return A new instance of the BackfillDataGraphBuilder class.
   */
  def apply(): ZIO[Environment, Nothing, BackfillDataGraphBuilder] =
    for
      _ <- zlog("Running in backfill mode")
      dp <- ZIO.service[BackfillDataProvider]
      ls <- ZIO.service[StreamLifetimeService]
      bp <- ZIO.service[BatchProcessor[DataRow, Chunk[DataRow]]]
      bc <- ZIO.service[BackfillConsumer]
    yield BackfillDataGraphBuilder(dp, ls, bp, bc)

