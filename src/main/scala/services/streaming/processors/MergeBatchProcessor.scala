package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.processors

import com.sneaksanddata.arcane.framework.services.consumers.StagedVersionedBatch
import com.sneaksanddata.arcane.framework.services.streaming.base.BatchProcessor

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

/**
 * Processor that merges data into a target table.
 *
 * @param jdbcConsumer The JDBC consumer.
 */
class MergeBatchProcessor(jdbcConsumer: JdbcConsumer[StagedVersionedBatch])
  extends BatchProcessor[StagedVersionedBatch, StagedVersionedBatch]:

  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  override def process: ZPipeline[Any, Throwable, StagedVersionedBatch, StagedVersionedBatch] =
    ZPipeline.mapZIO(batch => ZIO.fromFuture(implicit ec => jdbcConsumer.applyBatch(batch)).map(_ => batch))


object MergeBatchProcessor:

  /**
   * Factory method to create MergeProcessor
   * @param jdbcConsumer The JDBC consumer.
   * @return The initialized MergeProcessor instance
   */
  def apply(jdbcConsumer: JdbcConsumer[StagedVersionedBatch]): MergeBatchProcessor =
    new MergeBatchProcessor(jdbcConsumer)

  /**
   * The required environment for the MergeProcessor.
   */
  type Environment = JdbcConsumer[StagedVersionedBatch]

  /**
   * The ZLayer that creates the MergeProcessor.
   */
  val layer: ZLayer[Environment, Nothing, MergeBatchProcessor] =
    ZLayer {
      for
        jdbcConsumer <- ZIO.service[JdbcConsumer[StagedVersionedBatch]]
      yield processors.MergeBatchProcessor(jdbcConsumer)
    }
