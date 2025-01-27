package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.processors

import services.clients.JdbcConsumer

import com.sneaksanddata.arcane.framework.services.consumers.StagedVersionedBatch
import com.sneaksanddata.arcane.framework.services.streaming.base.BatchProcessor
import com.sneaksanddata.arcane.microsoft_synapse_link.models.app.ParallelismSettings
import com.sneaksanddata.arcane.microsoft_synapse_link.models.app.streaming.SourceCleanupRequest
import com.sneaksanddata.arcane.microsoft_synapse_link.services.streaming.consumers.InFlightBatch
import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

/**
 * Processor that merges data into a target table.
 *
 * @param jdbcConsumer The JDBC consumer.
 */
class MergeBatchProcessor(jdbcConsumer: JdbcConsumer[StagedVersionedBatch], parallelismSettings: ParallelismSettings)
  extends BatchProcessor[InFlightBatch, InFlightBatch]:

  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  override def process: ZPipeline[Any, Throwable, InFlightBatch, InFlightBatch] =
    ZPipeline.mapZIO({
      case (batch, other) => jdbcConsumer.applyBatch(batch).map(_ => (batch, other))
    })


object MergeBatchProcessor:

  /**
   * Factory method to create MergeProcessor
   * @param jdbcConsumer The JDBC consumer.
   * @return The initialized MergeProcessor instance
   */
  def apply(jdbcConsumer: JdbcConsumer[StagedVersionedBatch], parallelismSettings: ParallelismSettings): MergeBatchProcessor =
    new MergeBatchProcessor(jdbcConsumer, parallelismSettings)

  /**
   * The required environment for the MergeProcessor.
   */
  type Environment = JdbcConsumer[StagedVersionedBatch]
    & ParallelismSettings

  /**
   * The ZLayer that creates the MergeProcessor.
   */
  val layer: ZLayer[Environment, Nothing, MergeBatchProcessor] =
    ZLayer {
      for
        jdbcConsumer <- ZIO.service[JdbcConsumer[StagedVersionedBatch]]
        parallelismSettings <- ZIO.service[ParallelismSettings]
      yield MergeBatchProcessor(jdbcConsumer, parallelismSettings)
    }
