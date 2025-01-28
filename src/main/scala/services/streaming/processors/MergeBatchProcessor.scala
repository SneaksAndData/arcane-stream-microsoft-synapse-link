package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.processors

import models.app.{OptimizeSettings, ParallelismSettings, TargetTableSettings}
import services.clients.{BatchApplicationResult, JdbcConsumer}
import services.streaming.consumers.InFlightBatch

import com.sneaksanddata.arcane.framework.services.consumers.StagedVersionedBatch
import com.sneaksanddata.arcane.framework.services.streaming.base.BatchProcessor
import zio.stream.ZPipeline
import zio.{Task, ZIO, ZLayer}

/**
 * Processor that merges data into a target table.
 *
 * @param jdbcConsumer The JDBC consumer.
 */
class MergeBatchProcessor(jdbcConsumer: JdbcConsumer[StagedVersionedBatch],
                          parallelismSettings: ParallelismSettings,
                          targetTableSettings: TargetTableSettings)
  extends BatchProcessor[InFlightBatch, InFlightBatch]:

  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  override def process: ZPipeline[Any, Throwable, InFlightBatch, InFlightBatch] =
    ZPipeline.mapZIO({
      case ((batch, other), batchNumber) =>
        for _ <- ZIO.log(s"Applying batch $batchNumber")
            _ <- jdbcConsumer.applyBatch(batch)
            _ <- jdbcConsumer.optimizeTarget(targetTableSettings.targetTableFullName, batchNumber,
                  targetTableSettings.targetOptimizeSettings.batchThreshold,
                  targetTableSettings.targetOptimizeSettings.fileSizeThreshold)
            _ <- jdbcConsumer.expireSnapshots(targetTableSettings.targetTableFullName, batchNumber,
                  targetTableSettings.targetSnapshotExpirationSettings.batchThreshold,
                  targetTableSettings.targetSnapshotExpirationSettings.retentionThreshold)
            _ <- jdbcConsumer.expireOrphanFiles(targetTableSettings.targetTableFullName, batchNumber,
              targetTableSettings.targetOrphanFilesExpirationSettings.batchThreshold,
              targetTableSettings.targetOrphanFilesExpirationSettings.retentionThreshold)
        yield ((batch, other), batchNumber)
    })

object MergeBatchProcessor:

  /**
   * Factory method to create MergeProcessor
   * @param jdbcConsumer The JDBC consumer.
   * @return The initialized MergeProcessor instance
   */
  def apply(jdbcConsumer: JdbcConsumer[StagedVersionedBatch], parallelismSettings: ParallelismSettings, targetTableSettings: TargetTableSettings): MergeBatchProcessor =
    new MergeBatchProcessor(jdbcConsumer, parallelismSettings, targetTableSettings)

  /**
   * The required environment for the MergeProcessor.
   */
  type Environment = JdbcConsumer[StagedVersionedBatch]
    & ParallelismSettings
    & TargetTableSettings

  /**
   * The ZLayer that creates the MergeProcessor.
   */
  val layer: ZLayer[Environment, Nothing, MergeBatchProcessor] =
    ZLayer {
      for
        jdbcConsumer <- ZIO.service[JdbcConsumer[StagedVersionedBatch]]
        parallelismSettings <- ZIO.service[ParallelismSettings]
        targetTableSettings <- ZIO.service[TargetTableSettings]
      yield MergeBatchProcessor(jdbcConsumer, parallelismSettings, targetTableSettings)
    }
