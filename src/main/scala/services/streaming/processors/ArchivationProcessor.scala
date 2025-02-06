package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.processors

import services.clients.{BatchArchivationResult, JdbcConsumer}
import services.streaming.consumers.{CompletedBatch, InFlightBatch}
import models.app.streaming.SourceCleanupRequest
import models.app.{ArchiveTableSettings, ParallelismSettings}

import com.sneaksanddata.arcane.framework.services.consumers.StagedVersionedBatch
import com.sneaksanddata.arcane.framework.services.streaming.base.BatchProcessor
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.*

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

class ArchivationProcessor(jdbcConsumer: JdbcConsumer[StagedVersionedBatch],
                           archiveTableSettings: ArchiveTableSettings,
                           parallelismSettings: ParallelismSettings)
  extends BatchProcessor[InFlightBatch, CompletedBatch]:

  override def process: ZPipeline[Any, Throwable, InFlightBatch, CompletedBatch] =
    ZPipeline.mapZIO({
      case ((batch, other), batchNumber) => 
        for _ <- zlog(s"Archiving batch $batchNumber")
            _ <- jdbcConsumer.archiveBatch(batch)
            result <- jdbcConsumer.dropTempTable(batch)
            _ <- jdbcConsumer.optimizeTarget(archiveTableSettings.archiveTableFullName, batchNumber,
                archiveTableSettings.archiveOptimizeSettings.batchThreshold,
                archiveTableSettings.archiveOptimizeSettings.fileSizeThreshold)
            _ <- jdbcConsumer.expireSnapshots(archiveTableSettings.archiveTableFullName, batchNumber,
              archiveTableSettings.archiveSnapshotExpirationSettings.batchThreshold,
              archiveTableSettings.archiveSnapshotExpirationSettings.retentionThreshold)
            _ <- jdbcConsumer.expireOrphanFiles(archiveTableSettings.archiveTableFullName, batchNumber,
              archiveTableSettings.archiveOrphanFilesExpirationSettings.batchThreshold,
              archiveTableSettings.archiveOrphanFilesExpirationSettings.retentionThreshold)
        yield (result, other)
    })

object ArchivationProcessor:

  type Environment = JdbcConsumer[StagedVersionedBatch]
    & ArchiveTableSettings
    & ParallelismSettings
  
  def apply(jdbcConsumer: JdbcConsumer[StagedVersionedBatch], archiveTableSettings: ArchiveTableSettings,
            parallelismSettings: ParallelismSettings): ArchivationProcessor =
    new ArchivationProcessor(jdbcConsumer, archiveTableSettings, parallelismSettings)
    
  val layer: ZLayer[Environment, Nothing, ArchivationProcessor] =
    ZLayer {
      for
        jdbcConsumer <- ZIO.service[JdbcConsumer[StagedVersionedBatch]]
        archiveTableSettings <- ZIO.service[ArchiveTableSettings]
        parallelismSettings <- ZIO.service[ParallelismSettings]
      yield ArchivationProcessor(jdbcConsumer, archiveTableSettings, parallelismSettings)
    }
