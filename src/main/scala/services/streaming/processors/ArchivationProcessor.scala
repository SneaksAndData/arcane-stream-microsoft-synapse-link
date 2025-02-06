package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.processors

import services.clients.{BatchArchivationResult, JdbcConsumer}
import services.streaming.consumers.{CompletedBatch, InFlightBatch}
import models.app.streaming.SourceCleanupRequest
import models.app.{ArchiveTableSettings, ParallelismSettings}

import com.sneaksanddata.arcane.framework.models.ArcaneSchema
import com.sneaksanddata.arcane.framework.services.consumers.StagedVersionedBatch
import com.sneaksanddata.arcane.framework.services.streaming.base.BatchProcessor
import com.sneaksanddata.arcane.microsoft_synapse_link.models.app.streaming.SourceCleanupRequest
import com.sneaksanddata.arcane.microsoft_synapse_link.models.app.{ArchiveTableSettings, ParallelismSettings}
import com.sneaksanddata.arcane.microsoft_synapse_link.services.app.TableManager
import com.sneaksanddata.arcane.microsoft_synapse_link.services.streaming.consumers.{CompletedBatch, InFlightBatch}
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.*

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

class ArchivationProcessor(jdbcConsumer: JdbcConsumer[StagedVersionedBatch],
                           archiveTableSettings: ArchiveTableSettings,
                           parallelismSettings: ParallelismSettings, tableManager: TableManager)
  extends BatchProcessor[InFlightBatch, CompletedBatch]:

  override def process: ZPipeline[Any, Throwable, InFlightBatch, CompletedBatch] =
    ZPipeline.mapZIO({
      case ((batches, other), batchNumber) =>
        for _ <- zlog(s"Archiving batch $batchNumber")

            targetSchema <- jdbcConsumer.getTargetSchema(archiveTableSettings.archiveTableFullName)

            updatingFields = tableManager.getUpdatingFields(targetSchema, batches.map(_.schema)).flatten.toSeq
            _ <- tableManager.modifyColumns(archiveTableSettings.archiveTableFullName, updatingFields)

            targetSchema <- jdbcConsumer.getTargetSchema(archiveTableSettings.archiveTableFullName)
            missingFields = tableManager.getMissingFields(targetSchema, batches.map(_.schema)).flatten.toSeq
            _ <- tableManager.addColumns(archiveTableSettings.archiveTableFullName, missingFields)

            _ <- ZIO.foreach(batches)(batch => jdbcConsumer.archiveBatch(batch))

            results <- ZIO.foreach(batches)(batch => jdbcConsumer.dropTempTable(batch))
            _ <- jdbcConsumer.optimizeTarget(archiveTableSettings.archiveTableFullName, batchNumber,
                archiveTableSettings.archiveOptimizeSettings.batchThreshold,
                archiveTableSettings.archiveOptimizeSettings.fileSizeThreshold)
            _ <- jdbcConsumer.expireSnapshots(archiveTableSettings.archiveTableFullName, batchNumber,
              archiveTableSettings.archiveSnapshotExpirationSettings.batchThreshold,
              archiveTableSettings.archiveSnapshotExpirationSettings.retentionThreshold)
            _ <- jdbcConsumer.expireOrphanFiles(archiveTableSettings.archiveTableFullName, batchNumber,
              archiveTableSettings.archiveOrphanFilesExpirationSettings.batchThreshold,
              archiveTableSettings.archiveOrphanFilesExpirationSettings.retentionThreshold)
        yield (results, other)
    })

object ArchivationProcessor:

  type Environment = JdbcConsumer[StagedVersionedBatch]
    & ArchiveTableSettings
    & ParallelismSettings
    & TableManager

  def apply(jdbcConsumer: JdbcConsumer[StagedVersionedBatch], archiveTableSettings: ArchiveTableSettings,
            parallelismSettings: ParallelismSettings, tableManager: TableManager): ArchivationProcessor =
    new ArchivationProcessor(jdbcConsumer, archiveTableSettings, parallelismSettings, tableManager)
    
  val layer: ZLayer[Environment, Nothing, ArchivationProcessor] =
    ZLayer {
      for
        jdbcConsumer <- ZIO.service[JdbcConsumer[StagedVersionedBatch]]
        archiveTableSettings <- ZIO.service[ArchiveTableSettings]
        parallelismSettings <- ZIO.service[ParallelismSettings]
        tableManager <- ZIO.service[TableManager]
      yield ArchivationProcessor(jdbcConsumer, archiveTableSettings, parallelismSettings, tableManager)
    }
