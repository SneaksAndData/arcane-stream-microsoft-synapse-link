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
import com.sneaksanddata.arcane.framework.models.querygen.MergeQuery
import zio.stream.ZPipeline
import zio.{Task, ZIO, ZLayer}

class ArchivationProcessor(jdbcConsumer: JdbcConsumer,
                           archiveTableSettings: ArchiveTableSettings,
                           parallelismSettings: ParallelismSettings, tableManager: TableManager)
  extends BatchProcessor[InFlightBatch, CompletedBatch]:

  override def process: ZPipeline[Any, Throwable, InFlightBatch, CompletedBatch] =
    ZPipeline.mapZIO({
      case ((batches, other), batchNumber) =>
        for user <- System.env("USER")


        yield (results, other)
    })

  private def archiveTable(enabled: Boolean, bat): Task[Seq[BatchArchivationResult]] =
    if enabled then
      for
        _ <- zlog(s"Archiving batch $batchNumber")
        _ <- ZIO.foreach(batches) {
          batch =>
            tableManager.migrateSchema(batch.schema, archiveTableSettings.archiveTableFullName) *>
              tableManager.getTargetSchema(batch.name).flatMap(schema => jdbcConsumer.archiveBatch(batch, schema))
        }
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
      yield results
    else
      ZIO.succeed(Seq[BatchArchivationResult])

object ArchivationProcessor:

  type Environment = JdbcConsumer
    & ArchiveTableSettings
    & ParallelismSettings
    & TableManager

  def apply(jdbcConsumer: JdbcConsumer, archiveTableSettings: ArchiveTableSettings,
            parallelismSettings: ParallelismSettings, tableManager: TableManager): ArchivationProcessor =
    new ArchivationProcessor(jdbcConsumer, archiveTableSettings, parallelismSettings, tableManager)
    
  val layer: ZLayer[Environment, Nothing, ArchivationProcessor] =
    ZLayer {
      for
        jdbcConsumer <- ZIO.service[JdbcConsumer]
        archiveTableSettings <- ZIO.service[ArchiveTableSettings]
        parallelismSettings <- ZIO.service[ParallelismSettings]
        tableManager <- ZIO.service[TableManager]
      yield ArchivationProcessor(jdbcConsumer, archiveTableSettings, parallelismSettings, tableManager)
    }
