package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.processors

import services.clients.{BatchArchivationResult, JdbcConsumer}

import com.sneaksanddata.arcane.framework.services.consumers.StagedVersionedBatch
import com.sneaksanddata.arcane.framework.services.streaming.base.BatchProcessor
import com.sneaksanddata.arcane.microsoft_synapse_link.models.app.ArchiveTableSettings
import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

class ArchivationProcessor(jdbcConsumer: JdbcConsumer[StagedVersionedBatch], archiveTableSettings: ArchiveTableSettings)
  extends BatchProcessor[StagedVersionedBatch, BatchArchivationResult]:

  override def process: ZPipeline[Any, Throwable, StagedVersionedBatch, BatchArchivationResult] =
    ZPipeline.mapZIOPar(16)(batch => jdbcConsumer.archiveBatch(batch))

object ArchivationProcessor:

  type Environment = JdbcConsumer[StagedVersionedBatch]
    & ArchiveTableSettings
  
  def apply(jdbcConsumer: JdbcConsumer[StagedVersionedBatch], archiveTableSettings: ArchiveTableSettings): ArchivationProcessor =
    new ArchivationProcessor(jdbcConsumer, archiveTableSettings)
    
  val layer: ZLayer[Environment, Nothing, ArchivationProcessor] =
    ZLayer {
      for
        jdbcConsumer <- ZIO.service[JdbcConsumer[StagedVersionedBatch]]
        archiveTableSettings <- ZIO.service[ArchiveTableSettings]
      yield ArchivationProcessor(jdbcConsumer, archiveTableSettings)
    }
