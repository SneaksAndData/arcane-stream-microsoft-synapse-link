package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.processors

import services.clients.{BatchArchivationResult, JdbcConsumer}

import com.sneaksanddata.arcane.framework.services.consumers.StagedVersionedBatch
import com.sneaksanddata.arcane.framework.services.streaming.base.BatchProcessor
import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

class ArchivationProcessor(jdbcConsumer: JdbcConsumer[StagedVersionedBatch])
  extends BatchProcessor[StagedVersionedBatch, BatchArchivationResult]:

  override def process: ZPipeline[Any, Throwable, StagedVersionedBatch, BatchArchivationResult] =
    ZPipeline.mapZIO(batch => ZIO.fromFuture(implicit ec => jdbcConsumer.archiveBatch(batch)))

object ArchivationProcessor:

  type Environment = JdbcConsumer[StagedVersionedBatch]
  
  def apply(jdbcConsumer: JdbcConsumer[StagedVersionedBatch]): ArchivationProcessor =
    new ArchivationProcessor(jdbcConsumer)
    
  val layer: ZLayer[Environment, Nothing, ArchivationProcessor] =
    ZLayer {
      for
        jdbcConsumer <- ZIO.service[JdbcConsumer[StagedVersionedBatch]]
      yield ArchivationProcessor(jdbcConsumer)
    }
