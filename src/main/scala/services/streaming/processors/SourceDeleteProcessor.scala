package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.processors

import models.app.{ArchiveTableSettings, ParallelismSettings}
import services.clients.{BatchArchivationResult, JdbcConsumer}
import services.streaming.consumers.{CompletedBatch, InFlightBatch, PiplineResult}

import com.sneaksanddata.arcane.framework.services.consumers.StagedVersionedBatch
import com.sneaksanddata.arcane.framework.services.streaming.base.BatchProcessor
import com.sneaksanddata.arcane.microsoft_synapse_link.models.app.streaming.SourceCleanupRequest
import com.sneaksanddata.arcane.microsoft_synapse_link.services.data_providers.microsoft_synapse_link.AzureBlobStorageReaderZIO
import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

class SourceDeleteProcessor(azureBlobStorageReaderZIO: AzureBlobStorageReaderZIO)
  extends BatchProcessor[CompletedBatch, PiplineResult]:

  override def process: ZPipeline[Any, Throwable, CompletedBatch, PiplineResult] =
    ZPipeline.mapZIO({
      case sourceCleanupRequest: SourceCleanupRequest => azureBlobStorageReaderZIO.deleteSourceFile(sourceCleanupRequest.prefix)
      case other: BatchArchivationResult => ZIO.attempt(other)
    })

object SourceDeleteProcessor:

  type Environment = AzureBlobStorageReaderZIO
  
  def apply(azureBlobStorageReaderZIO: AzureBlobStorageReaderZIO): SourceDeleteProcessor =
    new SourceDeleteProcessor(azureBlobStorageReaderZIO)
    
  val layer: ZLayer[Environment, Nothing, SourceDeleteProcessor] =
    ZLayer {
      for
        bs <- ZIO.service[AzureBlobStorageReaderZIO]
      yield SourceDeleteProcessor(bs)
    }
