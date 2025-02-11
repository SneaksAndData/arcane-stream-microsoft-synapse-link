package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.processors

import services.data_providers.microsoft_synapse_link.AzureBlobStorageReaderZIO
import services.streaming.consumers.{CompletedBatch, PipelineResult}

import com.sneaksanddata.arcane.framework.services.streaming.base.BatchProcessor
import zio.stream.ZPipeline
import zio.{Task, ZIO, ZLayer}

import scala.annotation.tailrec

class SourceDeleteProcessor(azureBlobStorageReaderZIO: AzureBlobStorageReaderZIO)
  extends BatchProcessor[CompletedBatch, PipelineResult]:

  override def process: ZPipeline[Any, Throwable, CompletedBatch, PipelineResult] =
    ZPipeline.mapZIO({
      case (other, sourceCleanupRequest) =>
        val results = ZIO.foreach(sourceCleanupRequest)(r => azureBlobStorageReaderZIO.markForDeletion(r.prefix))
        results.map(r => (other, r))
    })

  def processEffects[A, B](effects: List[ZIO[Any, Throwable, A]], process: A => Task[B]): Task[List[B]] = {
    @tailrec
    def loop(remaining: List[ZIO[Any, Throwable, A]], acc: Task[List[B]]): Task[List[B]] = remaining match {
      case Nil => acc
      case head :: tail =>
        loop(tail, acc.flatMap(results => head.flatMap(a => process(a).map(b => results :+ b))))
    }

    loop(effects, ZIO.succeed(Nil))
  }
  
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
