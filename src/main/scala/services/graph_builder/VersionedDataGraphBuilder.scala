package com.sneaksanddata.arcane.microsoft_synapse_link
package services.graph_builder

import models.app.ParallelismSettings
import services.data_providers.microsoft_synapse_link.{CdmTableStream, DataStreamElement}
import services.streaming.consumers.IcebergSynapseConsumer

import com.sneaksanddata.arcane.framework.models.DataRow
import com.sneaksanddata.arcane.framework.models.settings.VersionedDataGraphBuilderSettings
import com.sneaksanddata.arcane.framework.services.app.base.StreamLifetimeService
import com.sneaksanddata.arcane.framework.services.streaming.base.{BatchProcessor, StreamGraphBuilder}
import com.sneaksanddata.arcane.framework.services.streaming.consumers.StreamingConsumer
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.*

import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Schedule, ZIO}

import java.time.{OffsetDateTime, ZoneOffset}
import scala.concurrent.Future

/**
 * The stream graph builder that reads the changes from the database.
 * @param versionedDataGraphBuilderSettings The settings for the stream source.
 * @param versionedDataProvider The versioned data provider.
 * @param streamLifetimeService The stream lifetime service.
 * @param batchProcessor The batch processor.
 */
class VersionedDataGraphBuilder(versionedDataGraphBuilderSettings: VersionedDataGraphBuilderSettings,
                                cdmTableStream: CdmTableStream,
                                streamLifetimeService: StreamLifetimeService,
                                batchProcessor: BatchProcessor[DataStreamElement, Chunk[DataStreamElement]],
                                batchConsumer: IcebergSynapseConsumer,
                                parallelismSettings: ParallelismSettings):

  /**
   * Builds a stream that reads the changes from the database.
   *
   * @return The stream that reads the changes from the database.
   */
  def create: ZStream[Any, Throwable, Chunk[DataStreamElement]] =
    this.createStream.via(this.batchProcessor.process)

  /**
   * Creates a ZStream for the stream graph.
   *
   * @return ZStream (stream source for the stream graph).
   */
  def consume: ZSink[Any, Throwable, Chunk[DataStreamElement], Any, Unit] = batchConsumer.consume

  private def createStream = cdmTableStream
    .snapshotPrefixes(versionedDataGraphBuilderSettings.lookBackInterval, versionedDataGraphBuilderSettings.changeCaptureInterval)
    .mapZIOPar(parallelismSettings.parallelism)(blob => cdmTableStream.getStream(blob))
    .flatMap(reader => cdmTableStream.getData(reader))


/**
 * The companion object for the VersionedDataGraphBuilder class.
 */
object VersionedDataGraphBuilder:
  
  type Environment = CdmTableStream
    & StreamLifetimeService
    & BatchProcessor[DataStreamElement, Chunk[DataStreamElement]]
    & IcebergSynapseConsumer
    & VersionedDataGraphBuilderSettings
    & ParallelismSettings

  /**
   * Creates a new instance of the BackfillDataGraphBuilder class.
   *
   * @param versionedDataProvider  The backfill data provider.
   * @param streamLifetimeService The stream lifetime service.
   * @param batchProcessor        The batch processor.
   * @return A new instance of the BackfillDataGraphBuilder class.
   */
  def apply[VersionType, BatchType](versionedDataGraphBuilderSettings: VersionedDataGraphBuilderSettings,
            cdmTableStream: CdmTableStream,
            streamLifetimeService: StreamLifetimeService,
            batchProcessor: BatchProcessor[DataStreamElement, Chunk[DataStreamElement]],
            batchConsumer: IcebergSynapseConsumer,
            parallelismSettings: ParallelismSettings): VersionedDataGraphBuilder =
    new VersionedDataGraphBuilder(versionedDataGraphBuilderSettings,
      cdmTableStream,
      streamLifetimeService,
      batchProcessor,
      batchConsumer,
      parallelismSettings)

  /**
   * Creates a new instance of the BackfillDataGraphBuilder using services provided by ZIO Environment.
   *
   * @return A new instance of the BackfillDataGraphBuilder class.
   */
  def layer: ZIO[Environment, Nothing, VersionedDataGraphBuilder] =
    for
      _ <- zlog("Running in streaming mode")
      sss <- ZIO.service[VersionedDataGraphBuilderSettings]
      dp <- ZIO.service[CdmTableStream]
      ls <- ZIO.service[StreamLifetimeService]
      bp <- ZIO.service[BatchProcessor[DataStreamElement, Chunk[DataStreamElement]]]
      bc <- ZIO.service[IcebergSynapseConsumer]
      parallelismSettings <- ZIO.service[ParallelismSettings]
    yield VersionedDataGraphBuilder(sss, dp, ls, bp, bc, parallelismSettings)
    

