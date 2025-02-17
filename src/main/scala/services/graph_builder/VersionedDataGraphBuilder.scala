package com.sneaksanddata.arcane.microsoft_synapse_link
package services.graph_builder

import models.app.{ParallelismSettings, TargetTableSettings}
import services.data_providers.microsoft_synapse_link.{CdmTableStream, DataStreamElement}
import services.streaming.consumers.{IcebergSynapseConsumer, IncomingBatch}

import com.sneaksanddata.arcane.framework.models.DataRow
import com.sneaksanddata.arcane.framework.models.settings.VersionedDataGraphBuilderSettings
import com.sneaksanddata.arcane.framework.services.app.base.StreamLifetimeService
import com.sneaksanddata.arcane.framework.services.streaming.base.{BatchProcessor, StreamGraphBuilder}
import com.sneaksanddata.arcane.framework.services.streaming.consumers.StreamingConsumer
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.*
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Schedule, ZIO, ZLayer}

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
                                targetTableSettings: TargetTableSettings,
                                batchProcessor: BatchProcessor[DataStreamElement, Chunk[DataStreamElement]],
                                batchConsumer: IcebergSynapseConsumer,
                                parallelismSettings: ParallelismSettings) extends StreamGraphBuilder:


  type StreamElementType = IncomingBatch

  /**
   * Builds a stream that reads the changes from the database.
   *
   * @return The stream that reads the changes from the database.
   */
  def create: ZStream[Any, Throwable, IncomingBatch] =
    this.createStream.via(this.batchProcessor.process).zip(ZStream.repeat(targetTableSettings.targetTableFullName))

  /**
   * Creates a ZStream for the stream graph.
   *
   * @return ZStream (stream source for the stream graph).
   */
  def consume: ZSink[Any, Throwable, IncomingBatch, Any, Unit] = batchConsumer.consume

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
    & BatchProcessor[DataStreamElement,  Chunk[DataStreamElement]]
    & IcebergSynapseConsumer
    & VersionedDataGraphBuilderSettings
    & ParallelismSettings
    & TargetTableSettings

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
            targetTableSettings: TargetTableSettings,
            streamLifetimeService: StreamLifetimeService,
            batchProcessor: BatchProcessor[DataStreamElement,  Chunk[DataStreamElement]],
            batchConsumer: IcebergSynapseConsumer,
            parallelismSettings: ParallelismSettings): VersionedDataGraphBuilder =
    new VersionedDataGraphBuilder(versionedDataGraphBuilderSettings,
      cdmTableStream,
      streamLifetimeService,
      targetTableSettings,
      batchProcessor,
      batchConsumer,
      parallelismSettings)

  /**
   * Creates a new instance of the BackfillDataGraphBuilder using services provided by ZIO Environment.
   *
   * @return A new instance of the BackfillDataGraphBuilder class.
   */
  def layer: ZLayer[Environment, Nothing, VersionedDataGraphBuilder] =
    ZLayer {
      for
        _ <- zlog("Running in streaming mode")
        sss <- ZIO.service[VersionedDataGraphBuilderSettings]
        dp <- ZIO.service[CdmTableStream]
        ls <- ZIO.service[StreamLifetimeService]
        bp <- ZIO.service[BatchProcessor[DataStreamElement,  Chunk[DataStreamElement]]]
        bc <- ZIO.service[IcebergSynapseConsumer]
        targetTableSettings <- ZIO.service[TargetTableSettings]
        parallelismSettings <- ZIO.service[ParallelismSettings]
      yield VersionedDataGraphBuilder(sss, dp, targetTableSettings, ls, bp, bc, parallelismSettings)
    }
    

