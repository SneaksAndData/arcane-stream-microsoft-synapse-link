package com.sneaksanddata.arcane.microsoft_synapse_link
package services.graph_builder

import models.app.{MicrosoftSynapseLinkStreamContext, ParallelismSettings}
import services.data_providers.microsoft_synapse_link.{CdmTableStream, DataStreamElement}
import services.streaming.consumers.{IcebergSynapseConsumer, IncomingBatch}
import services.streaming.processors.FieldFilteringProcessor

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.*
import com.sneaksanddata.arcane.framework.models.settings.{TargetTableSettings, VersionedDataGraphBuilderSettings}
import com.sneaksanddata.arcane.framework.services.app.base.StreamLifetimeService
import com.sneaksanddata.arcane.framework.services.streaming.base.{BatchProcessor, StreamGraphBuilder}
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, ZIO, ZLayer}

/**
 * The stream graph builder that reads the changes from the database.
 * @param settings The settings for the stream source.
 * @param versionedDataProvider The versioned data provider.
 * @param streamLifetimeService The stream lifetime service.
 * @param batchProcessor The batch processor.
 */
class VersionedDataGraphBuilder(settings: VersionedDataGraphBuilderSettings,
                                context: MicrosoftSynapseLinkStreamContext,
                                cdmTableStream: CdmTableStream,
                                streamLifetimeService: StreamLifetimeService,
                                targetTableSettings: TargetTableSettings,
                                batchProcessor: BatchProcessor[DataStreamElement, Chunk[DataStreamElement]],
                                batchConsumer: IcebergSynapseConsumer,
                                fieldFilteringProcessor: FieldFilteringProcessor,
                                parallelismSettings: ParallelismSettings) extends StreamGraphBuilder:


  type StreamElementType = IncomingBatch

  /**
   * Builds a stream that reads the changes from the database.
   *
   * @return The stream that reads the changes from the database.
   */
  def create: ZStream[Any, Throwable, IncomingBatch] = this.createStream.via(this.batchProcessor.process)

  /**
   * Creates a ZStream for the stream graph.
   *
   * @return ZStream (stream source for the stream graph).
   */
  def consume: ZSink[Any, Throwable, IncomingBatch, Any, Unit] = batchConsumer.consume

  private def createStream = cdmTableStream
    .snapshotPrefixes(settings.lookBackInterval, settings.changeCaptureInterval, context.changeCapturePeriod)
    .mapZIOPar(parallelismSettings.parallelism)(blob => cdmTableStream.getStream(blob))
    .flatMap(reader => cdmTableStream.getData(reader))
    .via(fieldFilteringProcessor.process)


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
    & FieldFilteringProcessor
    & MicrosoftSynapseLinkStreamContext

  /**
   * Creates a new instance of the BackfillDataGraphBuilder class.
   *
   * @param versionedDataProvider  The backfill data provider.
   * @param streamLifetimeService The stream lifetime service.
   * @param batchProcessor        The batch processor.
   * @return A new instance of the BackfillDataGraphBuilder class.
   */
  def apply[VersionType, BatchType](versionedDataGraphBuilderSettings: VersionedDataGraphBuilderSettings,
            microsoftSynapseLinkStreamContext: MicrosoftSynapseLinkStreamContext,
            cdmTableStream: CdmTableStream,
            targetTableSettings: TargetTableSettings,
            streamLifetimeService: StreamLifetimeService,
            batchProcessor: BatchProcessor[DataStreamElement,  Chunk[DataStreamElement]],
            batchConsumer: IcebergSynapseConsumer,
            fieldFilteringProcessor: FieldFilteringProcessor,
            parallelismSettings: ParallelismSettings): VersionedDataGraphBuilder =
    new VersionedDataGraphBuilder(versionedDataGraphBuilderSettings,
      microsoftSynapseLinkStreamContext,
      cdmTableStream,
      streamLifetimeService,
      targetTableSettings,
      batchProcessor,
      batchConsumer,
      fieldFilteringProcessor,
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
        fieldFilteringProcessor <- ZIO.service[FieldFilteringProcessor]
        sc <- ZIO.service[MicrosoftSynapseLinkStreamContext]
      yield VersionedDataGraphBuilder(sss, sc, dp, targetTableSettings, ls, bp, bc, fieldFilteringProcessor, parallelismSettings)
    }
    

