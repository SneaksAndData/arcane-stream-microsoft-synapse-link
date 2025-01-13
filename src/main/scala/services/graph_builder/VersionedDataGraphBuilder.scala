package com.sneaksanddata.arcane.microsoft_synapse_link
package services.graph_builder

import main.validateEnv
import services.streaming.base.VersionedDataProvider

import com.sneaksanddata.arcane.framework.models.DataRow
import com.sneaksanddata.arcane.framework.models.settings.VersionedDataGraphBuilderSettings
import com.sneaksanddata.arcane.framework.services.app.base.StreamLifetimeService
import com.sneaksanddata.arcane.framework.services.mssql.MsSqlConnection.DataBatch
import com.sneaksanddata.arcane.framework.services.streaming.base.{BatchProcessor, StreamGraphBuilder}
import com.sneaksanddata.arcane.framework.services.streaming.consumers.StreamingConsumer
import org.slf4j.{Logger, LoggerFactory}
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Schedule, ZIO}

import java.time.OffsetDateTime

/**
 * The stream graph builder that reads the changes from the database.
 * @param versionedDataGraphBuilderSettings The settings for the stream source.
 * @param versionedDataProvider The versioned data provider.
 * @param streamLifetimeService The stream lifetime service.
 * @param batchProcessor The batch processor.
 */
class VersionedDataGraphBuilder[VersionType, BatchType]
                                (versionedDataGraphBuilderSettings: VersionedDataGraphBuilderSettings,
                                versionedDataProvider: VersionedDataProvider[VersionType, BatchType],
                                streamLifetimeService: StreamLifetimeService,
                                batchProcessor: BatchProcessor[BatchType, Chunk[DataRow]],
                                batchConsumer: StreamingConsumer)
  extends StreamGraphBuilder:

  private val logger: Logger = LoggerFactory.getLogger(classOf[VersionedDataGraphBuilder[VersionType, BatchType]])
  override type StreamElementType = Chunk[DataRow]

  /**
   * Builds a stream that reads the changes from the database.
   *
   * @return The stream that reads the changes from the database.
   */
  override def create: ZStream[Any, Throwable, StreamElementType] = this.createStream.via(this.batchProcessor.process)

  /**
   * Creates a ZStream for the stream graph.
   *
   * @return ZStream (stream source for the stream graph).
   */
  override def consume: ZSink[Any, Throwable, Chunk[DataRow], Any, Unit] = batchConsumer.consume

  private def createStream = ZStream
    .unfoldZIO(versionedDataProvider.firstVersion) { previousVersion =>
      if streamLifetimeService.cancelled then
        ZIO.succeed(None)
      else
        continueStream(previousVersion)
    }
    .schedule(Schedule.spaced(versionedDataGraphBuilderSettings.changeCaptureInterval))

  private def continueStream(previousVersion: Option[VersionType]): ZIO[Any, Throwable, Some[(BatchType, Option[VersionType])]] =
    versionedDataProvider.requestChanges(previousVersion, versionedDataGraphBuilderSettings.lookBackInterval) map { versionedBatch  =>
      val latestVersion = versionedDataProvider.extractVersion(versionedBatch)
      logger.info(s"Latest version: $latestVersion")
      Some(versionedBatch, latestVersion)
    }

/**
 * The companion object for the VersionedDataGraphBuilder class.
 */
object VersionedDataGraphBuilder:
  
  type Environment = VersionedDataProvider[OffsetDateTime, LazyList[DataRow]]
    & StreamLifetimeService
    & BatchProcessor[LazyList[DataRow], Chunk[DataRow]]
    & StreamingConsumer
    & VersionedDataGraphBuilderSettings

  /**
   * Creates a new instance of the BackfillDataGraphBuilder class.
   *
   * @param versionedDataProvider  The backfill data provider.
   * @param streamLifetimeService The stream lifetime service.
   * @param batchProcessor        The batch processor.
   * @return A new instance of the BackfillDataGraphBuilder class.
   */
  def apply[VersionType, BatchType](versionedDataGraphBuilderSettings: VersionedDataGraphBuilderSettings,
             versionedDataProvider: VersionedDataProvider[VersionType, BatchType],
            streamLifetimeService: StreamLifetimeService,
            batchProcessor: BatchProcessor[BatchType, Chunk[DataRow]],
            batchConsumer: StreamingConsumer): VersionedDataGraphBuilder[VersionType, BatchType] =
    new VersionedDataGraphBuilder(versionedDataGraphBuilderSettings,
      versionedDataProvider,
      streamLifetimeService,
      batchProcessor,
      batchConsumer)

  /**
   * Creates a new instance of the BackfillDataGraphBuilder using services provided by ZIO Environment.
   *
   * @return A new instance of the BackfillDataGraphBuilder class.
   */
  def layer: ZIO[Environment, Nothing, VersionedDataGraphBuilder[OffsetDateTime, LazyList[DataRow]]] =
    for
      _ <- ZIO.log("Running in streaming mode")
      sss <- ZIO.service[VersionedDataGraphBuilderSettings]
      dp <- ZIO.service[VersionedDataProvider[OffsetDateTime, LazyList[DataRow]]]
      ls <- ZIO.service[StreamLifetimeService]
      bp <- ZIO.service[BatchProcessor[LazyList[DataRow], Chunk[DataRow]]]
      bc <- ZIO.service[StreamingConsumer]
    yield VersionedDataGraphBuilder(sss, dp, ls, bp, bc)
    

