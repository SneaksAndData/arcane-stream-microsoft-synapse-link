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
import com.sneaksanddata.arcane.microsoft_synapse_link.models.app.ParallelismSettings
import com.sneaksanddata.arcane.microsoft_synapse_link.services.data_providers.microsoft_synapse_link.CdmTableStream
import org.slf4j.{Logger, LoggerFactory}
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
class VersionedDataGraphBuilder[VersionType, BatchType]
                                (versionedDataGraphBuilderSettings: VersionedDataGraphBuilderSettings,
                                cdmTableStream: CdmTableStream,
                                streamLifetimeService: StreamLifetimeService,
                                batchProcessor: BatchProcessor[Array[DataRow], Chunk[DataRow]],
                                batchConsumer: StreamingConsumer,
                                parallelismSettings: ParallelismSettings)
  extends StreamGraphBuilder:

  private val logger: Logger = LoggerFactory.getLogger(classOf[VersionedDataGraphBuilder[VersionType, BatchType]])
  override type StreamElementType = Chunk[DataRow]

  /**
   * Builds a stream that reads the changes from the database.
   *
   * @return The stream that reads the changes from the database.
   */
  override def create: ZStream[Any, Throwable, Chunk[DataRow]] =
    this.createStream.via(this.batchProcessor.process)

  /**
   * Creates a ZStream for the stream graph.
   *
   * @return ZStream (stream source for the stream graph).
   */
  override def consume: ZSink[Any, Throwable, Chunk[DataRow], Any, Unit] = batchConsumer.consume

  private def createStream = cdmTableStream
    .snapshotPrefixes(versionedDataGraphBuilderSettings.lookBackInterval)
    .mapZIOPar(parallelismSettings.parallelism)(blob => cdmTableStream.getStream(blob).map(res => (res, blob.name)))
    .flatMap(reader => cdmTableStream.getData(reader))
    .map(e => Array(e))


/**
 * The companion object for the VersionedDataGraphBuilder class.
 */
object VersionedDataGraphBuilder:
  
  type Environment = CdmTableStream
    & StreamLifetimeService
    & BatchProcessor[Array[DataRow], Chunk[DataRow]]
    & StreamingConsumer
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
            batchProcessor: BatchProcessor[Array[DataRow], Chunk[DataRow]],
            batchConsumer: StreamingConsumer,
            parallelismSettings: ParallelismSettings): VersionedDataGraphBuilder[VersionType, BatchType] =
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
  def layer: ZIO[Environment, Nothing, VersionedDataGraphBuilder[OffsetDateTime, LazyList[DataRow]]] =
    for
      _ <- ZIO.log("Running in streaming mode")
      sss <- ZIO.service[VersionedDataGraphBuilderSettings]
      dp <- ZIO.service[CdmTableStream]
      ls <- ZIO.service[StreamLifetimeService]
      bp <- ZIO.service[BatchProcessor[Array[DataRow], Chunk[DataRow]]]
      bc <- ZIO.service[StreamingConsumer]
      parallelismSettings <- ZIO.service[ParallelismSettings]
    yield VersionedDataGraphBuilder(sss, dp, ls, bp, bc, parallelismSettings)
    

