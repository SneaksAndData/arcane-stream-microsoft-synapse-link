package com.sneaksanddata.arcane.microsoft_synapse_link

import extensions.DataRowExtensions.schema
import models.app.*
import models.app.streaming.SourceCleanupRequest
import services.app.TableManager
import services.clients.JdbcConsumer
import services.data_providers.microsoft_synapse_link.{CdmTableStream, DataStreamElement}
import services.streaming.consumers.{CompletedBatch, InFlightBatch, IncomingBatch}
import services.streaming.processors.{CdmGroupingProcessor, MergeBatchProcessor}

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.models.querygen.MergeQuery
import com.sneaksanddata.arcane.framework.models.{ArcaneSchema, DataRow}
import com.sneaksanddata.arcane.framework.services.consumers.{StagedBackfillBatch, SynapseLinkBackfillBatch}
import com.sneaksanddata.arcane.framework.services.lakehouse.{CatalogWriter, given_Conversion_ArcaneSchema_Schema}
import com.sneaksanddata.arcane.framework.services.streaming.base.{BackfillDataProvider, BatchProcessor}
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.stream.{ZPipeline, ZStream}
import zio.{Chunk, Task, UIO, URIO, ZIO, ZLayer}

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.UUID

type SynapseLinkBackfillBatchInFlight = (StagedBackfillBatch, Chunk[SourceCleanupRequest])

trait MicrosoftSynapseLinkDataProvider:

  def requestBackfill: Task[SynapseLinkBackfillBatchInFlight]

case class BackfillTempTableSettings(override val targetTableFullName: String) extends TargetTableSettings:
  override val targetOptimizeSettings: Option[OptimizeSettings] = None
  override val targetSnapshotExpirationSettings: Option[SnapshotExpirationSettings] = None
  override val targetOrphanFilesExpirationSettings: Option[OrphanFilesExpirationSettings] = None


class MicrosoftSynapseLinkDataProviderImpl(cdmTableStream: CdmTableStream,
                                           jdbcConsumer: JdbcConsumer[MergeQuery],
                                           streamContext: MicrosoftSynapseLinkStreamContext,
                                           parallelismSettings: ParallelismSettings,
                                           groupingProcessor: CdmGroupingProcessor,
                                           stageProcessor: BatchProcessor[IncomingBatch, InFlightBatch],
                                           archivationProcessor: BatchProcessor[InFlightBatch, CompletedBatch],
                                           catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
                                           tableManager: TableManager,
                                           sinkSettings: TargetTableSettings) extends MicrosoftSynapseLinkDataProvider:

  def requestBackfill: Task[SynapseLinkBackfillBatchInFlight] =
    val backFillTableName = streamContext.getBackfillTableName
    val tempTargetTableSettings = BackfillTempTableSettings(backFillTableName)
    val mergeProcessor = MergeBatchProcessor(jdbcConsumer, parallelismSettings, tempTargetTableSettings, tableManager)
    
    val backfillStream = cdmTableStream
      .getPrefixesFromBeginning
      .mapZIOPar(parallelismSettings.parallelism)(blob => cdmTableStream.getStream(blob))
      .flatMap(reader => cdmTableStream.getData(reader))
      .via(groupingProcessor.process)
      .zip(ZStream.repeat(backFillTableName))
      .via(stageProcessor.process)
      .via(mergeProcessor.process)
      .via(archivationProcessor.process)

    for
      _ <- zlog("Starting backfill process")
      res <- backfillStream.runCollect
      cleanups = res.flatMap(_._2)
      _ <- zlog("Starting process completed")
      bfb <- createBackfillBatch(backFillTableName)
    yield (bfb, cleanups)


  private def createBackfillBatch(tableName: String): Task[StagedBackfillBatch] =
    for schema <- tableManager.getTargetSchema(tableName)
    yield SynapseLinkBackfillBatch(tableName, schema, sinkSettings.targetTableFullName)

object MicrosoftSynapseLinkDataProviderImpl:

  type Environment = CdmTableStream
    & MicrosoftSynapseLinkStreamContext
    & ParallelismSettings
    & CdmGroupingProcessor
    & CatalogWriter[RESTCatalog, Table, Schema]
    & TableManager
    & TargetTableSettings
    & JdbcConsumer[MergeQuery]
    & BatchProcessor[IncomingBatch, InFlightBatch]
    & BatchProcessor[InFlightBatch, CompletedBatch]

  def apply(cdmTableStream: CdmTableStream,
            jdbcConsumer: JdbcConsumer[MergeQuery],
            streamContext: MicrosoftSynapseLinkStreamContext,
            parallelismSettings: ParallelismSettings,
            groupingProcessor: CdmGroupingProcessor,
            stageProcessor: BatchProcessor[IncomingBatch, InFlightBatch],
            archivationProcessor: BatchProcessor[InFlightBatch, CompletedBatch],
            catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
            tableManager: TableManager,
            sinkSettings: TargetTableSettings): MicrosoftSynapseLinkDataProviderImpl =
    new MicrosoftSynapseLinkDataProviderImpl(
      cdmTableStream,
      jdbcConsumer,
      streamContext,
      parallelismSettings,
      groupingProcessor,
      stageProcessor,
      archivationProcessor,
      catalogWriter,
      tableManager,
      sinkSettings)

  def layer: ZLayer[Environment, Nothing, MicrosoftSynapseLinkDataProvider] =
    ZLayer {
      for
        cdmTableStream <- ZIO.service[CdmTableStream]
        jdbcConsumer <- ZIO.service[JdbcConsumer[MergeQuery]]
        streamContext <- ZIO.service[MicrosoftSynapseLinkStreamContext]
        parallelismSettings <- ZIO.service[ParallelismSettings]
        groupingProcessor <- ZIO.service[CdmGroupingProcessor]
        catalogWriter <- ZIO.service[CatalogWriter[RESTCatalog, Table, Schema]]
        tableManager <- ZIO.service[TableManager]
        sinkSettings <- ZIO.service[TargetTableSettings]
        stageProcessor <- ZIO.service[BatchProcessor[IncomingBatch, InFlightBatch]]
        archivationProcessor <- ZIO.service[BatchProcessor[InFlightBatch, CompletedBatch]]
      yield MicrosoftSynapseLinkDataProviderImpl(cdmTableStream, jdbcConsumer, streamContext, parallelismSettings, groupingProcessor, stageProcessor, archivationProcessor, catalogWriter, tableManager, sinkSettings)
    }

