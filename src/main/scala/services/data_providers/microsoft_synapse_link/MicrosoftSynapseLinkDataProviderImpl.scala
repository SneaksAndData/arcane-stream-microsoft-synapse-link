package com.sneaksanddata.arcane.microsoft_synapse_link

import extensions.DataRowExtensions.schema
import models.app.*
import models.app.streaming.SourceCleanupRequest
import services.app.TableManager
import services.clients.JdbcConsumer
import services.data_providers.microsoft_synapse_link.{CdmTableStream, DataStreamElement}
import services.streaming.consumers.{CompletedBatch, InFlightBatch, IncomingBatch}
import services.streaming.processors.{CdmGroupingProcessor, FieldFilteringProcessor, MergeBatchProcessor}

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.models.querygen.MergeQuery
import com.sneaksanddata.arcane.framework.models.settings.TablePropertiesSettings
import com.sneaksanddata.arcane.framework.models.{ArcaneSchema, DataRow}
import com.sneaksanddata.arcane.framework.services.consumers.{StagedBackfillBatch, StagedBackfillOverwriteBatch, SynapseLinkBackfillMergeBatch, SynapseLinkBackfillOverwriteBatch, SynapseLinkMergeBatch}
import com.sneaksanddata.arcane.framework.services.lakehouse.{CatalogWriter, given_Conversion_ArcaneSchema_Schema}
import com.sneaksanddata.arcane.framework.services.streaming.base.{BackfillDataProvider, BatchProcessor}
import org.apache.iceberg.{Schema, Table}
import zio.stream.{ZPipeline, ZStream}
import zio.{Chunk, Task, UIO, URIO, ZIO, ZLayer}

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.UUID

type BackfillBatchInFlight = (StagedBackfillBatch, Chunk[SourceCleanupRequest])

trait MicrosoftSynapseLinkDataProvider:

  def requestBackfill: Task[BackfillBatchInFlight]

case class BackfillTempTableSettings(override val targetTableFullName: String) extends TargetTableSettings:
  override val targetOptimizeSettings: Option[OptimizeSettings] = None
  override val targetSnapshotExpirationSettings: Option[SnapshotExpirationSettings] = None
  override val targetOrphanFilesExpirationSettings: Option[OrphanFilesExpirationSettings] = None


class MicrosoftSynapseLinkDataProviderImpl(cdmTableStream: CdmTableStream,
                                           jdbcConsumer: JdbcConsumer,
                                           streamContext: MicrosoftSynapseLinkStreamContext,
                                           parallelismSettings: ParallelismSettings,
                                           groupingProcessor: CdmGroupingProcessor,
                                           stageProcessor: BatchProcessor[IncomingBatch, InFlightBatch],
                                           archivationProcessor: BatchProcessor[InFlightBatch, CompletedBatch],
                                           tableManager: TableManager,
                                           sinkSettings: TargetTableSettings,
                                           archiveTableSettings: ArchiveTableSettings,
                                           fieldFilteringProcessor: FieldFilteringProcessor,
                                           backfillSettings: BackfillSettings,
                                           tablePropertiesSettings: TablePropertiesSettings) extends MicrosoftSynapseLinkDataProvider:

  private val backFillTableName = streamContext.backfillTableName
  private val tempTargetTableSettings = BackfillTempTableSettings(backFillTableName)
  private val mergeProcessor = MergeBatchProcessor(jdbcConsumer, parallelismSettings, tempTargetTableSettings, tableManager)

  def requestBackfill: Task[BackfillBatchInFlight] =

    for
      _ <- zlog(s"Starting backfill process. Backfill behavior: ${backfillSettings.backfillBehavior}")
      cleanupRequests <- backfillStream.runCollect
      _ <- zlog("Backfill process completed")
      backFillCompletionBatch <- createBackfillBatch(backFillTableName)
    yield (backFillCompletionBatch, cleanupRequests.flatten)

  private def csvLinesStream = cdmTableStream.getPrefixesFromBeginning
    .mapZIOPar(parallelismSettings.parallelism)(blob => cdmTableStream.getStream(blob))
    .flatMap(reader => cdmTableStream.getData(reader))

  private def backfillStream = csvLinesStream
    .via(fieldFilteringProcessor.process)
    .via(groupingProcessor.process)
    .zip(ZStream.repeat(backFillTableName))
    .via(stageProcessor.process)
    .via(mergeProcessor.process)
    .via(archivationProcessor.process)
    .map({
      case (_, cleanupRequests) => cleanupRequests
    })


  private def createBackfillBatch(tableName: String): Task[StagedBackfillBatch] =
    for schema <- tableManager.getTargetSchema(tableName)
    yield backfillSettings.backfillBehavior match
      case BackfillBehavior.Overwrite => SynapseLinkBackfillOverwriteBatch(tableName,
        schema,
        sinkSettings.targetTableFullName,
        archiveTableSettings.archiveTableFullName,
        tablePropertiesSettings)
      case BackfillBehavior.Merge => SynapseLinkBackfillMergeBatch(tableName,
        schema,
        sinkSettings.targetTableFullName,
        archiveTableSettings.archiveTableFullName,
        tablePropertiesSettings)

object MicrosoftSynapseLinkDataProviderImpl:

  type Environment = CdmTableStream
    & MicrosoftSynapseLinkStreamContext
    & ParallelismSettings
    & CdmGroupingProcessor
    & TableManager
    & TargetTableSettings
    & JdbcConsumer
    & BatchProcessor[IncomingBatch, InFlightBatch]
    & BatchProcessor[InFlightBatch, CompletedBatch]
    & TablePropertiesSettings
    & FieldFilteringProcessor
    & BackfillSettings
    & ArchiveTableSettings

  def apply(cdmTableStream: CdmTableStream,
            jdbcConsumer: JdbcConsumer,
            streamContext: MicrosoftSynapseLinkStreamContext,
            parallelismSettings: ParallelismSettings,
            groupingProcessor: CdmGroupingProcessor,
            stageProcessor: BatchProcessor[IncomingBatch, InFlightBatch],
            archivationProcessor: BatchProcessor[InFlightBatch, CompletedBatch],
            tableManager: TableManager,
            sinkSettings: TargetTableSettings,
            archiveTableSettings: ArchiveTableSettings,
            fieldFilteringProcessor: FieldFilteringProcessor,
            backfillSettings: BackfillSettings,
            tablePropertiesSettings: TablePropertiesSettings): MicrosoftSynapseLinkDataProviderImpl =
    new MicrosoftSynapseLinkDataProviderImpl(
      cdmTableStream,
      jdbcConsumer,
      streamContext,
      parallelismSettings,
      groupingProcessor,
      stageProcessor,
      archivationProcessor,
      tableManager,
      sinkSettings,
      archiveTableSettings,
      fieldFilteringProcessor,
      backfillSettings,
      tablePropertiesSettings)

  def layer: ZLayer[Environment, Nothing, MicrosoftSynapseLinkDataProvider] =
    ZLayer {
      for
        cdmTableStream <- ZIO.service[CdmTableStream]
        jdbcConsumer <- ZIO.service[JdbcConsumer]
        streamContext <- ZIO.service[MicrosoftSynapseLinkStreamContext]
        parallelismSettings <- ZIO.service[ParallelismSettings]
        groupingProcessor <- ZIO.service[CdmGroupingProcessor]
        tableManager <- ZIO.service[TableManager]
        sinkSettings <- ZIO.service[TargetTableSettings]
        stageProcessor <- ZIO.service[BatchProcessor[IncomingBatch, InFlightBatch]]
        archivationProcessor <- ZIO.service[BatchProcessor[InFlightBatch, CompletedBatch]]
        tablePropertiesSettings <- ZIO.service[TablePropertiesSettings]
        fieldFilteringProcessor <- ZIO.service[FieldFilteringProcessor]
        backfillSettings <- ZIO.service[BackfillSettings]
        archiveTableSettings <- ZIO.service[ArchiveTableSettings]
      yield MicrosoftSynapseLinkDataProviderImpl(cdmTableStream,
        jdbcConsumer,
        streamContext,
        parallelismSettings,
        groupingProcessor,
        stageProcessor,
        archivationProcessor,
        tableManager,
        sinkSettings,
        archiveTableSettings,
        fieldFilteringProcessor,
        backfillSettings,
        tablePropertiesSettings)
    }

