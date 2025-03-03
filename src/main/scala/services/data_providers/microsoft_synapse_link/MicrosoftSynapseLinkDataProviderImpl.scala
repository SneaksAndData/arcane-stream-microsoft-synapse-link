package com.sneaksanddata.arcane.microsoft_synapse_link
package services.data_providers.microsoft_synapse_link

import extensions.DataRowExtensions.schema
import models.app.*
import models.app.streaming.SourceCleanupRequest
import services.data_providers.microsoft_synapse_link.{CdmTableStream, DataStreamElement}
import services.streaming.consumers.{CompletedBatch, InFlightBatch, IncomingBatch, IndexedStagedBatchesImpl}
import services.streaming.processors.{CdmGroupingProcessor, FieldFilteringProcessor}

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.models.querygen.MergeQuery
import com.sneaksanddata.arcane.framework.models.settings.{ArchiveTableSettings, TableMaintenanceSettings, TablePropertiesSettings, TargetTableSettings}
import com.sneaksanddata.arcane.framework.models.{ArcaneSchema, DataRow}
import com.sneaksanddata.arcane.framework.services.base.TableManager
import com.sneaksanddata.arcane.framework.services.consumers.{ArchiveableBatch, MergeableBatch, StagedBackfillBatch, StagedBackfillOverwriteBatch, StagedVersionedBatch, SynapseLinkBackfillMergeBatch, SynapseLinkBackfillOverwriteBatch, SynapseLinkMergeBatch}
import com.sneaksanddata.arcane.framework.services.lakehouse.given_Conversion_ArcaneSchema_Schema
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClient
import com.sneaksanddata.arcane.framework.services.streaming.base.{BackfillDataProvider, BatchProcessor, MetadataEnrichedRowStreamElement, OptimizationRequestConvertable, OrphanFilesExpirationRequestConvertable, SnapshotExpirationRequestConvertable}
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.{ArchivationProcessor, MergeBatchProcessor}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.{IndexedStagedBatches, StagingProcessor}
import com.sneaksanddata.arcane.framework.services.merging.models.{JdbcOptimizationRequest, JdbcOrphanFilesExpirationRequest, JdbcSnapshotExpirationRequest}
import com.sneaksanddata.arcane.microsoft_synapse_link.services.clients.JdbcConsumer
import org.apache.iceberg.{Schema, Table}
import zio.stream.{ZPipeline, ZStream}
import zio.{Chunk, Task, UIO, URIO, ZIO, ZLayer}

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.UUID
import com.sneaksanddata.arcane.microsoft_synapse_link.services.streaming.consumers.DataStreamElementExtensions.given_MetadataEnrichedRowStreamElement_DataStreamElement
import com.sneaksanddata.arcane.framework.models.settings.OptimizeSettings
import com.sneaksanddata.arcane.framework.models.settings.SnapshotExpirationSettings
import com.sneaksanddata.arcane.framework.models.settings.OrphanFilesExpirationSettings

type BackfillBatchInFlight = StagedBackfillBatch

trait MicrosoftSynapseLinkDataProvider:

  def requestBackfill: Task[BackfillBatchInFlight]

case class BackfillTempTableSettings(override val targetTableFullName: String) extends TargetTableSettings:
  override val maintenanceSettings: TableMaintenanceSettings = new TableMaintenanceSettings {
    override val targetOptimizeSettings: Option[OptimizeSettings] = None
    override val targetSnapshotExpirationSettings: Option[SnapshotExpirationSettings] = None
    override val targetOrphanFilesExpirationSettings: Option[OrphanFilesExpirationSettings] = None
  }


class MicrosoftSynapseLinkDataProviderImpl(cdmTableStream: CdmTableStream,
                                           jdbcConsumer: JdbcConsumer,
                                           streamContext: MicrosoftSynapseLinkStreamContext,
                                           parallelismSettings: ParallelismSettings,
                                           groupingProcessor: CdmGroupingProcessor,
                                           stageProcessor: StagingProcessor,
                                           archivationProcessor: ArchivationProcessor,
                                           tableManager: TableManager,
                                           sinkSettings: TargetTableSettings,
                                           archiveTableSettings: ArchiveTableSettings,
                                           fieldFilteringProcessor: FieldFilteringProcessor,
                                           backfillSettings: BackfillSettings,
                                           jdbcMergeServiceClient: JdbcMergeServiceClient,
                                           tablePropertiesSettings: TablePropertiesSettings) extends MicrosoftSynapseLinkDataProvider:

  private val backFillTableName = streamContext.backfillTableName
  private val tempTargetTableSettings = BackfillTempTableSettings(backFillTableName)
  private val mergeProcessor = MergeBatchProcessor(jdbcMergeServiceClient, jdbcMergeServiceClient, tempTargetTableSettings)

  def requestBackfill: Task[BackfillBatchInFlight] =

    for
      _ <- zlog(s"Starting backfill process. Backfill behavior: ${backfillSettings.backfillBehavior}")
      cleanupRequests <- backfillStream.runDrain
      _ <- zlog("Backfill process completed")
      backFillCompletionBatch <- createBackfillBatch(backFillTableName)
    yield backFillCompletionBatch

  private def backfillStream = 
    cdmTableStream.getPrefixesFromBeginning
      .mapZIOPar(parallelismSettings.parallelism)(blob => cdmTableStream.getStream(blob))
      .flatMap(reader => cdmTableStream.getData(reader))
      .via(fieldFilteringProcessor.process)
      .via(groupingProcessor.process)
      .via(stageProcessor.process(toInFlightBatch))
      .via(mergeProcessor.process)
      .via(archivationProcessor.process)

  def toInFlightBatch(batches: Iterable[StagedVersionedBatch & MergeableBatch & ArchiveableBatch], index: Long, others: Any): MergeBatchProcessor#BatchType =
    new IndexedStagedBatchesImpl(batches, index)

  private def createBackfillBatch(tableName: String): Task[StagedBackfillBatch] =
    for schema <- jdbcMergeServiceClient.getSchemaProvider(tableName).getSchema
    yield backfillSettings.backfillBehavior match
      case BackfillBehavior.Overwrite => SynapseLinkBackfillOverwriteBatch(tableName,
        schema,
        sinkSettings.targetTableFullName,
        archiveTableSettings.fullName,
        tablePropertiesSettings)
      case BackfillBehavior.Merge => SynapseLinkBackfillMergeBatch(tableName,
        schema,
        sinkSettings.targetTableFullName,
        archiveTableSettings.fullName,
        tablePropertiesSettings)

object MicrosoftSynapseLinkDataProviderImpl:

  type Environment = CdmTableStream
    & MicrosoftSynapseLinkStreamContext
    & ParallelismSettings
    & CdmGroupingProcessor
    & TableManager
    & TargetTableSettings
    & JdbcConsumer
    & StagingProcessor
    & ArchivationProcessor
    & TablePropertiesSettings
    & FieldFilteringProcessor
    & BackfillSettings
    & ArchiveTableSettings
    & JdbcMergeServiceClient

  def apply(cdmTableStream: CdmTableStream,
            jdbcConsumer: JdbcConsumer,
            streamContext: MicrosoftSynapseLinkStreamContext,
            parallelismSettings: ParallelismSettings,
            groupingProcessor: CdmGroupingProcessor,
            stageProcessor: StagingProcessor,
            archivationProcessor: ArchivationProcessor,
            tableManager: TableManager,
            sinkSettings: TargetTableSettings,
            archiveTableSettings: ArchiveTableSettings,
            fieldFilteringProcessor: FieldFilteringProcessor,
            backfillSettings: BackfillSettings,
            jdbcMergeServiceClient: JdbcMergeServiceClient,
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
      jdbcMergeServiceClient,
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
        stageProcessor <- ZIO.service[StagingProcessor]
        archivationProcessor <- ZIO.service[ArchivationProcessor]
        tablePropertiesSettings <- ZIO.service[TablePropertiesSettings]
        fieldFilteringProcessor <- ZIO.service[FieldFilteringProcessor]
        backfillSettings <- ZIO.service[BackfillSettings]
        archiveTableSettings <- ZIO.service[ArchiveTableSettings]
        jdbcMergeServiceClient <- ZIO.service[JdbcMergeServiceClient]
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
        jdbcMergeServiceClient,
        tablePropertiesSettings)
    }

