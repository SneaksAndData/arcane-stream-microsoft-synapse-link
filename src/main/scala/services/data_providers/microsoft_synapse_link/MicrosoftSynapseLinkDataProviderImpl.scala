package com.sneaksanddata.arcane.microsoft_synapse_link
package services.data_providers.microsoft_synapse_link

import models.app.*
import services.streaming.consumers.DataStreamElementExtensions.given_MetadataEnrichedRowStreamElement_DataStreamElement
import services.streaming.consumers.IndexedStagedBatchesImpl
import services.streaming.processors.{CdmGroupingProcessor, FieldFilteringProcessor}

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.models.settings.*
import com.sneaksanddata.arcane.framework.services.base.TableManager
import com.sneaksanddata.arcane.framework.services.consumers.*
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClient
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.{DisposeBatchProcessor, MergeBatchProcessor}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.StagingProcessor
import zio.{Task, ZIO, ZLayer}

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
                                           streamContext: MicrosoftSynapseLinkStreamContext,
                                           parallelismSettings: ParallelismSettings,
                                           groupingProcessor: CdmGroupingProcessor,
                                           stageProcessor: StagingProcessor,
                                           tableManager: TableManager,
                                           sinkSettings: TargetTableSettings,
                                           fieldFilteringProcessor: FieldFilteringProcessor,
                                           backfillSettings: BackfillSettings,
                                           jdbcMergeServiceClient: JdbcMergeServiceClient,
                                           tablePropertiesSettings: TablePropertiesSettings,
                                           disposeBatchProcessor: DisposeBatchProcessor) extends MicrosoftSynapseLinkDataProvider:

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
      .via(disposeBatchProcessor.process)

  def toInFlightBatch(batches: Iterable[StagedVersionedBatch & MergeableBatch], index: Long, others: Any): MergeBatchProcessor#BatchType =
    new IndexedStagedBatchesImpl(batches, index)

  private def createBackfillBatch(tableName: String): Task[StagedBackfillBatch] =
    for schema <- jdbcMergeServiceClient.getSchemaProvider(tableName).getSchema
    yield backfillSettings.backfillBehavior match
      case BackfillBehavior.Overwrite => SynapseLinkBackfillOverwriteBatch(tableName,
        schema,
        sinkSettings.targetTableFullName,
        tablePropertiesSettings)
      case BackfillBehavior.Merge => SynapseLinkBackfillMergeBatch(tableName,
        schema,
        sinkSettings.targetTableFullName,
        tablePropertiesSettings)

object MicrosoftSynapseLinkDataProviderImpl:

  type Environment = CdmTableStream
    & MicrosoftSynapseLinkStreamContext
    & ParallelismSettings
    & CdmGroupingProcessor
    & TableManager
    & TargetTableSettings
    & StagingProcessor
    & TablePropertiesSettings
    & FieldFilteringProcessor
    & BackfillSettings
    & JdbcMergeServiceClient
    & DisposeBatchProcessor

  def apply(cdmTableStream: CdmTableStream,
            streamContext: MicrosoftSynapseLinkStreamContext,
            parallelismSettings: ParallelismSettings,
            groupingProcessor: CdmGroupingProcessor,
            stageProcessor: StagingProcessor,
            tableManager: TableManager,
            sinkSettings: TargetTableSettings,
            fieldFilteringProcessor: FieldFilteringProcessor,
            backfillSettings: BackfillSettings,
            jdbcMergeServiceClient: JdbcMergeServiceClient,
            tablePropertiesSettings: TablePropertiesSettings,
            disposeBatchProcessor: DisposeBatchProcessor): MicrosoftSynapseLinkDataProviderImpl =
    new MicrosoftSynapseLinkDataProviderImpl(
      cdmTableStream,
      streamContext,
      parallelismSettings,
      groupingProcessor,
      stageProcessor,
      tableManager,
      sinkSettings,
      fieldFilteringProcessor,
      backfillSettings,
      jdbcMergeServiceClient,
      tablePropertiesSettings,
      disposeBatchProcessor)

  def layer: ZLayer[Environment, Nothing, MicrosoftSynapseLinkDataProvider] =
    ZLayer {
      for
        cdmTableStream <- ZIO.service[CdmTableStream]
        streamContext <- ZIO.service[MicrosoftSynapseLinkStreamContext]
        parallelismSettings <- ZIO.service[ParallelismSettings]
        groupingProcessor <- ZIO.service[CdmGroupingProcessor]
        tableManager <- ZIO.service[TableManager]
        sinkSettings <- ZIO.service[TargetTableSettings]
        stageProcessor <- ZIO.service[StagingProcessor]
        tablePropertiesSettings <- ZIO.service[TablePropertiesSettings]
        fieldFilteringProcessor <- ZIO.service[FieldFilteringProcessor]
        backfillSettings <- ZIO.service[BackfillSettings]
        jdbcMergeServiceClient <- ZIO.service[JdbcMergeServiceClient]
        disposeBatchProcessor <- ZIO.service[DisposeBatchProcessor]
      yield MicrosoftSynapseLinkDataProviderImpl(cdmTableStream,
        streamContext,
        parallelismSettings,
        groupingProcessor,
        stageProcessor,
        tableManager,
        sinkSettings,
        fieldFilteringProcessor,
        backfillSettings,
        jdbcMergeServiceClient,
        tablePropertiesSettings,
        disposeBatchProcessor)
    }

