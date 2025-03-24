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
import com.sneaksanddata.arcane.framework.services.lakehouse.base.{CatalogWriter, CatalogWriterBuilder, IcebergCatalogSettings}
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClient
import com.sneaksanddata.arcane.framework.services.streaming.base.HookManager
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.streaming.{DisposeBatchProcessor, MergeBatchProcessor}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.StagingProcessor
import com.sneaksanddata.arcane.microsoft_synapse_link.services.data_providers.microsoft_synapse_link.base.MicrosoftSynapseLinkBackfillDataProvider
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.{Task, ZIO, ZLayer}


case class BackfillTempTableSettings(override val targetTableFullName: String) extends TargetTableSettings:
  override val maintenanceSettings: TableMaintenanceSettings = new TableMaintenanceSettings {
    override val targetOptimizeSettings: Option[OptimizeSettings] = None
    override val targetSnapshotExpirationSettings: Option[SnapshotExpirationSettings] = None
    override val targetOrphanFilesExpirationSettings: Option[OrphanFilesExpirationSettings] = None
  }


class MicrosoftSynapseLinkBackfillOverwriteDataProvider(cdmTableStream: CdmTableStream,
                                                        streamContext: MicrosoftSynapseLinkStreamContext,
                                                        parallelismSettings: ParallelismSettings,
                                                        groupingProcessor: CdmGroupingProcessor,
                                                        tableManager: TableManager,
                                                        sinkSettings: TargetTableSettings,
                                                        fieldFilteringProcessor: FieldFilteringProcessor,
                                                        backfillSettings: BackfillSettings,
                                                        jdbcMergeServiceClient: JdbcMergeServiceClient,
                                                        tablePropertiesSettings: TablePropertiesSettings,
                                                        stagingDataSettings: StagingDataSettings,
                                                        targetTableSettings: TargetTableSettings,
                                                        icebergCatalogSettings: IcebergCatalogSettings,
                                                        catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
                                                        disposeBatchProcessor: DisposeBatchProcessor,
                                                        hookManager: HookManager) extends MicrosoftSynapseLinkBackfillDataProvider:

  private val backFillTableName = streamContext.backfillTableFullName
  private val tempTargetTableSettings = BackfillTempTableSettings(backFillTableName)
  private val mergeProcessor = MergeBatchProcessor(jdbcMergeServiceClient, jdbcMergeServiceClient, tempTargetTableSettings)
  
  private val stagingProcessor = StagingProcessor(stagingDataSettings, tablePropertiesSettings, tempTargetTableSettings, icebergCatalogSettings, catalogWriter)

  def requestBackfill: Task[StagedBackfillOverwriteBatch] =
    for
      _ <- backfillSettings.backfillBehavior match
        case BackfillBehavior.Merge => ZIO.die(new IllegalArgumentException("Running backfill merge in overwrite runner"))
        case BackfillBehavior.Overwrite => ZIO.unit
      _ <- zlog(s"Starting backfill process. Backfill behavior: ${backfillSettings.backfillBehavior}")
      cleanupRequests <- backfillStream.runDrain
      _ <- zlog("Backfill process completed")
      backFillCompletionBatch <- createBackfillBatch(backFillTableName)
    yield backFillCompletionBatch

  private def backfillStream =
    cdmTableStream.getPrefixesFromDate(backfillSettings.backfillStartDate.getOrElse(throw new IllegalArgumentException("Backfill start date is not set")))
      .mapZIOPar(parallelismSettings.parallelism)(blob => cdmTableStream.getStream(blob))
      .flatMap(reader => cdmTableStream.getData(reader))
      .via(fieldFilteringProcessor.process)
      .via(groupingProcessor.process)
      .via(stagingProcessor.process(toInFlightBatch, hookManager.onBatchStaged))
      .via(mergeProcessor.process)
      .via(disposeBatchProcessor.process)

  def toInFlightBatch(batches: Iterable[StagedVersionedBatch & MergeableBatch], index: Long, others: Any): MergeBatchProcessor#BatchType =
    new IndexedStagedBatchesImpl(batches, index)

  private def createBackfillBatch(tableName: String): Task[StagedBackfillOverwriteBatch] =
    for schema <- jdbcMergeServiceClient.getSchema(tableName)
    yield SynapseLinkBackfillOverwriteBatch(tableName,
        schema,
        sinkSettings.targetTableFullName,
        tablePropertiesSettings)

object MicrosoftSynapseLinkBackfillOverwriteDataProvider:

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
    & StagingDataSettings
    & TargetTableSettings
    & IcebergCatalogSettings
    & CatalogWriter[RESTCatalog, Table, Schema] 
    & HookManager

  def apply(cdmTableStream: CdmTableStream,
            streamContext: MicrosoftSynapseLinkStreamContext,
            parallelismSettings: ParallelismSettings,
            groupingProcessor: CdmGroupingProcessor,
            tableManager: TableManager,
            sinkSettings: TargetTableSettings,
            fieldFilteringProcessor: FieldFilteringProcessor,
            backfillSettings: BackfillSettings,
            jdbcMergeServiceClient: JdbcMergeServiceClient,
            tablePropertiesSettings: TablePropertiesSettings,
            stagingDataSettings: StagingDataSettings,
            targetTableSettings: TargetTableSettings,
            icebergCatalogSettings: IcebergCatalogSettings,
            catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
            disposeBatchProcessor: DisposeBatchProcessor,
            hookManager: HookManager): MicrosoftSynapseLinkBackfillOverwriteDataProvider =
    new MicrosoftSynapseLinkBackfillOverwriteDataProvider(
      cdmTableStream,
      streamContext,
      parallelismSettings,
      groupingProcessor,
      tableManager,
      sinkSettings,
      fieldFilteringProcessor,
      backfillSettings,
      jdbcMergeServiceClient,
      tablePropertiesSettings,
      stagingDataSettings,
      targetTableSettings,
      icebergCatalogSettings,
      catalogWriter,
      disposeBatchProcessor,
      hookManager)

  def layer: ZLayer[Environment, Nothing, MicrosoftSynapseLinkBackfillOverwriteDataProvider] =
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
        stagingDataSettings <- ZIO.service[StagingDataSettings]
        targetTableSettings <- ZIO.service[TargetTableSettings]
        icebergCatalogSettings <- ZIO.service[IcebergCatalogSettings]
        catalogWriter<- ZIO.service[CatalogWriter[RESTCatalog, Table, Schema]]
        hookManager <- ZIO.service[HookManager]
      yield MicrosoftSynapseLinkBackfillOverwriteDataProvider(cdmTableStream,
        streamContext,
        parallelismSettings,
        groupingProcessor,
        tableManager,
        sinkSettings,
        fieldFilteringProcessor,
        backfillSettings,
        jdbcMergeServiceClient,
        tablePropertiesSettings,
        stagingDataSettings,
        targetTableSettings,
        icebergCatalogSettings,
        catalogWriter,
        disposeBatchProcessor,
        hookManager)
    }

