package com.sneaksanddata.arcane.microsoft_synapse_link
package models.app

import models.app.contracts.{OptimizeSettingsSpec, SnapshotExpirationSettingsSpec, StreamSpec, given_Conversion_TablePropertiesSettings_TableProperties}

import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings
import com.sneaksanddata.arcane.framework.models.settings.TableFormat.PARQUET
import com.sneaksanddata.arcane.framework.models.settings.{BackfillBehavior, BackfillSettings, FieldSelectionRule, FieldSelectionRuleSettings, GroupingSettings, OptimizeSettings, OrphanFilesExpirationSettings, SnapshotExpirationSettings, StagingDataSettings, TableFormat, TableMaintenanceSettings, TablePropertiesSettings, TargetTableSettings, VersionedDataGraphBuilderSettings}
import com.sneaksanddata.arcane.framework.services.base.MergeServiceClient
import com.sneaksanddata.arcane.framework.services.cdm.CdmTableSettings
import com.sneaksanddata.arcane.framework.services.lakehouse.IcebergCatalogCredential
import com.sneaksanddata.arcane.framework.services.lakehouse.base.{IcebergCatalogSettings, S3CatalogFileIO}
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClientOptions
import zio.ZLayer

import java.time.format.DateTimeFormatter
import java.time.{Duration, OffsetDateTime, ZoneOffset}
import java.util.UUID
import scala.util.Try

trait AzureConnectionSettings:
  val endpoint: String
  val container: String
  val account: String
  val accessKey: String

trait ParallelismSettings:
  val parallelism: Int


trait GraphExecutionSettings:
  val sourceDeleteDryRun: Boolean
  
/**
 * The context for the SQL Server Change Tracking stream.
 *
 * @param spec The stream specification
 */
case class MicrosoftSynapseLinkStreamContext(spec: StreamSpec) extends StreamContext
  with GroupingSettings
  with IcebergCatalogSettings
  with JdbcMergeServiceClientOptions
  with VersionedDataGraphBuilderSettings
  with AzureConnectionSettings
  with TargetTableSettings
  with ParallelismSettings
  with TablePropertiesSettings
  with FieldSelectionRuleSettings
  with BackfillSettings
  with StagingDataSettings
  with GraphExecutionSettings:

  override val rowsPerGroup: Int = System.getenv().getOrDefault("STREAMCONTEXT__ROWS_PER_GROUP", spec.rowsPerGroup.toString).toInt
  
  override val lookBackInterval: Duration = Duration.ofSeconds(spec.lookBackInterval)
  override val changeCaptureInterval: Duration = Duration.ofSeconds(spec.sourceSettings.changeCaptureIntervalSeconds)
  override val groupingInterval: Duration = Duration.ofSeconds(spec.groupingIntervalSeconds)

  override val namespace: String = spec.stagingDataSettings.catalog.namespace
  override val warehouse: String = spec.stagingDataSettings.catalog.warehouse
  override val catalogUri: String = spec.stagingDataSettings.catalog.catalogUri
  override val stagingLocation: Option[String] = spec.stagingDataSettings.dataLocation

  override val additionalProperties: Map[String, String] = sys.env.get("ARCANE_FRAMEWORK__CATALOG_NO_AUTH") match
    case Some(_) => Map()
    case None => IcebergCatalogCredential.oAuth2Properties

  override val s3CatalogFileIO: S3CatalogFileIO = S3CatalogFileIO

  override val connectionUrl: String = sys.env("ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI")

  override val targetTableFullName: String = spec.sinkSettings.targetTableName

  override val maintenanceSettings: TableMaintenanceSettings = new TableMaintenanceSettings:
    override val targetOptimizeSettings: Option[OptimizeSettings] = Some(new OptimizeSettings{
      override val batchThreshold: Int = spec.sinkSettings.optimizeSettings.batchThreshold
      override val fileSizeThreshold: String = spec.sinkSettings.optimizeSettings.fileSizeThreshold
    })

    override val targetSnapshotExpirationSettings: Option[SnapshotExpirationSettings] = Some(new SnapshotExpirationSettings {
      override val batchThreshold: Int = spec.sinkSettings.snapshotExpirationSettings.batchThreshold
      override val retentionThreshold: String = spec.sinkSettings.snapshotExpirationSettings.retentionThreshold
    })

    override val targetOrphanFilesExpirationSettings: Option[OrphanFilesExpirationSettings] = Some(new OrphanFilesExpirationSettings {
      override val batchThreshold: Int = spec.sinkSettings.orphanFilesExpirationSettings.batchThreshold
      override val retentionThreshold: String = spec.sinkSettings.orphanFilesExpirationSettings.retentionThreshold

    })

  override val endpoint: String = sys.env("ARCANE_FRAMEWORK__STORAGE_ENDPOINT")
  override val container: String = sys.env("ARCANE_FRAMEWORK__STORAGE_CONTAINER")
  override val account: String = sys.env("ARCANE_FRAMEWORK__STORAGE_ACCOUNT")
  override val accessKey: String = sys.env("ARCANE_FRAMEWORK__STORAGE_ACCESS_KEY")
  
  override val parallelism: Int = 16
  
  val sourceDeleteDryRun: Boolean = sys.env.get("ARCANE_FRAMEWORK__SOURCE_DELETE_DRY_RUN").exists(v => v.toLowerCase == "true")

  override val stagingTablePrefix: String = spec.stagingDataSettings.tableNamePrefix

  val stagingCatalogName: String = spec.stagingDataSettings.catalog.catalogName
  val stagingSchemaName: String = spec.stagingDataSettings.catalog.schemaName

  val partitionExpressions: Array[String] = spec.tableProperties.partitionExpressions
  val tableProperties: TablePropertiesSettings = spec.tableProperties
  
  val format: TableFormat = TableFormat.valueOf(spec.tableProperties.format)
  val sortedBy: Array[String] = spec.tableProperties.sortedBy
  val parquetBloomFilterColumns: Array[String] = spec.tableProperties.parquetBloomFilterColumns
  
  val backfillTableFullName: String = s"$stagingCatalogName.$stagingSchemaName.${stagingTablePrefix}__backfill_${UUID.randomUUID().toString}".replace('-', '_')
  
  val changeCapturePeriod: Duration = Duration.ofSeconds(spec.sourceSettings.changeCapturePeriodSeconds)

  override val rule: FieldSelectionRule = spec.fieldSelectionRule.ruleType match
    case "include" => FieldSelectionRule.IncludeFields(spec.fieldSelectionRule.fields.map(f => f.toLowerCase()).toSet)
    case "exclude" => FieldSelectionRule.ExcludeFields(spec.fieldSelectionRule.fields.map(f => f.toLowerCase()).toSet)
    case _ => FieldSelectionRule.AllFields

  override val backfillBehavior: BackfillBehavior = spec.backfillBehavior match
    case "merge" => BackfillBehavior.Merge
    case "overwrite" => BackfillBehavior.Overwrite
    case _ => throw new IllegalArgumentException(s"Unknown backfill behavior: ${spec.backfillBehavior}")

  override val backfillStartDate: Option[OffsetDateTime] = parseBackfillStartDate(spec.backfillStartDate)

  private def parseBackfillStartDate(str: String): Option[OffsetDateTime] =
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ss'Z'").withZone(ZoneOffset.UTC)
    Try(OffsetDateTime.parse(str, formatter)) match
      case scala.util.Success(value) => Some(value)
      case scala.util.Failure(e) => throw new IllegalArgumentException(s"Invalid backfill start date: $str. The backfill start date must be in the format 'yyyy-MM-dd'T'HH.mm.ss'Z'", e)

given Conversion[StreamSpec, CdmTableSettings] with
  def apply(spec: StreamSpec): CdmTableSettings = CdmTableSettings(spec.sourceSettings.name.toLowerCase, spec.sourceSettings.baseLocation, None)


object MicrosoftSynapseLinkStreamContext {

  type Environment = StreamContext
    & CdmTableSettings
    & GroupingSettings
    & VersionedDataGraphBuilderSettings
    & IcebergCatalogSettings
    & JdbcMergeServiceClientOptions
    & AzureConnectionSettings
    & TargetTableSettings
    & MicrosoftSynapseLinkStreamContext
    & GraphExecutionSettings
    & TablePropertiesSettings
    & FieldSelectionRuleSettings
    & BackfillSettings
    & StagingDataSettings

  /**
   * The ZLayer that creates the VersionedDataGraphBuilder.
   */
  val layer: ZLayer[Any, Throwable, Environment] = StreamSpec
      .fromEnvironment("STREAMCONTEXT__SPEC")
      .map(combineSettingsLayer)
      .getOrElse(ZLayer.fail(new Exception("The stream context is not specified.")))

  private def combineSettingsLayer(spec: StreamSpec): ZLayer[Any, Throwable, Environment] =
    val context = MicrosoftSynapseLinkStreamContext(spec)
    val cdmTableSettings = CdmTableSettings(spec.sourceSettings.name.toLowerCase, spec.sourceSettings.baseLocation, None)

    ZLayer.succeed(context) ++ ZLayer.succeed(cdmTableSettings)
}
