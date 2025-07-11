package com.sneaksanddata.arcane.microsoft_synapse_link
package models.app

import models.app.contracts.StreamSpec

import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings
import com.sneaksanddata.arcane.framework.models.settings.*
import com.sneaksanddata.arcane.framework.services.iceberg.IcebergCatalogCredential
import com.sneaksanddata.arcane.framework.services.iceberg.base.S3CatalogFileIO
import zio.ZLayer
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig

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

/** The context for the SQL Server Change Tracking stream.
  *
  * @param spec
  *   The stream specification
  */
case class MicrosoftSynapseLinkStreamContext(spec: StreamSpec)
    extends StreamContext
    with GroupingSettings
    with IcebergCatalogSettings
    with JdbcMergeServiceClientSettings
    with VersionedDataGraphBuilderSettings
    with AzureConnectionSettings
    with TargetTableSettings
    with ParallelismSettings
    with TablePropertiesSettings
    with FieldSelectionRuleSettings
    with BackfillSettings
    with StagingDataSettings
    with GraphExecutionSettings
    with SynapseSourceSettings
    with SourceBufferingSettings:

  override val rowsPerGroup: Int =
    System.getenv().getOrDefault("STREAMCONTEXT__ROWS_PER_GROUP", spec.rowsPerGroup.toString).toInt

  override val lookBackInterval: Duration      = Duration.ofSeconds(spec.lookBackInterval)
  override val changeCaptureInterval: Duration = Duration.ofSeconds(spec.sourceSettings.changeCaptureIntervalSeconds)
  override val groupingInterval: Duration      = Duration.ofSeconds(spec.groupingIntervalSeconds)

  override val namespace: String               = spec.stagingDataSettings.catalog.namespace
  override val warehouse: String               = spec.stagingDataSettings.catalog.warehouse
  override val catalogUri: String              = spec.stagingDataSettings.catalog.catalogUri
  override val stagingLocation: Option[String] = spec.stagingDataSettings.dataLocation

  override val additionalProperties: Map[String, String] = sys.env.get("ARCANE_FRAMEWORK__CATALOG_NO_AUTH") match
    case Some(_) => Map()
    case None    => IcebergCatalogCredential.oAuth2Properties

  override val s3CatalogFileIO: S3CatalogFileIO = S3CatalogFileIO

  override val connectionUrl: String = sys.env("ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI")

  override val targetTableFullName: String = spec.sinkSettings.targetTableName

  override val maintenanceSettings: TableMaintenanceSettings = new TableMaintenanceSettings:
    override val targetOptimizeSettings: Option[OptimizeSettings] = Some(new OptimizeSettings {
      override val batchThreshold: Int       = spec.sinkSettings.optimizeSettings.batchThreshold
      override val fileSizeThreshold: String = spec.sinkSettings.optimizeSettings.fileSizeThreshold
    })

    override val targetSnapshotExpirationSettings: Option[SnapshotExpirationSettings] = Some(
      new SnapshotExpirationSettings {
        override val batchThreshold: Int        = spec.sinkSettings.snapshotExpirationSettings.batchThreshold
        override val retentionThreshold: String = spec.sinkSettings.snapshotExpirationSettings.retentionThreshold
      }
    )

    override val targetOrphanFilesExpirationSettings: Option[OrphanFilesExpirationSettings] = Some(
      new OrphanFilesExpirationSettings {
        override val batchThreshold: Int        = spec.sinkSettings.orphanFilesExpirationSettings.batchThreshold
        override val retentionThreshold: String = spec.sinkSettings.orphanFilesExpirationSettings.retentionThreshold

      }
    )

  override val endpoint: String  = sys.env("ARCANE_FRAMEWORK__STORAGE_ENDPOINT")
  override val container: String = sys.env("ARCANE_FRAMEWORK__STORAGE_CONTAINER")
  override val account: String   = sys.env("ARCANE_FRAMEWORK__STORAGE_ACCOUNT")
  override val accessKey: String = sys.env("ARCANE_FRAMEWORK__STORAGE_ACCESS_KEY")

  override val parallelism: Int = 16

  val sourceDeleteDryRun: Boolean =
    sys.env.get("ARCANE_FRAMEWORK__SOURCE_DELETE_DRY_RUN").exists(v => v.toLowerCase == "true")

  override val stagingTablePrefix: String = spec.stagingDataSettings.tableNamePrefix

  val stagingCatalogName: String = spec.stagingDataSettings.catalog.catalogName
  val stagingSchemaName: String  = spec.stagingDataSettings.catalog.schemaName

  val partitionExpressions: Array[String] = spec.tableProperties.partitionExpressions

  val format: TableFormat                      = TableFormat.valueOf(spec.tableProperties.format)
  val sortedBy: Array[String]                  = spec.tableProperties.sortedBy
  val parquetBloomFilterColumns: Array[String] = spec.tableProperties.parquetBloomFilterColumns

  val backfillTableFullName: String =
    s"$stagingCatalogName.$stagingSchemaName.${stagingTablePrefix}__backfill_${UUID.randomUUID().toString}"
      .replace('-', '_')

  override val rule: FieldSelectionRule = spec.fieldSelectionRule.ruleType match
    case "include" => FieldSelectionRule.IncludeFields(spec.fieldSelectionRule.fields.map(f => f.toLowerCase()).toSet)
    case "exclude" => FieldSelectionRule.ExcludeFields(spec.fieldSelectionRule.fields.map(f => f.toLowerCase()).toSet)
    case _         => FieldSelectionRule.AllFields

  override val essentialFields: Set[String] = Set("id", "versionnumber", "isdelete", "arcane_merge_key")

  override val backfillBehavior: BackfillBehavior = spec.backfillBehavior match
    case "merge"     => BackfillBehavior.Merge
    case "overwrite" => BackfillBehavior.Overwrite
    case _           => throw new IllegalArgumentException(s"Unknown backfill behavior: ${spec.backfillBehavior}")

  override val backfillStartDate: Option[OffsetDateTime] = parseBackfillStartDate(spec.backfillStartDate)

  override val entityName: String                = spec.sourceSettings.name
  override val baseLocation: String              = spec.sourceSettings.baseLocation
  override val changeCaptureIntervalSeconds: Int = spec.sourceSettings.changeCaptureIntervalSeconds

  override val maxRowsPerFile: Option[Int] = Some(spec.stagingDataSettings.maxRowsPerFile)

  private def parseBackfillStartDate(str: String): Option[OffsetDateTime] =
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ss'Z'").withZone(ZoneOffset.UTC)
    Try(OffsetDateTime.parse(str, formatter)) match
      case scala.util.Success(value) => Some(value)
      case scala.util.Failure(e) =>
        throw new IllegalArgumentException(
          s"Invalid backfill start date: $str. The backfill start date must be in the format 'yyyy-MM-dd'T'HH.mm.ss'Z'",
          e
        )

  override val bufferingEnabled: Boolean            = false
  override val bufferingStrategy: BufferingStrategy = BufferingStrategy.Buffering(0)

  override val isUnifiedSchema: Boolean = false

  override val isServerSide: Boolean = false

  val datadogSocketPath: String =
    sys.env.getOrElse("ARCANE_FRAMEWORK__DATADOG_SOCKET_PATH", "/var/run/datadog/dsd.socket")
  val metricsPublisherInterval: Duration = Duration.ofMillis(
    sys.env.getOrElse("ARCANE_FRAMEWORK__METRICS_PUBLISHER_INTERVAL_MILLIS", "100").toInt
  )

given Conversion[MicrosoftSynapseLinkStreamContext, DatagramSocketConfig] with
  def apply(context: MicrosoftSynapseLinkStreamContext): DatagramSocketConfig =
    DatagramSocketConfig(context.datadogSocketPath)

given Conversion[MicrosoftSynapseLinkStreamContext, MetricsConfig] with
  def apply(context: MicrosoftSynapseLinkStreamContext): MetricsConfig =
    MetricsConfig(context.metricsPublisherInterval)

object MicrosoftSynapseLinkStreamContext {

  type Environment = StreamContext & GroupingSettings & VersionedDataGraphBuilderSettings & IcebergCatalogSettings &
    JdbcMergeServiceClientSettings & AzureConnectionSettings & TargetTableSettings & MicrosoftSynapseLinkStreamContext &
    GraphExecutionSettings & TablePropertiesSettings & FieldSelectionRuleSettings & BackfillSettings &
    StagingDataSettings & SynapseSourceSettings & SourceBufferingSettings & MetricsConfig & DatagramSocketConfig &
    DatadogPublisherConfig

  /** The ZLayer that creates the VersionedDataGraphBuilder.
    */
  val layer: ZLayer[Any, Throwable, Environment] = StreamSpec
    .fromEnvironment("STREAMCONTEXT__SPEC")
    .map(combineSettingsLayer)
    .getOrElse(ZLayer.fail(new Exception("The stream context is not specified.")))

  private def combineSettingsLayer(spec: StreamSpec): ZLayer[Any, Throwable, Environment] =
    val context = MicrosoftSynapseLinkStreamContext(spec)

    ZLayer.succeed(context)
      ++ ZLayer.succeed[DatagramSocketConfig](context)
      ++ ZLayer.succeed[MetricsConfig](context)
      ++ ZLayer.succeed(DatadogPublisherConfig())
}
