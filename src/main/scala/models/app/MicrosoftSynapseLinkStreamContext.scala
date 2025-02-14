package com.sneaksanddata.arcane.microsoft_synapse_link
package models.app

import models.app.contracts.{OptimizeSettingsSpec, StreamSpec}

import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings.TableFormat.PARQUET
import com.sneaksanddata.arcane.framework.models.settings.{GroupingSettings, TableFormat, TablePropertiesSettings, VersionedDataGraphBuilderSettings}
import com.sneaksanddata.arcane.framework.services.cdm.CdmTableSettings
import com.sneaksanddata.arcane.framework.services.consumers.JdbcConsumerOptions
import com.sneaksanddata.arcane.framework.services.lakehouse.base.IcebergCatalogSettings
import com.sneaksanddata.arcane.framework.services.lakehouse.{IcebergCatalogCredential, S3CatalogFileIO}
import zio.ZLayer

import java.time.Duration

trait AzureConnectionSettings:
  val endpoint: String
  val container: String
  val account: String
  val accessKey: String

trait OptimizeSettings:
  val batchThreshold: Int
  val fileSizeThreshold: String

trait TargetTableSettings:
  val targetTableFullName: String
  val targetOptimizeSettings: Option[OptimizeSettings]
  val targetSnapshotExpirationSettings: Option[SnapshotExpirationSettings]
  val targetOrphanFilesExpirationSettings: Option[OrphanFilesExpirationSettings]

trait ArchiveTableSettings:
  val archiveTableFullName: String
  val archiveOptimizeSettings: OptimizeSettings
  val archiveSnapshotExpirationSettings: SnapshotExpirationSettings
  val archiveOrphanFilesExpirationSettings: OrphanFilesExpirationSettings

trait ParallelismSettings:
  val parallelism: Int


trait SnapshotExpirationSettings:
  val batchThreshold: Int
  val retentionThreshold: String

trait GraphExecutionSettings:
  val sourceDeleteDryRun: Boolean
  

trait OrphanFilesExpirationSettings:
  val batchThreshold: Int
  val retentionThreshold: String

case class OptimizeSettingsImpl(batchThreshold: Int, fileSizeThreshold: String) extends OptimizeSettings

case class SnapshotExpirationSettingsImpl(batchThreshold: Int, retentionThreshold: String) extends SnapshotExpirationSettings

case class OrphanFilesExpirationSettingsImpl(batchThreshold: Int, retentionThreshold: String) extends OrphanFilesExpirationSettings

/**
 * The context for the SQL Server Change Tracking stream.
 *
 * @param spec The stream specification
 */
case class MicrosoftSynapseLinkStreamContext(spec: StreamSpec) extends StreamContext
  with GroupingSettings
  with IcebergCatalogSettings
  with JdbcConsumerOptions
  with VersionedDataGraphBuilderSettings
  with AzureConnectionSettings
  with TargetTableSettings
  with ArchiveTableSettings
  with ParallelismSettings
  with TablePropertiesSettings
  with GraphExecutionSettings:

  override val rowsPerGroup: Int = spec.rowsPerGroup
  override val lookBackInterval: Duration = Duration.ofSeconds(spec.lookBackInterval)
  override val changeCaptureInterval: Duration = Duration.ofSeconds(spec.sourceSettings.changeCaptureIntervalSeconds)
  override val groupingInterval: Duration = Duration.ofSeconds(spec.groupingIntervalSeconds)

  override val namespace: String = spec.stagingDataSettings.catalog.namespace
  override val warehouse: String = spec.stagingDataSettings.catalog.warehouse
  override val catalogUri: String = spec.stagingDataSettings.catalog.catalogUri
  override val stagingLocation: Option[String] = spec.stagingDataSettings.dataLocation

  override val additionalProperties: Map[String, String] = IcebergCatalogCredential.oAuth2Properties
  override val s3CatalogFileIO: S3CatalogFileIO = S3CatalogFileIO

  override val connectionUrl: String = sys.env("ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI")

  override val targetTableFullName: String = spec.sinkSettings.targetTableName

  override val targetOptimizeSettings: OptimizeSettings = OptimizeSettingsImpl(
    spec.sinkSettings.optimizeSettings.batchThreshold,
    spec.sinkSettings.optimizeSettings.fileSizeThreshold)

  override val archiveOptimizeSettings: OptimizeSettings = OptimizeSettingsImpl(
    spec.sinkSettings.optimizeSettings.batchThreshold,
    spec.sinkSettings.optimizeSettings.fileSizeThreshold)

  override val targetSnapshotExpirationSettings: SnapshotExpirationSettings = SnapshotExpirationSettingsImpl(
    spec.sinkSettings.snapshotExpirationSettings.batchThreshold,
    spec.sinkSettings.snapshotExpirationSettings.retentionThreshold)

  override val archiveSnapshotExpirationSettings: SnapshotExpirationSettings = SnapshotExpirationSettingsImpl(
    spec.sinkSettings.snapshotExpirationSettings.batchThreshold,
    spec.sinkSettings.snapshotExpirationSettings.retentionThreshold)

  override val targetOrphanFilesExpirationSettings: OrphanFilesExpirationSettings = OrphanFilesExpirationSettingsImpl(
    spec.sinkSettings.orphanFilesExpirationSettings.batchThreshold,
    spec.sinkSettings.orphanFilesExpirationSettings.retentionThreshold)

  override val archiveOrphanFilesExpirationSettings: OrphanFilesExpirationSettings = OrphanFilesExpirationSettingsImpl(
    spec.sinkSettings.orphanFilesExpirationSettings.batchThreshold,
    spec.sinkSettings.orphanFilesExpirationSettings.retentionThreshold)

  override val archiveTableFullName: String = spec.sinkSettings.archiveTableName

  override val endpoint: String = sys.env("ARCANE_FRAMEWORK__STORAGE_ENDPOINT")
  override val container: String = sys.env("ARCANE_FRAMEWORK__STORAGE_CONTAINER")
  override val account: String = sys.env("ARCANE_FRAMEWORK__STORAGE_ACCOUNT")
  override val accessKey: String = sys.env("ARCANE_FRAMEWORK__STORAGE_ACCESS_KEY")
  
  override val parallelism: Int = 16
  
  val sourceDeleteDryRun: Boolean = sys.env.get("ARCANE_FRAMEWORK__SOURCE_DELETE_DRY_RUN").exists(v => v.toLowerCase == "true")

  val stagingTableNamePrefix: String = spec.stagingDataSettings.tableNamePrefix
  val stagingCatalog: String = s"${spec.stagingDataSettings.catalog.catalogName}.${spec.stagingDataSettings.catalog.schemaName}"

  val partitionExpressions: Array[String] = spec.tableProperties.partitionExpressions
  val format: TableFormat = TableFormat.valueOf(spec.tableProperties.format)
  val sortedBy: Array[String] = spec.tableProperties.sortedBy
  val parquetBloomFilterColumns: Array[String] = spec.tableProperties.parquetBloomFilterColumns

given Conversion[StreamSpec, CdmTableSettings] with
  def apply(spec: StreamSpec): CdmTableSettings = CdmTableSettings(spec.sourceSettings.name.toLowerCase, spec.sourceSettings.baseLocation)


object MicrosoftSynapseLinkStreamContext {
  type Environment = StreamContext
    & CdmTableSettings
    & GroupingSettings
    & VersionedDataGraphBuilderSettings
    & IcebergCatalogSettings
    & JdbcConsumerOptions
    & TargetTableSettings
    & AzureConnectionSettings
    & ArchiveTableSettings
    & TargetTableSettings
    & MicrosoftSynapseLinkStreamContext
    & GraphExecutionSettings
    & TablePropertiesSettings

  /**
   * The ZLayer that creates the VersionedDataGraphBuilder.
   */
  val layer: ZLayer[Any, Throwable, Environment] = StreamSpec
      .fromEnvironment("STREAMCONTEXT__SPEC")
      .map(c => ZLayer.succeed(MicrosoftSynapseLinkStreamContext(c)) ++ ZLayer.succeed[CdmTableSettings](c))
      .getOrElse(ZLayer.fail(new Exception("The stream context is not specified.")))
}
