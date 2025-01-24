package com.sneaksanddata.arcane.microsoft_synapse_link
package models.app

import models.app.contracts.StreamSpec

import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings.{GroupingSettings, VersionedDataGraphBuilderSettings}
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

trait TargetTableSettings:
  val targetTableFullName: String

trait ArchiveTableSettings:
  val archiveTableFullName: String

trait ParallelismSettings:
  val parallelism: Int

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
  with ParallelismSettings:

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
  override val archiveTableFullName: String = spec.sinkSettings.archiveTableName

  override val endpoint: String = sys.env("ARCANE_FRAMEWORK__STORAGE_ENDPOINT")
  override val container: String = sys.env("ARCANE_FRAMEWORK__STORAGE_CONTAINER")
  override val account: String = sys.env("ARCANE_FRAMEWORK__STORAGE_ACCOUNT")
  override val accessKey: String = sys.env("ARCANE_FRAMEWORK__STORAGE_ACCESS_KEY")
  override val parallelism: Int = 16

  val stagingTableNamePrefix: String = spec.stagingDataSettings.tableNamePrefix
  val stagingCatalog: String = s"${spec.stagingDataSettings.catalog.catalogName}.${spec.stagingDataSettings.catalog.schemaName}"

given Conversion[StreamSpec, CdmTableSettings] with
  def apply(spec: StreamSpec): CdmTableSettings = CdmTableSettings(spec.sourceSettings.name, spec.sourceSettings.baseLocation)

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

  /**
   * The ZLayer that creates the VersionedDataGraphBuilder.
   */
  val layer: ZLayer[Any, Throwable, Environment] = StreamSpec
      .fromEnvironment("STREAMCONTEXT__SPEC")
      .map(c => ZLayer.succeed(MicrosoftSynapseLinkStreamContext(c)) ++ ZLayer.succeed[CdmTableSettings](c))
      .getOrElse(ZLayer.fail(new Exception("The stream context is not specified.")))
}
