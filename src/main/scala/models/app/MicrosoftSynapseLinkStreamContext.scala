package com.sneaksanddata.arcane.microsoft_synapse_link
package models.app

import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings.{GroupingSettings, SinkSettings, VersionedDataGraphBuilderSettings}
import com.sneaksanddata.arcane.framework.services.cdm.CdmTableSettings
import com.sneaksanddata.arcane.framework.services.consumers.JdbcConsumerOptions
import com.sneaksanddata.arcane.framework.services.lakehouse.{IcebergCatalogCredential, S3CatalogFileIO}
import com.sneaksanddata.arcane.framework.services.lakehouse.base.IcebergCatalogSettings
import zio.ZLayer
import upickle.default.*

import java.time.Duration

trait AzureConnectionSettings:
  val endpoint: String
  val container: String
  val account: String
  val accessKey: String

/**
 * The configuration of Iceberg sink.
 */
case class CatalogSettings(namespace: String, warehouse: String, catalogUri: String) derives ReadWriter


/**
 * The specification for the stream.
 *
 * @param name                         The name of the CDM table
 * @param baseLocation                 The entity base location
 * @param rowsPerGroup                 The number of rows per group in the staging table
 * @param groupingIntervalSeconds      The grouping interval in seconds
 * @param groupsPerFile                The number of groups per file
 * @param lookBackInterval             The look back interval in seconds
 * @param changeCaptureIntervalSeconds The change capture interval in seconds
 * @param partitionExpression          Partition expression for partitioning the data in the staging table (optional)
 */
case class StreamSpec(name: String,
                      baseLocation: String,

                      // Grouping settings
                      rowsPerGroup: Int,
                      groupingIntervalSeconds: Int,
                      groupsPerFile: Int,
                      lookBackInterval: Int,

                      // Timeouts
                      changeCaptureIntervalSeconds: Int,

                      // Iceberg settings
                      catalog: CatalogSettings,

                      stagingLocation: Option[String],
                      sinkLocation: String,
                     
                      targetTableName: String,
                      archiveTableName: String,
                     
                      partitionExpression: Option[String])
  derives ReadWriter

trait TargetTableSettings:
  val targetTableFullName: String

trait ArchiveTableSettings:
  val archiveTableFullName: String

// TODO
trait StagingTableSettings:
  val tableNamePrefix: String
  val namespace: String
  val warehouse: String
  val catalogUri: String

/**
 * The context for the SQL Server Change Tracking stream.
 * @param spec The stream specification
 */
case class MicrosoftSynapseLinkStreamContext(spec: StreamSpec) extends StreamContext
  with GroupingSettings
  with IcebergCatalogSettings
  with JdbcConsumerOptions
  with VersionedDataGraphBuilderSettings
  with AzureConnectionSettings
  with TargetTableSettings
  with ArchiveTableSettings:

  override val rowsPerGroup: Int = spec.rowsPerGroup
  override val lookBackInterval: Duration = Duration.ofSeconds(spec.lookBackInterval)
  override val changeCaptureInterval: Duration = Duration.ofSeconds(spec.changeCaptureIntervalSeconds)
  override val groupingInterval: Duration = Duration.ofSeconds(spec.groupingIntervalSeconds)

  override val namespace: String = spec.catalog.namespace
  override val warehouse: String = spec.catalog.warehouse
  override val catalogUri: String = spec.catalog.catalogUri

  override val additionalProperties: Map[String, String] = IcebergCatalogCredential.oAuth2Properties
  override val s3CatalogFileIO: S3CatalogFileIO = S3CatalogFileIO

  override val stagingLocation: Option[String] = spec.stagingLocation

  override val connectionUrl: String = sys.env("ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI")

  override val targetTableFullName: String = spec.targetTableName
  override val archiveTableFullName: String = spec.archiveTableName

  override val endpoint: String = sys.env("ARCANE_FRAMEWORK__STORAGE_ENDPOINT")
  override val container: String = sys.env("ARCANE_FRAMEWORK__STORAGE_CONTAINER")
  override val account: String = sys.env("ARCANE_FRAMEWORK__STORAGE_ACCOUNT")
  override val accessKey: String = sys.env("ARCANE_FRAMEWORK__STORAGE_ACCESS_KEY")


given Conversion[MicrosoftSynapseLinkStreamContext, CdmTableSettings] with
  def apply(context: MicrosoftSynapseLinkStreamContext): CdmTableSettings = CdmTableSettings(context.spec.name, context.spec.baseLocation)

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

  /**
   * The ZLayer that creates the VersionedDataGraphBuilder.
   */
  val layer: ZLayer[Any, Throwable, Environment] =
    sys.env.get("STREAMCONTEXT__SPEC") map { raw =>
      val spec = read[StreamSpec](raw)
      val context = MicrosoftSynapseLinkStreamContext(spec)
      ZLayer.succeed(context) ++ ZLayer.succeed[CdmTableSettings](context)
    } getOrElse {
      ZLayer.fail(new Exception("The stream context is not specified."))
    }
}
