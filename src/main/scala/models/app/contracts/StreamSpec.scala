package com.sneaksanddata.arcane.microsoft_synapse_link
package models.app.contracts

import models.app.OptimizeSettings

import com.sneaksanddata.arcane.framework.models.settings.{TableFormat, TablePropertiesSettings as TableProperties}
import upickle.default.*

/**
 * The configuration of Iceberg catalog
 */
case class CatalogSettings(namespace: String,
                           warehouse: String,
                           catalogUri: String,
                           catalogName: String,
                           schemaName: String) derives ReadWriter

/**
 * The configuration of staging data.
 */
case class StagingDataSettings(tableNamePrefix: String, catalog: CatalogSettings, dataLocation: Option[String]) derives ReadWriter

/**
 * The configuration of Iceberg sink.
 */
case class OptimizeSettingsSpec(batchThreshold: Int,
                                fileSizeThreshold: String) derives ReadWriter
/**
 * The configuration of Iceberg sink.
 */
case class SnapshotExpirationSettingsSpec(batchThreshold: Int,
                                          retentionThreshold: String) derives ReadWriter
/**
 * The configuration of Iceberg sink.
 */
case class OrphanFilesExpirationSettings(batchThreshold: Int,
                                          retentionThreshold: String) derives ReadWriter
/**
 * The configuration of Iceberg sink.
 */
case class SinkSettings(targetTableName: String,
                         archiveTableName: String,
                         optimizeSettings: OptimizeSettingsSpec,
                         snapshotExpirationSettings: SnapshotExpirationSettingsSpec,
                         orphanFilesExpirationSettings: OrphanFilesExpirationSettings) derives ReadWriter


/**
 * The configuration of the stream source.
 */
case class SourceSettings(name: String, baseLocation: String, changeCaptureIntervalSeconds: Int) derives ReadWriter

case class TablePropertiesSettings(partitionExpressions: Array[String], sortedBy: Array[String], parquetBloomFilterColumns: Array[String], format: String) derives ReadWriter

given Conversion[TablePropertiesSettings, TableProperties] with
  def apply(x: TablePropertiesSettings): TableProperties = new TableProperties:
    val partitionExpressions: Array[String] = x.partitionExpressions
    val format: TableFormat = TableFormat.valueOf(x.format)
    val sortedBy: Array[String] = x.sortedBy
    val parquetBloomFilterColumns: Array[String] = x.parquetBloomFilterColumns

/**
 * The configuration of the stream source.
 */
case class FieldSelectionRuleSpec(ruleType: String, fields: Array[String]) derives ReadWriter

/**
 * The specification for the stream.
 *
 * @param rowsPerGroup                 The number of rows per group in the staging table
 * @param groupingIntervalSeconds      The grouping interval in seconds
 * @param groupsPerFile                The number of groups per file
 * @param lookBackInterval             The look back interval in seconds
 */
case class StreamSpec(sourceSettings: SourceSettings,

                      // Grouping settings
                      rowsPerGroup: Int,
                      groupingIntervalSeconds: Int,
                      groupsPerFile: Int,
                      lookBackInterval: Int,

                      // Iceberg settings
                      stagingDataSettings: StagingDataSettings,
                      sinkSettings: SinkSettings,
                     
                      // Iceberg table properties

                      tableProperties: TablePropertiesSettings,
                      fieldSelectionRule: FieldSelectionRuleSpec,
                      backfillBehavior: String)
  derives ReadWriter

object StreamSpec:

  def fromEnvironment(envVarName: String): Option[StreamSpec] =
    sys.env.get(envVarName).map(env => read[StreamSpec](env))
