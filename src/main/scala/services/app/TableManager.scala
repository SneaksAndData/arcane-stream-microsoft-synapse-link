package com.sneaksanddata.arcane.microsoft_synapse_link
package services.app

import extensions.ArcaneSchemaExtensions.getMissingFields
import models.app.{ArchiveTableSettings, MicrosoftSynapseLinkStreamContext, TargetTableSettings}
import services.app.JdbcTableManager.generateAlterTableSQL
import services.clients.BatchArchivationResult

import com.sneaksanddata.arcane.framework.utils.SqlUtils.readArcaneSchema
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.*
import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings.TablePropertiesSettings
import com.sneaksanddata.arcane.framework.models.{ArcaneSchema, ArcaneSchemaField}
import com.sneaksanddata.arcane.framework.services.base.SchemaProvider
import com.sneaksanddata.arcane.framework.services.lakehouse.{SchemaConversions, given_Conversion_ArcaneSchema_Schema}
import org.apache.iceberg.Schema
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.Types.{NestedField, TimestampType}
import zio.{Task, ZIO, ZLayer}

import java.sql.{Connection, DriverManager, ResultSet}
import java.time.{OffsetDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClientOptions


trait TableManager:
  
  def createTargetTable: Task[TableCreationResult]

  def createArchiveTable: Task[TableCreationResult]
  
  def tryCreateBackfillTable: Task[TableCreationResult]
  
  def getTargetSchema(tableName: String): Task[ArcaneSchema]

  def cleanupStagingTables: Task[Unit]
  
  def migrateSchema(batchSchema: ArcaneSchema, tableName: String): Task[Unit]

  def getLastUpdateTime(tableName: String): Task[OffsetDateTime]


/**
 * The result of applying a batch.
 */
type TableCreationResult = Boolean

/**
 * The result of applying a batch.
 */
type TableModificationResult = Boolean

class JdbcTableManager(options: JdbcMergeServiceClientOptions,
                       targetTableSettings: TargetTableSettings,
                       archiveTableSettings: ArchiveTableSettings,
                       tablePropertiesSettings: TablePropertiesSettings,
                       schemaProvider: SchemaProvider[ArcaneSchema],
                       fieldsFilteringService: FieldsFilteringService,
                       streamContext: MicrosoftSynapseLinkStreamContext)
  extends TableManager with AutoCloseable:

  require(options.isValid, "Invalid JDBC url provided for the consumer")

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  private lazy val sqlConnection: Connection = DriverManager.getConnection(options.connectionUrl)

  private val tableProperties = 
    def serializeArrayProperty(prop: Array[String]): String =
      val value = prop.map { expr => s"'$expr'" }.mkString(",")
      s"ARRAY[$value]"

    Map(
      "partitioning" -> serializeArrayProperty(tablePropertiesSettings.partitionExpressions),
      "format" -> s"'${tablePropertiesSettings.format.toString}'",
      "sorted_by" -> serializeArrayProperty(tablePropertiesSettings.sortedBy),
      "parquet_bloom_filter_columns" -> serializeArrayProperty(tablePropertiesSettings.parquetBloomFilterColumns)
    )

  def createTargetTable: Task[TableCreationResult] =
    for
      _ <- zlog("Creating target table", Seq(getAnnotation("targetTableName", targetTableSettings.targetTableFullName)))
      schema: ArcaneSchema <- ZIO.fromFuture(implicit ec => schemaProvider.getSchema )
      created <- ZIO.fromFuture(implicit ec => createTable(targetTableSettings.targetTableFullName, fieldsFilteringService.filter(schema), tableProperties))
    yield created

  def createArchiveTable: Task[TableCreationResult] =
    for
      _ <- zlog("Creating archive table", Seq(getAnnotation("archiveTableName", archiveTableSettings.archiveTableFullName)))
      schema: ArcaneSchema <- ZIO.fromFuture(implicit ec => schemaProvider.getSchema )
      created <- ZIO.fromFuture(implicit ec => createTable(archiveTableSettings.archiveTableFullName, fieldsFilteringService.filter(schema), tableProperties))
    yield created
    
  def tryCreateBackfillTable: Task[TableCreationResult] =
    if streamContext.IsBackfilling then
      for
        _ <- zlog("Creating backfill table", Seq(getAnnotation("backfillTableName", streamContext.backfillTableName)))
        schema: ArcaneSchema <- ZIO.fromFuture(implicit ec => schemaProvider.getSchema)
        created <- ZIO.fromFuture(implicit ec => createTable(streamContext.backfillTableName, fieldsFilteringService.filter(schema), tableProperties))
      yield created
    else
      ZIO.succeed(false)

  def cleanupStagingTables: Task[Unit] =
    val sql = s"SHOW TABLES FROM ${streamContext.stagingCatalog} LIKE '${streamContext.stagingTableNamePrefix}\\_\\_%' escape '\\'"
    val statement = ZIO.attemptBlocking {
      sqlConnection.prepareStatement(sql)
    }
    ZIO.acquireReleaseWith(statement)(st => ZIO.succeed(st.close())) { statement =>
      for
        resultSet <- ZIO.attemptBlocking { statement.executeQuery() }
        strings <- ZIO.attemptBlocking { readStrings(resultSet) }
        _ <- ZIO.foreach(strings)(tableName => zlog("Found lost staging table: " + tableName))
        _ <- ZIO.foreach(strings)(dropTable)
      yield ()
    }
    
  def migrateSchema(batchSchema: ArcaneSchema, tableName: String): Task[Unit] =
    for targetSchema <- getTargetSchema(tableName)
        missingFields = targetSchema.getMissingFields(batchSchema)
        _ <- addColumns(tableName, missingFields)
    yield ()

  def getTargetSchema(tableName: String): Task[ArcaneSchema] =
    val query = s"SELECT * FROM $tableName where true and false"
    val ack = ZIO.attemptBlocking(sqlConnection.prepareStatement(query))
    ZIO.acquireReleaseWith(ack)(st => ZIO.succeed(st.close())) { statement =>
      for
        schemaResult <- ZIO.attemptBlocking(statement.executeQuery())
        fields <- ZIO.attemptBlocking(schemaResult.readArcaneSchema)
      yield fields.get
    }

  def addColumns(targetTableName: String, missingFields: ArcaneSchema): Task[Unit] =
    for _ <- ZIO.foreach(missingFields)(field => {
        val query = generateAlterTableSQL(targetTableName, field.name, SchemaConversions.toIcebergType(field.fieldType))
        zlog(s"Adding column to table $targetTableName: ${field.name} ${field.fieldType}, $query")
          *> ZIO.attemptBlocking(sqlConnection.prepareStatement(query).execute())
      })
    yield ()

  def getLastUpdateTime(tableName: String): Task[OffsetDateTime] =
    val segments = tableName.split("\\.")
    val historyTableName = s"${segments(0)}.${segments(1)}.\"${segments(2)}$$history\""
    val query = s"SELECT MAX(made_current_at) FROM $historyTableName"
    val ack = ZIO.attemptBlocking(sqlConnection.prepareStatement(query))
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS XXX")

    ZIO.acquireReleaseWith(ack)(st => ZIO.succeed(st.close())) { statement =>
      for
        dateResult <- ZIO.attemptBlocking(statement.executeQuery())
        date <- ZIO.attemptBlocking {
          dateResult.next()
          dateResult.getTimestamp(1).toInstant.atOffset(ZoneOffset.UTC)
        }
      yield date
    }

  private def dropTable(tableName: String): Task[Unit] =
    val sql = s"DROP TABLE IF EXISTS $tableName"
    val statement = ZIO.attemptBlocking {
      sqlConnection.prepareStatement(sql)
    }
    ZIO.acquireReleaseWith(statement)(st => ZIO.succeed(st.close())) { statement =>
      for
        _ <- zlog("Dropping table: " + tableName)
        _ <- ZIO.attemptBlocking { statement.execute() }
      yield ()
    }

  private def readStrings(row: ResultSet): List[String] =
    Iterator.iterate(row.next())(_ => row.next())
      .takeWhile(identity)
      .map(_ => row.getString(1)).toList

  private def createTable(name: String, schema: Schema, properties: Map[String, String]): Future[TableCreationResult] =
    Future(sqlConnection.prepareStatement(JdbcTableManager.generateCreateTableSQL(name, schema, properties)).execute())

  override def close(): Unit = sqlConnection.close()


object JdbcTableManager:
  type Environment = JdbcMergeServiceClientOptions
    & TargetTableSettings
    & ArchiveTableSettings
    & SchemaProvider[ArcaneSchema]
    & MicrosoftSynapseLinkStreamContext
    & FieldsFilteringService

  def apply(options: JdbcMergeServiceClientOptions,
            targetTableSettings: TargetTableSettings,
            archiveTableSettings: ArchiveTableSettings,
            tablePropertiesSettings: TablePropertiesSettings,
            schemaProvider: SchemaProvider[ArcaneSchema],
            fieldsFilteringService: FieldsFilteringService,
            streamContext: MicrosoftSynapseLinkStreamContext): JdbcTableManager =
    new JdbcTableManager(options, targetTableSettings, archiveTableSettings, tablePropertiesSettings, schemaProvider, fieldsFilteringService, streamContext)

  /**
   * The ZLayer that creates the JdbcConsumer.
   */
  val layer: ZLayer[Environment, Nothing, TableManager] =
    ZLayer.scoped {
      ZIO.fromAutoCloseable {
        for connectionOptions <- ZIO.service[JdbcMergeServiceClientOptions]
            targetTableSettings <- ZIO.service[TargetTableSettings]
            archiveTableSettings <- ZIO.service[ArchiveTableSettings]
            tablePropertiesSettings <- ZIO.service[TablePropertiesSettings]
            schemaProvider <- ZIO.service[SchemaProvider[ArcaneSchema]]
            streamContext <- ZIO.service[MicrosoftSynapseLinkStreamContext]
            fieldsFilteringService <- ZIO.service[FieldsFilteringService]
        yield JdbcTableManager(connectionOptions, targetTableSettings, archiveTableSettings, tablePropertiesSettings, schemaProvider, fieldsFilteringService, streamContext)
      }
    }

  def generateAlterTableSQL(tableName: String, fieldName: String, fieldType: Type): String =
    s"ALTER TABLE $tableName ADD COLUMN $fieldName ${fieldType.convertType}"

  private def generateCreateTableSQL(tableName: String, schema: Schema, properties: Map[String, String]): String =
    val columns = schema.columns().asScala.map { field => s"${field.name()} ${field.`type`().convertType}" }.mkString(", ")
    val supportedProperties = properties.map { (propertyKey, propertyValue) => s"$propertyKey=$propertyValue" }.mkString(", ")
    s"CREATE TABLE IF NOT EXISTS $tableName ($columns) WITH ($supportedProperties)"

  // See: https://trino.io/docs/current/connector/iceberg.html#iceberg-to-trino-type-mapping
  extension (icebergType: Type) def convertType: String = icebergType.typeId() match {
    case TypeID.BOOLEAN => "BOOLEAN"
    case TypeID.INTEGER => "INTEGER"
    case TypeID.LONG => "BIGINT"
    case TypeID.FLOAT => "REAL"
    case TypeID.DOUBLE => "DOUBLE"
    case TypeID.DECIMAL => "DECIMAL(1, 2)"
    case TypeID.DATE => "DATE"
    case TypeID.TIME => "TIME(6)"
    case TypeID.TIMESTAMP if icebergType.isInstanceOf[TimestampType] && icebergType.asInstanceOf[TimestampType].shouldAdjustToUTC() => "TIMESTAMP(6) WITH TIME ZONE"
    case TypeID.TIMESTAMP if icebergType.isInstanceOf[TimestampType] && !icebergType.asInstanceOf[TimestampType].shouldAdjustToUTC() => "TIMESTAMP(6)"
    case TypeID.STRING => "VARCHAR"
    case TypeID.UUID => "UUID"
    case TypeID.BINARY => "VARBINARY"
    case _ => throw new IllegalArgumentException(s"Unsupported type: $icebergType")
  }
