package com.sneaksanddata.arcane.microsoft_synapse_link
package services.app

import extensions.ArcaneSchemaExtensions.getMissingFields
import models.app.{ArchiveTableSettings, MicrosoftSynapseLinkStreamContext, TargetTableSettings}
import services.app.JdbcTableManager.generateAlterTableSQL
import services.clients.BatchArchivationResult

import com.sneaksanddata.arcane.framework.utils.SqlUtils.readArcaneSchema
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.*
import com.sneaksanddata.arcane.framework.models.{ArcaneSchema, ArcaneSchemaField}
import com.sneaksanddata.arcane.framework.services.base.SchemaProvider
import com.sneaksanddata.arcane.framework.services.consumers.JdbcConsumerOptions
import com.sneaksanddata.arcane.framework.services.lakehouse.{SchemaConversions, given_Conversion_ArcaneSchema_Schema}
import org.apache.iceberg.Schema
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.Types.{NestedField, TimestampType}
import zio.{Task, ZIO, ZLayer}

import java.sql.{Connection, DriverManager, ResultSet}
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*


trait TableManager:
  
  def createTargetTable: Task[TableCreationResult]

  def createArchiveTable: Task[TableCreationResult]
  
  def getTargetSchema(tableName: String): Task[ArcaneSchema]

  def cleanupStagingTables: Task[Unit]
  
  def migrateSchema(batchSchema: ArcaneSchema, tableName: String): Task[Unit]
  

/**
 * The result of applying a batch.
 */
type TableCreationResult = Boolean

/**
 * The result of applying a batch.
 */
type TableModificationResult = Boolean

class JdbcTableManager(options: JdbcConsumerOptions,
                       targetTableSettings: TargetTableSettings,
                       archiveTableSettings: ArchiveTableSettings,
                       schemaProvider: SchemaProvider[ArcaneSchema],
                       streamContext: MicrosoftSynapseLinkStreamContext)
  extends TableManager with AutoCloseable:

  require(options.isValid, "Invalid JDBC url provided for the consumer")

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  private lazy val sqlConnection: Connection = DriverManager.getConnection(options.connectionUrl)

  def createTargetTable: Task[TableCreationResult] =
    for
      _ <- zlog("Creating target table", Seq(getAnnotation("targetTableName", targetTableSettings.targetTableFullName)))
      schema: ArcaneSchema <- ZIO.fromFuture(implicit ec => schemaProvider.getSchema )
      created <- ZIO.fromFuture(implicit ec => createTable(targetTableSettings.targetTableFullName, schema))
    yield created

  def createArchiveTable: Task[TableCreationResult] =
    for
      _ <- zlog("Creating archive table", Seq(getAnnotation("archiveTableName", archiveTableSettings.archiveTableFullName)))
      schema: ArcaneSchema <- ZIO.fromFuture(implicit ec => schemaProvider.getSchema )
      created <- ZIO.fromFuture(implicit ec => createTable(archiveTableSettings.archiveTableFullName, schema))
    yield created

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

  private def createTable(name: String, schema: Schema): Future[TableCreationResult] =
    Future(sqlConnection.prepareStatement(JdbcTableManager.generateCreateTableSQL(name, schema)).execute())

  override def close(): Unit = sqlConnection.close()


object JdbcTableManager:
  type Environment = JdbcConsumerOptions
    & TargetTableSettings
    & ArchiveTableSettings
    & SchemaProvider[ArcaneSchema]
    & MicrosoftSynapseLinkStreamContext

  def apply(options: JdbcConsumerOptions,
            targetTableSettings: TargetTableSettings,
            archiveTableSettings: ArchiveTableSettings,
            schemaProvider: SchemaProvider[ArcaneSchema],
            streamContext: MicrosoftSynapseLinkStreamContext): JdbcTableManager =
    new JdbcTableManager(options, targetTableSettings, archiveTableSettings, schemaProvider, streamContext)

  /**
   * The ZLayer that creates the JdbcConsumer.
   */
  val layer: ZLayer[Environment, Nothing, TableManager] =
    ZLayer.scoped {
      ZIO.fromAutoCloseable {
        for connectionOptions <- ZIO.service[JdbcConsumerOptions]
            targetTableSettings <- ZIO.service[TargetTableSettings]
            archiveTableSettings <- ZIO.service[ArchiveTableSettings]
            schemaProvider <- ZIO.service[SchemaProvider[ArcaneSchema]]
            streamContext <- ZIO.service[MicrosoftSynapseLinkStreamContext]
        yield JdbcTableManager(connectionOptions, targetTableSettings, archiveTableSettings, schemaProvider, streamContext)
      }
    }

  def generateAlterTableSQL(tableName: String, fieldName: String, fieldType: Type): String =
    s"ALTER TABLE $tableName ADD COLUMN $fieldName ${fieldType.convertType}"

  private def generateCreateTableSQL(tableName: String, schema: Schema): String =
    val columns = schema.columns().asScala.map { field => s"${field.name()} ${field.`type`().convertType}" }.mkString(", ")
    s"CREATE TABLE IF NOT EXISTS $tableName ($columns)"

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
