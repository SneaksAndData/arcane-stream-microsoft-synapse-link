package com.sneaksanddata.arcane.microsoft_synapse_link
package services.app

import models.app.{ArchiveTableSettings, MicrosoftSynapseLinkStreamContext, TargetTableSettings}

import com.sneaksanddata.arcane.framework.models.{ArcaneSchema, ArcaneSchemaField}
import com.sneaksanddata.arcane.framework.services.base.SchemaProvider

import scala.jdk.CollectionConverters.*
import com.sneaksanddata.arcane.framework.services.consumers.JdbcConsumerOptions
import org.apache.iceberg.Schema
import org.slf4j.{Logger, LoggerFactory}
import zio.{Scope, Task, UIO, ZIO, ZLayer}

import java.sql.{Connection, DriverManager, ResultSet}
import scala.concurrent.Future
import org.apache.iceberg.Schema
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.Types.{NestedField, TimestampType}
import com.sneaksanddata.arcane.framework.services.lakehouse.given_Conversion_ArcaneSchema_Schema
import org.apache.iceberg.types.Types.{NestedField, TimestampType}
import com.sneaksanddata.arcane.framework.services.lakehouse.{SchemaConversions, given_Conversion_ArcaneSchema_Schema}
import com.sneaksanddata.arcane.microsoft_synapse_link.services.app.JdbcTableManager.{generateAlterTableSQL, generateUpdateTableSQL}
import com.sneaksanddata.arcane.microsoft_synapse_link.services.clients.BatchArchivationResult

import scala.annotation.tailrec
import scala.util.{Try, Using}

trait TableManager:
  
  def createTargetTable: Future[TableCreationResult]

  def createArchiveTable: Future[TableCreationResult]

  def cleanupStagingTables: Task[Unit]

  def addColumns(targetTableName: String, missingFields: ArcaneSchema): Task[Unit]

  def modifyColumns(targetTableName: String, missingFields: ArcaneSchema): Task[Unit]

  def getMissingFields(targetSchema: ArcaneSchema, bathes: Iterable[ArcaneSchema]): Iterable[Seq[ArcaneSchemaField]] =
    bathes.map {
      batch => batch.filter{
        batchField => !targetSchema.exists(targetField => targetField.name.toLowerCase() == batchField.name.toLowerCase() && targetField.fieldType == batchField.fieldType)
      }
    }

  def getUpdatingFields(targetSchema: ArcaneSchema, bathes: Iterable[ArcaneSchema]): Iterable[Seq[ArcaneSchemaField]] =
    bathes.map {
      batch => batch.filter{
        batchField => targetSchema.exists(targetField => targetField.name.toLowerCase() == batchField.name.toLowerCase() && targetField.fieldType != batchField.fieldType)
      }
    }

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

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private lazy val sqlConnection: Connection = DriverManager.getConnection(options.connectionUrl)

  def createTargetTable: Future[TableCreationResult] =
    logger.info(s"Creating target table ${targetTableSettings.targetTableFullName}")
    for schema <- schemaProvider.getSchema
        result <- createTable(targetTableSettings.targetTableFullName, schema)
    yield result

  def createArchiveTable: Future[TableCreationResult] = 
    logger.info(s"Creating target table ${archiveTableSettings.archiveTableFullName}")
    for schema <- schemaProvider.getSchema
        result <- createTable(archiveTableSettings.archiveTableFullName, schema)
    yield result

  def cleanupStagingTables: Task[Unit] =
    val sql = s"SHOW TABLES FROM ${streamContext.stagingCatalog} LIKE '${streamContext.stagingTableNamePrefix}_%'"
    val statement = ZIO.attemptBlocking {
      sqlConnection.prepareStatement(sql)
    }
    ZIO.acquireReleaseWith(statement)(st => ZIO.succeed(st.close())) { statement =>
      for
        resultSet <- ZIO.attemptBlocking { statement.executeQuery() }
        strings <- ZIO.attemptBlocking { readStrings(resultSet) }
        _ <- ZIO.foreach(strings)(tableName => ZIO.log("Found lost staging table: " + tableName))
        _ <- ZIO.foreach(strings)(dropTable)
      yield ()
    }

  def addColumns(targetTableName: String, missingFields: ArcaneSchema): Task[Unit] =
    for _ <- ZIO.foreach(missingFields)(field => {
        val query = generateAlterTableSQL(targetTableName, field.name, SchemaConversions.toIcebergType(field.fieldType))
        ZIO.log(s"Adding column to table $targetTableName: ${field.name} ${field.fieldType}, $query")
          *> ZIO.attemptBlocking(sqlConnection.prepareStatement(query).execute())
      })
    yield ()

  def modifyColumns(targetTableName: String, missingFields: ArcaneSchema): Task[Unit] =
    for _ <- ZIO.foreach(missingFields)(field => {
      val query = generateUpdateTableSQL(targetTableName, field.name, SchemaConversions.toIcebergType(field.fieldType))
      ZIO.log(s"Modifing column to table $targetTableName: ${field.name} ${field.fieldType}, $query")
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
        _ <- ZIO.log("Dropping table: " + tableName)
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
  type Environemnt = JdbcConsumerOptions
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
  val layer: ZLayer[Environemnt, Nothing, TableManager] =
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
    s"ALTER TABLE ${tableName} ADD COLUMN ${fieldName} ${fieldType.convertType}"

  def generateUpdateTableSQL(tableName: String, fieldName: String, fieldType: Type): String =
    s"ALTER TABLE ${tableName} ALTER COLUMN ${fieldName} SET DATA TYPE ${fieldType.convertType}"

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
    case _ => throw new IllegalArgumentException(s"Unsupported type: ${icebergType}")
  }
