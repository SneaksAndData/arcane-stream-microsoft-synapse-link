package com.sneaksanddata.arcane.microsoft_synapse_link
package services.app

import models.app.{ArchiveTableSettings, TargetTableSettings}

import com.sneaksanddata.arcane.framework.models.ArcaneSchema
import com.sneaksanddata.arcane.framework.services.base.SchemaProvider

import scala.jdk.CollectionConverters.*
import com.sneaksanddata.arcane.framework.services.consumers.JdbcConsumerOptions
import org.apache.iceberg.Schema
import org.slf4j.{Logger, LoggerFactory}
import zio.{ZIO, ZLayer}

import java.sql.{Connection, DriverManager}
import scala.concurrent.Future
import org.apache.iceberg.Schema
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.Types.NestedField
import com.sneaksanddata.arcane.framework.services.lakehouse.given_Conversion_ArcaneSchema_Schema

trait TableManager:
  
  def createTargetTable: Future[TableCreationResult]

  def createArchiveTable: Future[TableCreationResult]

/**
 * The result of applying a batch.
 */
type TableCreationResult = Boolean

class JdbcTableManager(options: JdbcConsumerOptions,
                       targetTableSettings: TargetTableSettings,
                       archiveTableSettings: ArchiveTableSettings,
                       schemaProvider: SchemaProvider[ArcaneSchema]
                      )
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

  private def createTable(name: String, schema: Schema): Future[TableCreationResult] =
    Future(sqlConnection.prepareStatement(JdbcTableManager.generateCreateTableSQL(name, schema)).execute())

  override def close(): Unit = sqlConnection.close()


object JdbcTableManager:
  type Environemnt = JdbcConsumerOptions
    & TargetTableSettings
    & ArchiveTableSettings
    & SchemaProvider[ArcaneSchema]

  def apply(options: JdbcConsumerOptions,
            targetTableSettings: TargetTableSettings,
            archiveTableSettings: ArchiveTableSettings,
            schemaProvider: SchemaProvider[ArcaneSchema]): JdbcTableManager =
    new JdbcTableManager(options, targetTableSettings, archiveTableSettings, schemaProvider)

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
        yield JdbcTableManager(connectionOptions, targetTableSettings, archiveTableSettings, schemaProvider)
      }
    }

  private def generateCreateTableSQL(tableName: String, schema: Schema): String =
    val columns = schema.columns().asScala.map { field => s"${field.name()} ${field.convertType}" }.mkString(", ")
    s"CREATE TABLE $tableName ($columns)"

  // See: https://trino.io/docs/current/connector/iceberg.html#iceberg-to-trino-type-mapping
  extension (field: NestedField) def convertType: String = field.`type`().typeId() match {
    case TypeID.BOOLEAN => "BOOLEAN"
    case TypeID.INTEGER => "INTEGER"
    case TypeID.LONG => "BIGINT"
    case TypeID.FLOAT => "REAL"
    case TypeID.DOUBLE => "DOUBLE"
    case TypeID.DECIMAL => "DECIMAL(1, 2)"
    case TypeID.DATE => "DATE"
    case TypeID.TIME => "TIME(6)"
    case TypeID.TIMESTAMP => "TIMESTAMP(6)"
    case TypeID.STRING => "VARCHAR"
    case TypeID.UUID => "UUID"
    case TypeID.BINARY => "VARBINARY"
    case _ => throw new IllegalArgumentException(s"Unsupported type: ${field.`type`()}")
  }
