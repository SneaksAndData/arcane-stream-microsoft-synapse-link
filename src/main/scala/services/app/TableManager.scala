package com.sneaksanddata.arcane.microsoft_synapse_link
package services.app

import models.app.{ArchiveTableSettings, MicrosoftSynapseLinkStreamContext, TargetTableSettings}

import com.sneaksanddata.arcane.framework.models.ArcaneSchema
import com.sneaksanddata.arcane.framework.services.base.SchemaProvider
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.*

import scala.jdk.CollectionConverters.*
import com.sneaksanddata.arcane.framework.services.consumers.JdbcConsumerOptions
import zio.{Task, ZIO, ZLayer}

import java.sql.{Connection, DriverManager, ResultSet}
import scala.concurrent.Future
import org.apache.iceberg.Schema
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.Types.NestedField
import com.sneaksanddata.arcane.framework.services.lakehouse.given_Conversion_ArcaneSchema_Schema


trait TableManager:
  
  def createTargetTable: Task[TableCreationResult]

  def createArchiveTable: Task[TableCreationResult]

  def cleanupStagingTables: Task[Unit]

/**
 * The result of applying a batch.
 */
type TableCreationResult = Boolean

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
    val sql = s"SHOW TABLES FROM ${streamContext.stagingCatalog} LIKE '${streamContext.stagingTableNamePrefix}_%'"
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

  private def generateCreateTableSQL(tableName: String, schema: Schema): String =
    val columns = schema.columns().asScala.map { field => s"${field.name()} ${field.convertType}" }.mkString(", ")
    s"CREATE TABLE IF NOT EXISTS $tableName ($columns)"

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
