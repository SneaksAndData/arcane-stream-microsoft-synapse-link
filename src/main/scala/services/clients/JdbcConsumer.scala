package com.sneaksanddata.arcane.microsoft_synapse_link
package services.clients

import com.sneaksanddata.arcane.framework.services.consumers.{JdbcConsumerOptions, StagedVersionedBatch}
import services.clients.{BatchArchivationResult, JdbcConsumer}

import com.sneaksanddata.arcane.microsoft_synapse_link.models.app.ArchiveTableSettings
import org.slf4j.{Logger, LoggerFactory}
import zio.{Schedule, Task, ZIO, ZLayer}

import java.sql.{Connection, DriverManager, ResultSet}
import java.time.Duration
import scala.concurrent.Future
import scala.util.Try


/**
 * The result of applying a batch.
 */
type BatchApplicationResult = Boolean


/**
 * The result of applying a batch.
 */
class BatchArchivationResult

/**
 * A consumer that consumes batches from a JDBC source.
 *
 * @param options The options for the consumer.
 */
class JdbcConsumer[Batch <: StagedVersionedBatch](options: JdbcConsumerOptions,
                                                  archiveTableSettings: ArchiveTableSettings)
  extends AutoCloseable:
  
  require(options.isValid, "Invalid JDBC url provided for the consumer")

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private lazy val sqlConnection: Connection = DriverManager.getConnection(options.connectionUrl)

  val retryPolicy = Schedule.exponential(Duration.ofSeconds(1)) && Schedule.recurs(5)
  
  def getPartitionValues(batchName: String, partitionFields: List[String]): Future[Map[String, List[String]]] =
    Future.sequence(partitionFields
      .map(partitionField =>
        val query = s"SELECT DISTINCT $partitionField FROM $batchName"
        Future(sqlConnection.prepareStatement(query).executeQuery())
          .map(collectPartitionColumn(_, partitionField))
          .map(values => partitionField -> values.toList)
      )).map(_.toMap)

  
  def applyBatch(batch: Batch): Task[BatchApplicationResult] =
    val ack = ZIO.attemptBlocking({ sqlConnection.prepareStatement(batch.batchQuery.query) }) retry  retryPolicy
    ZIO.acquireReleaseWith(ack)(st => ZIO.succeed(st.close())){ statement =>
      for
        applicationResult <- ZIO.attemptBlocking{ statement.execute() }
      yield applicationResult
    }

  def archiveBatch(batch: Batch): Task[BatchArchivationResult] =
    for _ <- executeArchivationQuery(batch)
    yield new BatchArchivationResult

  def optimizeTarget(tableName: String, batchNumber: Long, optimizeThreshold: Long, fileSizeThreshold: String): Task[BatchApplicationResult] =
    if (batchNumber+1) % optimizeThreshold == 0 then
      val query = ZIO.attemptBlocking {
        sqlConnection.prepareStatement(s"ALTER TABLE $tableName execute optimize(file_size_threshold => '$fileSizeThreshold')")
      }
      ZIO.acquireReleaseWith(query)(st => ZIO.succeed(st.close())) { statement =>
        for
          _ <- ZIO.log(s"Optimizing table $tableName. Batch number: $batchNumber. fileSizeThreshold: $fileSizeThreshold")
          _ <- ZIO.attemptBlocking { statement.execute() }
        yield true
      }
    else
      ZIO.succeed(false)

  def expireSnapshots(tableName: String, batchNumber: Long, optimizeThreshold: Long, retentionThreshold: String): Task[BatchApplicationResult] =
    if (batchNumber+1) % optimizeThreshold == 0 then
      val query = ZIO.attemptBlocking {
        sqlConnection.prepareStatement(s"ALTER TABLE $tableName execute expire_snapshots(retention_threshold => '$retentionThreshold')")
      }
      ZIO.acquireReleaseWith(query)(st => ZIO.succeed(st.close())) { statement =>
        for
          _ <- ZIO.log(s"Run expire_snapshots for table $tableName. Batch number: $batchNumber. retentionThreshold: $retentionThreshold")
          _ <- ZIO.attemptBlocking { statement.execute() }
        yield true
      }
    else
      ZIO.succeed(false)

  def expireOrphanFiles(tableName: String, batchNumber: Long, optimizeThreshold: Long, retentionThreshold: String): Task[BatchApplicationResult] =
    if (batchNumber+1) % optimizeThreshold == 0 then
      val query = ZIO.attemptBlocking {
        sqlConnection.prepareStatement(s"ALTER TABLE $tableName execute remove_orphan_files(retention_threshold => '$retentionThreshold')")
      }
      ZIO.acquireReleaseWith(query)(st => ZIO.succeed(st.close())) { statement =>
        for
          _ <- ZIO.log(s"Run remove_orphan_files for table $tableName. Batch number: $batchNumber. retentionThreshold: $retentionThreshold")
          _ <- ZIO.attemptBlocking { statement.execute() }
        yield true
      }
    else
      ZIO.succeed(false)

  private def executeArchivationQuery(batch: Batch): Task[BatchArchivationResult] =
    val expression = batch.archiveExpr(archiveTableSettings.archiveTableFullName)
    val ack = ZIO.blocking {
      ZIO.succeed(sqlConnection.prepareStatement(expression))
    }
    ZIO.acquireReleaseWith(ack)(st => ZIO.succeed(st.close())) { statement =>
      for
        _ <- ZIO.log(s"archiving batch ${batch.name}")
        _ <- ZIO.blocking { ZIO.succeed(statement.execute()) }
        _ <- ZIO.log(s"archivation completed ${batch.name}")
      yield new BatchArchivationResult
    }

  def dropTempTable(batch: Batch): Task[BatchArchivationResult] =
    val ack = ZIO.blocking {
      ZIO.succeed(sqlConnection.prepareStatement(s"DROP TABLE ${batch.name}"))
    }
    ZIO.acquireReleaseWith(ack)(st => ZIO.succeed(st.close())) { statement =>
      for
        _ <- ZIO.log(s"archiving batch ${batch.name}")
        _ <- ZIO.blocking { ZIO.succeed(statement.execute()) }
      yield new BatchArchivationResult
    }

  def close(): Unit = sqlConnection.close()

  private def collectPartitionColumn(resultSet: ResultSet, columnName: String): Seq[String] =
    // do not fail on closed result sets
    if resultSet.isClosed then
      Seq.empty
    else
      val current = resultSet.getString(columnName)
      if resultSet.next() then
        collectPartitionColumn(resultSet, columnName) :+ current
      else
        resultSet.close()
        Seq(current)


object JdbcConsumer:
  type Environment = JdbcConsumerOptions & ArchiveTableSettings
  
  /**
   * Factory method to create JdbcConsumer.
   * @param options The options for the consumer.
   * @return The initialized JdbcConsumer instance
   */
  def apply[Batch <: StagedVersionedBatch](options: JdbcConsumerOptions, archiveTableSettings: ArchiveTableSettings): JdbcConsumer[Batch] =
    new JdbcConsumer[Batch](options, archiveTableSettings)

  /**
   * The ZLayer that creates the JdbcConsumer.
   */
  val layer: ZLayer[Environment, Nothing, JdbcConsumer[StagedVersionedBatch]] =
    ZLayer.scoped {
      ZIO.fromAutoCloseable {
        for
          connectionOptions <- ZIO.service[JdbcConsumerOptions]
          archiveTableSettings <- ZIO.service[ArchiveTableSettings]
        yield JdbcConsumer(connectionOptions, archiveTableSettings)
      }
    }

