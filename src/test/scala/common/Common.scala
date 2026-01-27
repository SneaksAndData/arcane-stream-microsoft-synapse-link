package com.sneaksanddata.arcane.microsoft_synapse_link
package common

import main.{appLayer, schemaCache, storageExplorerLayer}

import com.azure.storage.common.StorageSharedKeyCredential
import models.app.{AzureConnectionSettings, MicrosoftSynapseLinkStreamContext}

import com.sneaksanddata.arcane.framework.services.app.{GenericStreamRunnerService, PosixStreamLifetimeService}
import com.sneaksanddata.arcane.framework.services.app.base.{InterruptionToken, StreamLifetimeService}
import com.sneaksanddata.arcane.framework.services.caching.schema_cache.MutableSchemaCache
import com.sneaksanddata.arcane.framework.services.filters.{ColumnSummaryFieldsFilteringService, FieldsFilteringService}
import com.sneaksanddata.arcane.framework.services.iceberg.IcebergS3CatalogWriter
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClient
import com.sneaksanddata.arcane.framework.services.metrics.{ArcaneDimensionsProvider, DataDog, DeclaredMetrics}
import com.sneaksanddata.arcane.framework.services.storage.services.azure.AzureBlobStorageReader
import com.sneaksanddata.arcane.framework.services.streaming.data_providers.backfill.{
  GenericBackfillStreamingMergeDataProvider,
  GenericBackfillStreamingOverwriteDataProvider
}
import com.sneaksanddata.arcane.framework.services.streaming.graph_builders.{
  GenericGraphBuilderFactory,
  GenericStreamingGraphBuilder
}
import com.sneaksanddata.arcane.framework.services.streaming.processors.GenericGroupingTransformer
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.backfill.{
  BackfillApplyBatchProcessor,
  BackfillOverwriteWatermarkProcessor
}
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.streaming.{
  DisposeBatchProcessor,
  MergeBatchProcessor,
  WatermarkProcessor
}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.{
  FieldFilteringTransformer,
  StagingProcessor
}
import com.sneaksanddata.arcane.framework.services.synapse.{
  SynapseBackfillOverwriteBatchFactory,
  SynapseHookManager,
  SynapseLinkStreamingDataProvider
}
import com.sneaksanddata.arcane.framework.services.synapse.base.{SynapseLinkDataProvider, SynapseLinkReader}
import com.sneaksanddata.arcane.framework.services.synapse.versioning.SynapseWatermark
import zio.{ZIO, ZLayer}

import java.sql.{Connection, DriverManager, ResultSet}
import java.time.Duration

/** Common utilities for tests.
  */
object Common:

  type StreamLifeTimeServiceLayer = ZLayer[Any, Nothing, StreamLifetimeService & InterruptionToken]
  type StreamContextLayer         = ZLayer[Any, Nothing, MicrosoftSynapseLinkStreamContext.Environment]

  private val storageExplorerLayer: ZLayer[AzureConnectionSettings, Nothing, AzureBlobStorageReader] = ZLayer {
    for {
      connectionOptions <- ZIO.service[AzureConnectionSettings]
      credentials = StorageSharedKeyCredential(connectionOptions.account, connectionOptions.accessKey)
    } yield AzureBlobStorageReader(connectionOptions.account, connectionOptions.endpoint, credentials)
  }

  /** Builds the test application from the provided layers.
    * @param lifetimeService
    *   The lifetime service layer.
    * @param streamContextLayer
    *   The stream context layer.
    * @return
    *   The test application.
    */
  def buildTestApp(
      lifetimeService: StreamLifeTimeServiceLayer,
      streamContextLayer: StreamContextLayer
  ): ZIO[Any, Throwable, Unit] =
    appLayer.provide(
      storageExplorerLayer,
      GenericStreamRunnerService.layer,
      GenericGraphBuilderFactory.composedLayer,
      GenericGroupingTransformer.layer,
      DisposeBatchProcessor.layer,
      FieldFilteringTransformer.layer,
      MergeBatchProcessor.layer,
      StagingProcessor.layer,
      FieldsFilteringService.layer,
      PosixStreamLifetimeService.layer,
      SynapseLinkStreamingDataProvider.layer,
      SynapseBackfillOverwriteBatchFactory.layer,
      SynapseLinkReader.layer,
      SynapseLinkDataProvider.layer,
      streamContextLayer,
      IcebergS3CatalogWriter.layer,
      JdbcMergeServiceClient.layer,
      SynapseHookManager.layer,
      BackfillApplyBatchProcessor.layer,
      GenericBackfillStreamingOverwriteDataProvider.layer,
      GenericBackfillStreamingMergeDataProvider.layer,
      GenericStreamingGraphBuilder.backfillSubStreamLayer,
      ZLayer.succeed(MutableSchemaCache()),
      DeclaredMetrics.layer,
      ArcaneDimensionsProvider.layer,
      DataDog.UdsPublisher.layer,
      WatermarkProcessor.layer,
      BackfillOverwriteWatermarkProcessor.layer
    )

  /** Gets the data from the *target* table. Using the connection string provided in the
    * `ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI` environment variable.
    * @param targetTableName
    *   The name of the target table.
    * @param decoder
    *   The decoder for the result set.
    * @tparam Result
    *   The type of the result.
    * @return
    *   A ZIO effect that gets the data.
    */
  def getData[Result](
      targetTableName: String,
      columnList: String,
      decoder: ResultSet => Result
  ): ZIO[Any, Throwable, List[Result]] = ZIO.scoped {
    for
      connection <- ZIO.attempt(DriverManager.getConnection(sys.env("ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI")))
      statement  <- ZIO.attempt(connection.createStatement())
      resultSet <- ZIO.fromAutoCloseable(
        ZIO.attemptBlocking(statement.executeQuery(s"SELECT $columnList from $targetTableName"))
      )
      data <- ZIO.attempt {
        Iterator
          .continually((resultSet.next(), resultSet))
          .takeWhile(_._1)
          .map { case (_, rs) => decoder(rs) }
          .toList
      }
    yield data
  }

  def getWatermark(targetTableName: String): ZIO[Any, Throwable, SynapseWatermark] = ZIO.scoped {
    for
      connection <- ZIO.attempt(DriverManager.getConnection(sys.env("ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI")))
      statement  <- ZIO.attempt(connection.createStatement())
      resultSet <- ZIO.fromAutoCloseable(
        ZIO.attemptBlocking(
          statement.executeQuery(
            s"SELECT value FROM iceberg.test.\"$targetTableName$$properties\" WHERE key = 'comment'"
          )
        )
      )
      _         <- ZIO.attemptBlocking(resultSet.next())
      watermark <- ZIO.attempt(SynapseWatermark.fromJson(resultSet.getString("value")))
    yield watermark
  }

  val StrStrDecoder: ResultSet => (String, String) = (rs: ResultSet) => (rs.getString(1), rs.getString(2))

  def waitForData[T](
      tableName: String,
      columnList: String,
      decoder: ResultSet => T,
      expectedSize: Int
  ): ZIO[Any, Nothing, Unit] = ZIO
    .sleep(Duration.ofSeconds(1))
    .repeatUntilZIO(_ =>
      (for {
        _ <- ZIO.log(s"Waiting for data to be loaded for $tableName, schema: $columnList")
        inserted <- Common.getData(
          tableName,
          columnList,
          decoder
        )
        _ <- ZIO.log(s"Loaded so far: ${inserted.size}, expecting: $expectedSize")
      } yield inserted.length == expectedSize).orElseSucceed(false)
    )
