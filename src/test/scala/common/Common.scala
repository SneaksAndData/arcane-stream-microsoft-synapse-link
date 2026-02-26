package com.sneaksanddata.arcane.microsoft_synapse_link
package common

import main.appLayer
import models.app.{AzureConnectionSettings, MicrosoftSynapseLinkStreamContext}

import com.azure.storage.common.StorageSharedKeyCredential
import com.sneaksanddata.arcane.framework.services.app.GenericStreamRunnerService
import com.sneaksanddata.arcane.framework.services.caching.schema_cache.MutableSchemaCache
import com.sneaksanddata.arcane.framework.services.filters.FieldsFilteringService
import com.sneaksanddata.arcane.framework.services.iceberg.{IcebergS3CatalogWriter, IcebergTablePropertyManager}
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
import com.sneaksanddata.arcane.framework.services.synapse.base.{SynapseLinkDataProvider, SynapseLinkReader}
import com.sneaksanddata.arcane.framework.services.synapse.{
  SynapseBackfillOverwriteBatchFactory,
  SynapseHookManager,
  SynapseLinkStreamingDataProvider
}
import com.sneaksanddata.arcane.framework.testkit.appbuilder.TestAppBuilder.buildTestApp
import com.sneaksanddata.arcane.framework.testkit.streaming.TimeLimitLifetimeService
import zio.{ZIO, ZLayer}

import java.time.Duration

/** Common utilities for tests.
  */
object Common:

  private val storageExplorerLayer: ZLayer[AzureConnectionSettings, Nothing, AzureBlobStorageReader] = ZLayer {
    for {
      connectionOptions <- ZIO.service[AzureConnectionSettings]
      credentials = StorageSharedKeyCredential(connectionOptions.account, connectionOptions.accessKey)
    } yield AzureBlobStorageReader(connectionOptions.account, connectionOptions.endpoint, credentials)
  }

  /** Builds the test application from the provided layers.
    * @param streamContextLayer
    *   The stream context layer.
    * @return
    *   The test application.
    */
  def getTestApp(
      runTimeout: Duration,
      streamContextLayer: ZLayer[Any, Nothing, MicrosoftSynapseLinkStreamContext.Environment]
  ): ZIO[Any, Throwable, Unit] =
    buildTestApp(
      appLayer,
      storageExplorerLayer,
      SynapseLinkStreamingDataProvider.layer,
      SynapseBackfillOverwriteBatchFactory.layer,
      streamContextLayer,
      SynapseHookManager.layer
    )(
      GenericStreamRunnerService.layer,
      GenericGraphBuilderFactory.composedLayer,
      GenericGroupingTransformer.layer,
      DisposeBatchProcessor.layer,
      FieldFilteringTransformer.layer,
      MergeBatchProcessor.layer,
      StagingProcessor.layer,
      FieldsFilteringService.layer,
      ZLayer.succeed(TimeLimitLifetimeService(runTimeout)),
      IcebergS3CatalogWriter.layer,
      JdbcMergeServiceClient.layer,
      BackfillApplyBatchProcessor.layer,
      GenericBackfillStreamingOverwriteDataProvider.layer,
      GenericBackfillStreamingMergeDataProvider.layer,
      GenericStreamingGraphBuilder.backfillSubStreamLayer,
      ZLayer.succeed(MutableSchemaCache()),
      DeclaredMetrics.layer,
      ArcaneDimensionsProvider.layer,
      DataDog.UdsPublisher.layer,
      WatermarkProcessor.layer,
      BackfillOverwriteWatermarkProcessor.layer,
      IcebergTablePropertyManager.layer,
      // had to move these as they are Throwable instead of Nothing.
      SynapseLinkReader.layer,
      SynapseLinkDataProvider.layer
    )
