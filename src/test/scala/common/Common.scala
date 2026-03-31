package com.sneaksanddata.arcane.microsoft_synapse_link
package common

import main.{appLayer, synapseLinkReaderLayer}
import models.app.MicrosoftSynapseLinkPluginStreamContext

import com.sneaksanddata.arcane.framework.services.app.GenericStreamRunnerService
import com.sneaksanddata.arcane.framework.services.bootstrap.DefaultStreamBootstrapper
import com.sneaksanddata.arcane.framework.services.filters.FieldsFilteringService
import com.sneaksanddata.arcane.framework.services.iceberg.{
  IcebergEntityManager,
  IcebergS3CatalogWriter,
  IcebergTablePropertyManager
}
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClient
import com.sneaksanddata.arcane.framework.services.metrics.{ArcaneDimensionsProvider, DeclaredMetrics}
import com.sneaksanddata.arcane.framework.services.streaming.data_providers.backfill.{
  GenericBackfillStreamingMergeDataProvider,
  GenericBackfillStreamingOverwriteDataProvider
}
import com.sneaksanddata.arcane.framework.services.streaming.graph_builders.{
  GenericGraphBuilderFactory,
  GenericStreamingGraphBuilder
}
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
import com.sneaksanddata.arcane.framework.services.streaming.throughput.base.ThroughputShaperBuilder
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

  /** Builds the test application from the provided layers.
    * @param streamContextLayer
    *   The stream context layer.
    * @return
    *   The test application.
    */
  def getTestApp(
      runTimeout: Duration,
      streamContextLayer: ZLayer[Any, Nothing, MicrosoftSynapseLinkPluginStreamContext]
  ): ZIO[Any, Throwable, Unit] =
    buildTestApp(
      appLayer,
      SynapseLinkStreamingDataProvider.layer,
      SynapseBackfillOverwriteBatchFactory.layer,
      streamContextLayer,
      SynapseHookManager.layer
    )(
      GenericStreamRunnerService.layer,
      GenericGraphBuilderFactory.composedLayer,
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
      DeclaredMetrics.layer,
      ArcaneDimensionsProvider.layer,
      WatermarkProcessor.layer,
      BackfillOverwriteWatermarkProcessor.layer,
      IcebergEntityManager.sinkLayer,
      IcebergEntityManager.stagingLayer,
      IcebergTablePropertyManager.stagingLayer,
      IcebergTablePropertyManager.sinkLayer,
      // had to move these as they are Throwable instead of Nothing.
      synapseLinkReaderLayer,
      SynapseLinkDataProvider.layer,
      DefaultStreamBootstrapper.layer,
      ThroughputShaperBuilder.layer
    )
