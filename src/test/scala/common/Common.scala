package com.sneaksanddata.arcane.microsoft_synapse_link
package common

import main.{appLayer, synapseLinkReaderLayer}
import models.app.MicrosoftSynapseLinkPluginStreamContext

import com.sneaksanddata.arcane.framework.services.app.{GenericStreamRunnerService, StreamGraphResolver}
import com.sneaksanddata.arcane.framework.services.backfill.DefaultBackfillStateManager
import com.sneaksanddata.arcane.framework.services.backfill.processors.{
  BackfillCompletionProcessor,
  ShardStagingProcessor
}
import com.sneaksanddata.arcane.framework.services.bootstrap.DefaultStreamBootstrapper
import com.sneaksanddata.arcane.framework.services.filters.FieldsFilteringService
import com.sneaksanddata.arcane.framework.services.iceberg.{
  IcebergEntityManager,
  IcebergS3CatalogWriter,
  IcebergTablePropertyManager
}
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClient
import com.sneaksanddata.arcane.framework.services.merging.cleanup.CatalogDisposeServiceClient
import com.sneaksanddata.arcane.framework.services.metrics.{DeclaredMetrics, GlobalMetricTagProvider}
import com.sneaksanddata.arcane.framework.services.naming.DefaultNameGenerator
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.maintenance.TargetMaintenanceProcessor
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.streaming.{
  DisposeBatchProcessor,
  MergeBatchProcessor,
  SchemaMigrationProcessor,
  WatermarkProcessor
}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.{
  FieldFilteringTransformer,
  StagingProcessor
}
import com.sneaksanddata.arcane.framework.services.streaming.throughput.base.ThroughputShaperBuilder
import com.sneaksanddata.arcane.framework.services.synapse.{SynapseBatchFactory, SynapseLinkStreamingDataProvider}
import com.sneaksanddata.arcane.framework.services.synapse.backfill.{
  SynapseBackfillMergeStreamDataProvider,
  SynapseBackfillSourceDataProvider,
  SynapseShardFactory,
  SynapseShardedBackfillStreamDataProvider
}
import com.sneaksanddata.arcane.framework.services.synapse.base.SynapseLinkDataProvider
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
      streamContextLayer
    )(
      GenericStreamRunnerService.layer,
      StreamGraphResolver.composedLayer,
      DisposeBatchProcessor.layer,
      FieldFilteringTransformer.layer,
      MergeBatchProcessor.layer,
      StagingProcessor.layer,
      FieldsFilteringService.layer,
      ZLayer.succeed(TimeLimitLifetimeService(runTimeout)),
      IcebergS3CatalogWriter.layer,
      JdbcMergeServiceClient.layer,
      DeclaredMetrics.layer,
      GlobalMetricTagProvider.layer,
      WatermarkProcessor.layer,
      IcebergEntityManager.sinkLayer,
      IcebergEntityManager.stagingLayer,
      IcebergTablePropertyManager.stagingLayer,
      IcebergTablePropertyManager.sinkLayer,
      SynapseLinkDataProvider.layer,

      // had to move these as they are Throwable instead of Nothing.
      synapseLinkReaderLayer,
      DefaultStreamBootstrapper.layer,
      ThroughputShaperBuilder.layer,
      // streaming
      SynapseLinkStreamingDataProvider.layer,
      SynapseBatchFactory.layer,

      // backfill
      SynapseBackfillSourceDataProvider.layer,
      SynapseShardFactory.layer,
      SynapseShardedBackfillStreamDataProvider.layer,
      SynapseBackfillMergeStreamDataProvider.layer,
      DefaultBackfillStateManager.layer,
      ShardStagingProcessor.layer,
      BackfillCompletionProcessor.layer,

      // schema
      SchemaMigrationProcessor.layer,

      // maintenance and cleanup
      TargetMaintenanceProcessor.layer,
      CatalogDisposeServiceClient.layer,
      DefaultNameGenerator.layer
    )
