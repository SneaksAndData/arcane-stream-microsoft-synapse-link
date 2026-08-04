package com.sneaksanddata.arcane.microsoft_synapse_link
package common

import main.{appLayer, synapseLinkReaderLayer}
import models.app.MicrosoftSynapseLinkPluginStreamContext

import com.sneaksanddata.arcane.framework.plugins.LayerAssemblies
import com.sneaksanddata.arcane.framework.services.app.{GenericStreamRunnerService, StreamGraphResolver}
import com.sneaksanddata.arcane.framework.services.backfill.DefaultBackfillStateManager
import com.sneaksanddata.arcane.framework.services.backfill.processors.{
  BackfillCompletionProcessor,
  ShardStagingProcessor
}
import com.sneaksanddata.arcane.framework.services.bootstrap.DefaultStreamBootstrapper
import com.sneaksanddata.arcane.framework.services.completion.DefaultStreamFinalizer
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
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig
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
      streamContextLayer: ZLayer[
        Any,
        Nothing,
        MicrosoftSynapseLinkPluginStreamContext & DatagramSocketConfig & MetricsConfig & DatadogPublisherConfig
      ]
  ): ZIO[Any, Throwable, Unit] =
    buildTestApp(
      appLayer,
      streamContextLayer
    )(
      LayerAssemblies.synapseLinkSourceLayer,
      LayerAssemblies.frameworkPipelineServicesLayer,
      LayerAssemblies.frameworkStagingServicesLayer,

      // had to move these as they are Throwable instead of Nothing.
      synapseLinkReaderLayer,
      GenericStreamRunnerService.layer,
      StreamGraphResolver.composedLayer
    )
