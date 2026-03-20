package com.sneaksanddata.arcane.microsoft_synapse_link

import com.azure.storage.common.StorageSharedKeyCredential
import com.sneaksanddata.arcane.framework.exceptions.StreamFailException
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.models.app.PluginStreamContext
import com.sneaksanddata.arcane.framework.services.app.{GenericStreamRunnerService, PosixStreamLifetimeService}
import com.sneaksanddata.arcane.framework.services.app.base.{StreamLifetimeService, StreamRunnerService}
import com.sneaksanddata.arcane.framework.services.bootstrap.DefaultStreamBootstrapper
import com.sneaksanddata.arcane.framework.services.synapse.SynapseHookManager
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClient
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.{
  FieldFilteringTransformer,
  StagingProcessor
}
import zio.*
import zio.logging.backend.SLF4J
import com.sneaksanddata.arcane.framework.services.filters.{
  FieldsFilteringService,
  FieldsFilteringService as FrameworkFieldsFilteringService
}
import com.sneaksanddata.arcane.framework.services.iceberg.{
  IcebergEntityManager,
  IcebergS3CatalogWriter,
  IcebergTablePropertyManager
}
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
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.backfill.{
  BackfillApplyBatchProcessor,
  BackfillOverwriteWatermarkProcessor
}
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.streaming.{
  DisposeBatchProcessor,
  MergeBatchProcessor,
  WatermarkProcessor
}
import com.sneaksanddata.arcane.framework.services.streaming.throughput.base.ThroughputShaperBuilder
import com.sneaksanddata.arcane.framework.services.synapse.base.{SynapseLinkDataProvider, SynapseLinkReader}
import com.sneaksanddata.arcane.framework.services.synapse.{
  SynapseBackfillOverwriteBatchFactory,
  SynapseLinkStreamingDataProvider
}
import com.sneaksanddata.arcane.microsoft_synapse_link.models.app.MicrosoftSynapseLinkPluginStreamContext

object main extends ZIOAppDefault {

  override val bootstrap: ZLayer[Any, Nothing, Unit] = Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  val appLayer: ZIO[StreamRunnerService, Throwable, Unit] = for
    _            <- zlog("Application starting")
    streamRunner <- ZIO.service[StreamRunnerService]
    _            <- streamRunner.run
  yield ()

  private def getExitCode(exception: Throwable): zio.ExitCode =
    exception match
      case _: StreamFailException => zio.ExitCode(2)
      case _                      => zio.ExitCode(1)

  val synapseLinkReaderLayer: ZLayer[PluginStreamContext, Throwable, SynapseLinkReader] =
    SynapseLinkReader.getLayer(context =>
      context.asInstanceOf[MicrosoftSynapseLinkPluginStreamContext].source.configuration
    )

  private lazy val streamRunner = appLayer.provide(
    MicrosoftSynapseLinkPluginStreamContext.layer,
    GenericStreamRunnerService.layer,
    GenericGraphBuilderFactory.composedLayer,
    DisposeBatchProcessor.layer,
    FieldFilteringTransformer.layer,
    MergeBatchProcessor.layer,
    StagingProcessor.layer,
    FieldsFilteringService.layer,
    PosixStreamLifetimeService.layer,
    synapseLinkReaderLayer,
    SynapseLinkStreamingDataProvider.layer,
    SynapseBackfillOverwriteBatchFactory.layer,
    SynapseLinkDataProvider.layer,
    IcebergS3CatalogWriter.layer,
    IcebergEntityManager.sinkLayer,
    IcebergEntityManager.stagingLayer,
    IcebergTablePropertyManager.stagingLayer,
    IcebergTablePropertyManager.sinkLayer,
    JdbcMergeServiceClient.layer,
    SynapseHookManager.layer,
    BackfillApplyBatchProcessor.layer,
    GenericBackfillStreamingOverwriteDataProvider.layer,
    GenericBackfillStreamingMergeDataProvider.layer,
    GenericStreamingGraphBuilder.backfillSubStreamLayer,
    DeclaredMetrics.layer,
    ArcaneDimensionsProvider.layer,
    DataDog.UdsPublisher.layer,
    WatermarkProcessor.layer,
    BackfillOverwriteWatermarkProcessor.layer,
    DefaultStreamBootstrapper.layer,
    ThroughputShaperBuilder.layer
  )

  @main
  def run: ZIO[Any, Throwable, Unit] =
    val app = streamRunner

    app.catchAllCause { cause =>
      for {
        _ <- zlog(s"Application failed: ${cause.squashTrace.getMessage}", cause)
        _ <- exit(getExitCode(cause.squashTrace))
      } yield ()
    }
}
