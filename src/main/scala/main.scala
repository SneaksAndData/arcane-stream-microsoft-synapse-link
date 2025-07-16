package com.sneaksanddata.arcane.microsoft_synapse_link

import models.app.{AzureConnectionSettings, GraphExecutionSettings, MicrosoftSynapseLinkStreamContext}

import com.azure.storage.common.StorageSharedKeyCredential
import com.sneaksanddata.arcane.framework.excpetions.StreamFailException
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings.{GroupingSettings, VersionedDataGraphBuilderSettings}
import com.sneaksanddata.arcane.framework.services.app.{GenericStreamRunnerService, PosixStreamLifetimeService}
import com.sneaksanddata.arcane.framework.services.app.base.{StreamLifetimeService, StreamRunnerService}
import com.sneaksanddata.arcane.framework.services.caching.schema_cache.MutableSchemaCache
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
import com.sneaksanddata.arcane.framework.services.iceberg.IcebergS3CatalogWriter
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
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.backfill.BackfillApplyBatchProcessor
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.streaming.{
  DisposeBatchProcessor,
  MergeBatchProcessor
}
import com.sneaksanddata.arcane.framework.services.synapse.base.{SynapseLinkDataProvider, SynapseLinkReader}
import com.sneaksanddata.arcane.framework.services.synapse.{
  SynapseBackfillOverwriteBatchFactory,
  SynapseLinkStreamingDataProvider
}

object main extends ZIOAppDefault {

  override val bootstrap: ZLayer[Any, Nothing, Unit] = Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  val appLayer: ZIO[StreamRunnerService, Throwable, Unit] = for
    _            <- zlog("Application starting")
    streamRunner <- ZIO.service[StreamRunnerService]
    _            <- streamRunner.run
  yield ()

  private val storageExplorerLayer: ZLayer[AzureConnectionSettings, Nothing, AzureBlobStorageReader] = ZLayer {
    for {
      connectionOptions <- ZIO.service[AzureConnectionSettings]
      credentials = StorageSharedKeyCredential(connectionOptions.account, connectionOptions.accessKey)
    } yield AzureBlobStorageReader(connectionOptions.account, connectionOptions.endpoint, credentials)
  }

  private val schemaCache = MutableSchemaCache()

  private def getExitCode(exception: Throwable): zio.ExitCode =
    exception match
      case _: StreamFailException => zio.ExitCode(2)
      case _                      => zio.ExitCode(1)

  private lazy val streamRunner = appLayer.provide(
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
    MicrosoftSynapseLinkStreamContext.layer,
    IcebergS3CatalogWriter.layer,
    JdbcMergeServiceClient.layer,
    SynapseHookManager.layer,
    BackfillApplyBatchProcessor.layer,
    GenericBackfillStreamingOverwriteDataProvider.layer,
    GenericBackfillStreamingMergeDataProvider.layer,
    GenericStreamingGraphBuilder.backfillSubStreamLayer,
    ZLayer.succeed(schemaCache),
    DeclaredMetrics.layer,
    ArcaneDimensionsProvider.layer,
    DataDog.UdsPublisher.layer
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
