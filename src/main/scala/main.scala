package com.sneaksanddata.arcane.microsoft_synapse_link

import models.app.contracts.EnvironmentGarbageCollectorSettings
import models.app.{AzureConnectionSettings, GraphExecutionSettings, MicrosoftSynapseLinkStreamContext}
import services.app.*
import services.graph_builder.{BackfillDataGraphBuilder, VersionedDataGraphBuilder}
import services.streaming.consumers.{IcebergSynapseBackfillConsumer, IcebergSynapseConsumer}
import services.streaming.processors.*

import com.azure.storage.common.StorageSharedKeyCredential
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings.{GroupingSettings, VersionedDataGraphBuilderSettings}
import com.sneaksanddata.arcane.framework.services.app.PosixStreamLifetimeService
import com.sneaksanddata.arcane.framework.services.app.base.{StreamLifetimeService, StreamRunnerService}
import com.sneaksanddata.arcane.framework.services.cdm.{CdmSchemaProvider, SynapseHookManager}
import com.sneaksanddata.arcane.framework.services.merging.{JdbcMergeServiceClient, MutableSchemaCache}
import com.sneaksanddata.arcane.framework.services.storage.services.AzureBlobStorageReader
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.StagingProcessor
import com.sneaksanddata.arcane.microsoft_synapse_link.services.data_providers.microsoft_synapse_link.{CdmTableStream, MicrosoftSynapseLinkDataProviderImpl}
import zio.*
import zio.logging.backend.SLF4J
import com.sneaksanddata.arcane.framework.services.filters.FieldsFilteringService as FrameworkFieldsFilteringService
import com.sneaksanddata.arcane.framework.services.lakehouse.IcebergS3CatalogWriter
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.streaming.{DisposeBatchProcessor, MergeBatchProcessor}


object main extends ZIOAppDefault {

  override val bootstrap: ZLayer[Any, Nothing, Unit] = Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  private val streamApplication = for {
    _ <- zlog("Application starting")
    _ <- ZIO.service[StreamContext]
    streamRunner <- ZIO.service[StreamRunnerService]
    _ <- streamRunner.run
  } yield ()

  private val storageExplorerLayer: ZLayer[AzureConnectionSettings, Nothing, AzureBlobStorageReader] = ZLayer {
    for {
      connectionOptions <- ZIO.service[AzureConnectionSettings]
      credentials = StorageSharedKeyCredential(connectionOptions.account, connectionOptions.accessKey)
    } yield AzureBlobStorageReader(connectionOptions.account, connectionOptions.endpoint, credentials)
  }

  private val schemaCache = MutableSchemaCache()

  private lazy val streamRunner = streamApplication.provide(
    storageExplorerLayer,
    CdmTableStream.layer,
    CdmSchemaProvider.layer,
    MicrosoftSynapseLinkStreamContext.layer,
    PosixStreamLifetimeService.layer,
    StreamRunnerServiceCdm.layer,
    IcebergS3CatalogWriter.layer,
    IcebergSynapseConsumer.layer,
    MergeBatchProcessor.layer,
    CdmGroupingProcessor.layer,
    TypeAlignmentService.layer,
    BackfillDataGraphBuilder.layer,
    VersionedDataGraphBuilder.layer,
    MicrosoftSynapseLinkDataProviderImpl.layer,
    IcebergSynapseBackfillConsumer.layer,
    FieldFilteringProcessor.layer,
    FieldsFilteringService.layer,
    FrameworkFieldsFilteringService.layer,
    StagingProcessor.layer,
    JdbcMergeServiceClient.layer,
    DisposeBatchProcessor.layer,
    SynapseHookManager.layer,
    ZLayer.succeed(schemaCache))

  @main
  def run: ZIO[Any, Throwable, Unit] =
    val app = streamRunner

    app.catchAllCause { cause =>
      for {
        _ <- zlog(s"Application failed: ${cause.squashTrace.getMessage}", cause)
        _ <- exit(zio.ExitCode(1))
      } yield ()
    }
}
