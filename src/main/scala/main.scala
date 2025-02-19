package com.sneaksanddata.arcane.microsoft_synapse_link

import models.app.contracts.EnvironmentGarbageCollectorSettings
import models.app.{AzureConnectionSettings, GraphExecutionSettings, MicrosoftSynapseLinkStreamContext}
import services.app.{AzureBlobStorageGarbageCollector, FieldsFilteringService, GarbageCollectorStream, JdbcTableManager, StreamRunnerServiceCdm}
import services.clients.JdbcConsumer
import services.data_providers.microsoft_synapse_link.{AzureBlobStorageReaderZIO, CdmSchemaProvider, CdmTableStream}
import services.streaming.consumers.{IcebergSynapseBackfillConsumer, IcebergSynapseConsumer}
import services.streaming.processors.*

import com.azure.storage.common.StorageSharedKeyCredential
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings.{GroupingSettings, VersionedDataGraphBuilderSettings}
import com.sneaksanddata.arcane.framework.services.app.PosixStreamLifetimeService
import com.sneaksanddata.arcane.framework.services.app.base.{StreamLifetimeService, StreamRunnerService}
import com.sneaksanddata.arcane.framework.services.lakehouse.IcebergS3CatalogWriter
import com.sneaksanddata.arcane.framework.services.storage.models.azure.AzureBlobStorageReader
import com.sneaksanddata.arcane.framework.services.streaming.consumers.IcebergBackfillConsumer
import com.sneaksanddata.arcane.microsoft_synapse_link.services.graph_builder.{BackfillDataGraphBuilder, VersionedDataGraphBuilder}
import zio.*
import zio.logging.backend.SLF4J


object main extends ZIOAppDefault {

  override val bootstrap: ZLayer[Any, Nothing, Unit] = Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  private val streamApplication = for {
    _ <- zlog("Application starting")
    _ <- ZIO.service[StreamContext]
    streamRunner <- ZIO.service[StreamRunnerService]
    _ <- streamRunner.run
  } yield ()

  private val garbageCollectorApplication = for
    _ <- ZIO.log("Starting the garbage collector")
    stream <- ZIO.service[GarbageCollectorStream]
    _ <- stream.run
  yield ()


  private val storageExplorerLayerZio: ZLayer[AzureConnectionSettings & GraphExecutionSettings, Nothing, AzureBlobStorageReaderZIO] = ZLayer {
    for {
      connectionOptions <- ZIO.service[AzureConnectionSettings]
      executionSettings <- ZIO.service[GraphExecutionSettings]
      credentials = StorageSharedKeyCredential(connectionOptions.account, connectionOptions.accessKey)
    } yield AzureBlobStorageReaderZIO(connectionOptions.account, connectionOptions.endpoint, credentials, executionSettings.sourceDeleteDryRun)
  }

  private val storageExplorerLayer: ZLayer[AzureConnectionSettings, Nothing, AzureBlobStorageReader] = ZLayer {
    for {
      connectionOptions <- ZIO.service[AzureConnectionSettings]
      credentials = StorageSharedKeyCredential(connectionOptions.account, connectionOptions.accessKey)
    } yield AzureBlobStorageReader(connectionOptions.account, connectionOptions.endpoint, credentials)
  }

  private lazy val garbageCollector = garbageCollectorApplication.provide(
    storageExplorerLayerZio,
    EnvironmentGarbageCollectorSettings.layer,
    AzureBlobStorageGarbageCollector.layer)

  private lazy val streamRunner = streamApplication.provide(
    storageExplorerLayer,
    storageExplorerLayerZio,
    CdmTableStream.layer,
    CdmSchemaProvider.layer,
    MicrosoftSynapseLinkStreamContext.layer,
    PosixStreamLifetimeService.layer,
    StreamRunnerServiceCdm.layer,
    IcebergS3CatalogWriter.layer,
    IcebergSynapseConsumer.layer,
    MergeBatchProcessor.layer,
    CdmGroupingProcessor.layer,
    ArchivationProcessor.layer,
    TypeAlignmentService.layer,
    SourceDeleteProcessor.layer,
    JdbcTableManager.layer,
    StagingTableProcessor.layer,
    BackfillDataGraphBuilder.layer,
    VersionedDataGraphBuilder.layer,
    JdbcConsumer.layer,
    MicrosoftSynapseLinkDataProviderImpl.layer,
    IcebergSynapseBackfillConsumer.layer,
    FieldFilteringProcessor.layer,
    FieldsFilteringService.layer)

  @main
  def run: ZIO[Any, Throwable, Unit] =
    val app = for
      mode <- System.env("ARCANE__MODE")
      _ <- mode match
        case Some("garbage-collector") => garbageCollector
        case None => streamRunner
        case Some(_) => streamRunner
    yield ()

    app.catchAllCause { cause =>
      for {
        _ <- zlog(s"Application failed: ${cause.squashTrace.getMessage}", cause)
        _ <- exit(zio.ExitCode(1))
      } yield ()
    }
}
