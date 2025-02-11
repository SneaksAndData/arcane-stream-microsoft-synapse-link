package com.sneaksanddata.arcane.microsoft_synapse_link

import models.app.{AzureConnectionSettings, GraphExecutionSettings, MicrosoftSynapseLinkStreamContext}
import services.StreamGraphBuilderFactory
import services.app.logging.{JsonEnvironmentEnricher, StreamIdEnricher, StreamKindEnricher}
import services.app.{AzureBlobStorageGarbageCollector, GarbageCollectorStream, JdbcTableManager, StreamRunnerServiceCdm}
import services.app.{JdbcTableManager, StreamRunnerServiceCdm}
import services.clients.JdbcConsumer
import services.data_providers.microsoft_synapse_link.{AzureBlobStorageReaderZIO, CdmSchemaProvider, CdmTableStream}
import services.streaming.consumers.IcebergSynapseConsumer
import services.streaming.processors.{ArchivationProcessor, CdmGroupingProcessor, MergeBatchProcessor, SourceDeleteProcessor, TypeAlignmentService}
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.*

import com.azure.storage.common.StorageSharedKeyCredential
import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings.{GroupingSettings, VersionedDataGraphBuilderSettings}
import com.sneaksanddata.arcane.framework.services.app.PosixStreamLifetimeService
import com.sneaksanddata.arcane.framework.services.app.base.{StreamLifetimeService, StreamRunnerService}
import com.sneaksanddata.arcane.framework.services.lakehouse.IcebergS3CatalogWriter
import com.sneaksanddata.arcane.framework.services.storage.models.azure.AzureBlobStorageReader
import com.sneaksanddata.arcane.microsoft_synapse_link.models.app.contracts.EnvironmentGarbageCollectorSettings
import com.sneaksanddata.arcane.framework.services.streaming.base.StreamGraphBuilder
import com.sneaksanddata.arcane.microsoft_synapse_link.models.app.contracts.EnvironmentGarbageCollectorSettings
import org.slf4j.{Logger, LoggerFactory, MDC}
import zio.*
import zio.logging.backend.SLF4J


object main extends ZIOAppDefault {

  override val bootstrap: ZLayer[Any, Nothing, Unit] = Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  private val appLayer = for {
    _ <- zlog("Application starting")
    _ <- ZIO.service[StreamContext]
    streamRunner <- ZIO.service[StreamRunnerService]
    _ <- streamRunner.run
  } yield ()

  private val storageExplorerLayerZio: ZLayer[AzureConnectionSettings & MicrosoftSynapseLinkStreamContext, Nothing, AzureBlobStorageReaderZIO] = ZLayer {
    for {
      connectionOptions <- ZIO.service[AzureConnectionSettings]
      streamContext <- ZIO.service[MicrosoftSynapseLinkStreamContext]
      streamContext <- ZIO.service[MicrosoftSynapseLinkStreamContext]
      credentials = StorageSharedKeyCredential(connectionOptions.account, connectionOptions.accessKey)
    } yield AzureBlobStorageReaderZIO(connectionOptions.account, connectionOptions.endpoint, credentials, streamContext.sourceDeleteDryRun)
  }


  private val runGarbageCollector = for
    _ <- ZIO.log("Starting the garbage collector")
    stream <- ZIO.service[GarbageCollectorStream]
    _ <- stream.run
  yield ()


  private val storageExplorerLayer: ZLayer[AzureConnectionSettings, Nothing, AzureBlobStorageReader] = ZLayer {
    for {
      connectionOptions <- ZIO.service[AzureConnectionSettings]
      credentials = StorageSharedKeyCredential(connectionOptions.account, connectionOptions.accessKey)
    } yield AzureBlobStorageReader(connectionOptions.account, connectionOptions.endpoint, credentials)
  }

  private val garbageCollector = runGarbageCollector.provide(
    storageExplorerLayerZio,
    EnvironmentGarbageCollectorSettings.layer,
    AzureBlobStorageGarbageCollector.layer)

  @main
  def run: ZIO[Any, Throwable, Unit] =
    appLayer.provide(
      storageExplorerLayer,
      storageExplorerLayerZio,
      CdmTableStream.layer,
      CdmSchemaProvider.layer,
      MicrosoftSynapseLinkStreamContext.layer,
      PosixStreamLifetimeService.layer,
      StreamRunnerServiceCdm.layer,
      StreamGraphBuilderFactory.layer,
      IcebergS3CatalogWriter.layer,
      IcebergSynapseConsumer.layer,
      MergeBatchProcessor.layer,
      JdbcConsumer.layer,
      CdmGroupingProcessor.layer,
      ArchivationProcessor.layer,
      TypeAlignmentService.layer,
      SourceDeleteProcessor.layer,
      JdbcTableManager.layer)
      .catchAllCause {cause =>
        for {
          _ <- zlog(s"Application failed: ${cause.squashTrace.getMessage}", cause)
          _ <- exit(zio.ExitCode(1))
        } yield ()
      }
}

@main
def run: ZIO[Any, Throwable, Unit] =
  val app = for
    mode <- System.env("ARCANE__MODE")
    _ <- mode match
      case Some("garbage-collector") => garbageCollector
      case None => streamRunner
      case Some(_) => streamRunner
  yield ()

  app.catchAllTrace {
    case (e, trace) =>
      logger.error("Application failed", e)
      ZIO.fail(e)
  }
}
