package com.sneaksanddata.arcane.microsoft_synapse_link

import models.app.{AzureConnectionSettings, MicrosoftSynapseLinkStreamContext}
import services.StreamGraphBuilderFactory
import services.app.logging.{JsonEnvironmentEnricher, StreamIdEnricher, StreamKindEnricher}
import services.app.{JdbcTableManager, StreamRunnerServiceCdm}
import services.clients.JdbcConsumer
import services.data_providers.microsoft_synapse_link.{AzureBlobStorageReaderZIO, CdmSchemaProvider, CdmTableStream}
import services.streaming.consumers.IcebergSynapseConsumer
import services.streaming.processors.{ArchivationProcessor, CdmGroupingProcessor, MergeBatchProcessor, SourceDeleteProcessor, TypeAlignmentService}

import com.azure.storage.common.StorageSharedKeyCredential
import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings.{GroupingSettings, VersionedDataGraphBuilderSettings}
import com.sneaksanddata.arcane.framework.services.app.PosixStreamLifetimeService
import com.sneaksanddata.arcane.framework.services.app.base.{StreamLifetimeService, StreamRunnerService}
import com.sneaksanddata.arcane.framework.services.app.logging.base.Enricher
import com.sneaksanddata.arcane.framework.services.lakehouse.IcebergS3CatalogWriter
import com.sneaksanddata.arcane.framework.services.storage.models.azure.AzureBlobStorageReader
import com.sneaksanddata.arcane.framework.services.streaming.base.StreamGraphBuilder
import org.slf4j.{Logger, LoggerFactory, MDC}
import zio.*
import zio.logging.LogFormat
import zio.logging.backend.SLF4J


object main extends ZIOAppDefault {

  private val loggingProprieties = Enricher("Application", "Arcane.Stream")
    ++ Enricher.fromEnvironment("APPLICATION_VERSION", "0.0.0")
    ++ JsonEnvironmentEnricher("ARCANE__LOGGING_PROPERTIES")
    ++ StreamKindEnricher.apply
    ++ StreamIdEnricher.apply

  override val bootstrap: ZLayer[Any, Nothing, Unit] = SLF4J.slf4j(
    LogFormat.make{ (builder, _, _, _, line, _, _, _, _) =>
      loggingProprieties.enrichLoggerWith(builder.appendKeyValue)
      loggingProprieties.enrichLoggerWith(MDC.put)
      builder.appendText(line())
    }
  )

  private val appLayer  = for
    _ <- ZIO.log("Application starting")
    context <- ZIO.service[StreamContext].debug("initialized stream context")
    streamRunner <- ZIO.service[StreamRunnerService].debug("initialized stream runner")
    _ <- streamRunner.run
  yield ()

  val storageExplorerLayerZio: ZLayer[AzureConnectionSettings & MicrosoftSynapseLinkStreamContext, Nothing, AzureBlobStorageReaderZIO] = ZLayer {
   for {
     connectionOptions <- ZIO.service[AzureConnectionSettings]
     streamContext <- ZIO.service[MicrosoftSynapseLinkStreamContext]
     credentials = StorageSharedKeyCredential(connectionOptions.account, connectionOptions.accessKey)
   } yield AzureBlobStorageReaderZIO(connectionOptions.account, connectionOptions.endpoint, credentials, streamContext.sourceDeleteDryRun)
  }
  
  val storageExplorerLayer: ZLayer[AzureConnectionSettings, Nothing, AzureBlobStorageReader] = ZLayer {
    for {
      connectionOptions <- ZIO.service[AzureConnectionSettings]
      credentials = StorageSharedKeyCredential(connectionOptions.account, connectionOptions.accessKey)
    } yield AzureBlobStorageReader(connectionOptions.account, connectionOptions.endpoint, credentials)
  }

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

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
      .catchAllTrace {
        case (e, trace) =>
          logger.error("Application failed", e)
          ZIO.fail(e)
      }
}

