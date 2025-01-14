package com.sneaksanddata.arcane.microsoft_synapse_link

import models.app.{AzureConnectionSettings, MicrosoftSynapseLinkStreamContext}
import services.StreamGraphBuilderFactory
import services.data_providers.microsoft_synapse_link.{CdmDataProvider, CdmSchemaProvider}
import services.clients.JdbcConsumer
import services.streaming.consumers.IcebergSynapseConsumer
import services.streaming.processors.{ArchivationProcessor, CdmGroupingProcessor, MergeBatchProcessor, TypeAlignmentService}

import com.azure.storage.common.StorageSharedKeyCredential
import com.sneaksanddata.arcane.framework.models.DataRow
import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings.{GroupingSettings, VersionedDataGraphBuilderSettings}
import com.sneaksanddata.arcane.framework.services.app.base.{StreamLifetimeService, StreamRunnerService}
import com.sneaksanddata.arcane.framework.services.app.logging.base.Enricher
import com.sneaksanddata.arcane.framework.services.app.{PosixStreamLifetimeService, StreamRunnerServiceImpl}
import com.sneaksanddata.arcane.framework.services.lakehouse.IcebergS3CatalogWriter
import com.sneaksanddata.arcane.framework.services.storage.models.azure.AzureBlobStorageReader
import com.sneaksanddata.arcane.framework.services.streaming.base.{BatchProcessor, StreamGraphBuilder}
import com.sneaksanddata.arcane.framework.services.streaming.consumers.IcebergBackfillConsumer
import com.sneaksanddata.arcane.framework.services.streaming.processors.{BackfillGroupingProcessor, MergeProcessor}
import com.sneaksanddata.arcane.microsoft_synapse_link.services.app.{JdbcTableManager, StreamRunnerServiceCdm}
import org.slf4j.MDC
import zio.*
import zio.logging.LogFormat
import zio.logging.backend.SLF4J


object main extends ZIOAppDefault {

  private val loggingProprieties = Enricher("Application", "Arcane.Stream.Scala")
    ++ Enricher("App", "Arcane.Stream.Scala")
    ++ Enricher.fromEnvironment("APPLICATION_VERSION", "0.0.0")

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

  val storageExplorerLayer: ZLayer[AzureConnectionSettings, Nothing, AzureBlobStorageReader] = ZLayer {
   for {
     connectionOptions <- ZIO.service[AzureConnectionSettings]
     credentials = StorageSharedKeyCredential(connectionOptions.account, connectionOptions.accessKey)
   } yield AzureBlobStorageReader(connectionOptions.account, connectionOptions.endpoint, credentials)
  }

  @main
  def run: ZIO[Any, Throwable, Unit] =
    appLayer.provide(
      storageExplorerLayer,
      CdmDataProvider.layer,
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
      JdbcTableManager.layer)
    .orDie
}

