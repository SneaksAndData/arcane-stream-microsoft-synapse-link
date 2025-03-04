package com.sneaksanddata.arcane.microsoft_synapse_link
package services.app

import models.app.MicrosoftSynapseLinkStreamContext
import services.graph_builder.{BackfillDataGraphBuilder, VersionedDataGraphBuilder}

import com.sneaksanddata.arcane.framework.models.ArcaneSchema
import com.sneaksanddata.arcane.framework.services.app.base.{StreamLifetimeService, StreamRunnerService}
import com.sneaksanddata.arcane.framework.services.base.{SchemaProvider, TableManager}
import com.sneaksanddata.arcane.framework.services.cdm.CdmTableSettings
import com.sneaksanddata.arcane.framework.services.storage.models.azure.AdlsStoragePath
import com.sneaksanddata.arcane.framework.services.streaming.base.StreamGraphBuilder
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.*
import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.services.storage.services.AzureBlobStorageReader
import zio.Console.printLine
import zio.{ZIO, ZLayer}

/**
 * A service that can be used to run a stream.
 *
 * @param builder The stream graph builder.
 * @param lifetimeService The stream lifetime service.
 */
private class StreamRunnerServiceCdm(builder: StreamGraphBuilder,
                                     lifetimeService: StreamLifetimeService,
                                     tableManager: TableManager,
                                     reader: AzureBlobStorageReader,
                                     streamContext: MicrosoftSynapseLinkStreamContext,
                                     rootPath: String) extends StreamRunnerService:

  /**
   * Runs the stream.
   *
   * @return A ZIO effect that represents the stream.
   */
  def run: ZIO[Any, Throwable, Unit] =
    lifetimeService.start()
    for {
      _ <- zlog("Starting the stream runner")
      _ <- tableManager.cleanupStagingTables(streamContext.stagingCatalog, streamContext.stagingTablePrefix)
      _ <- tableManager.createTargetTable
      _ <- tableManager.createBackFillTable
      _ <- builder.create.run(builder.consume)
      _ <- zlog("Stream completed")
    } yield ()

/**
 * The companion object for the StreamRunnerServiceImpl class.
 */
object StreamRunnerServiceCdm:

  type Environemnt = TableManager
    & VersionedDataGraphBuilder
    & BackfillDataGraphBuilder
    & StreamLifetimeService
    & AzureBlobStorageReader
    & CdmTableSettings
    & MicrosoftSynapseLinkStreamContext
  
  /**
   * The ZLayer for the stream runner service.
   */
  val layer: ZLayer[Environemnt, Nothing, StreamRunnerService] =
    ZLayer {
      for {
        context <- ZIO.service[MicrosoftSynapseLinkStreamContext]
        builder <- if context.IsBackfilling then ZIO.service[BackfillDataGraphBuilder] else ZIO.service[VersionedDataGraphBuilder]
        _ <- zlog(s"Using ${if context.IsBackfilling then "Backfill" else "Versioned"}DataGraphBuilder")
        lifetimeService <- ZIO.service[StreamLifetimeService]
        tableManager <- ZIO.service[TableManager]
        reader <- ZIO.service[AzureBlobStorageReader]
        tableSettings <- ZIO.service[CdmTableSettings]
      } yield new StreamRunnerServiceCdm(builder, lifetimeService, tableManager, reader, context, tableSettings.rootPath)
    }
