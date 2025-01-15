package com.sneaksanddata.arcane.microsoft_synapse_link
package services.app

import models.app.{ArchiveTableSettings, TargetTableSettings}

import com.sneaksanddata.arcane.framework.models.ArcaneSchema
import com.sneaksanddata.arcane.framework.services.app.base.{StreamLifetimeService, StreamRunnerService}
import com.sneaksanddata.arcane.framework.services.base.SchemaProvider
import com.sneaksanddata.arcane.framework.services.consumers.JdbcConsumerOptions
import com.sneaksanddata.arcane.framework.services.streaming.base.StreamGraphBuilder
import zio.{ZIO, ZLayer}

/**
 * A service that can be used to run a stream.
 *
 * @param builder The stream graph builder.
 * @param lifetimeService The stream lifetime service.
 */
private class StreamRunnerServiceCdm(builder: StreamGraphBuilder,
                                     lifetimeService: StreamLifetimeService,
                                     tableManager: TableManager) extends StreamRunnerService:

  /**
   * Runs the stream.
   *
   * @return A ZIO effect that represents the stream.
   */
  def run: ZIO[Any, Throwable, Unit] =
    lifetimeService.start()
    for {
      _ <- ZIO.log("Starting the stream runner")
      _ <- ZIO.fromFuture(implicit ec => tableManager.createTargetTable)
      _ <- ZIO.fromFuture(implicit ec => tableManager.createArchiveTable)
      _ <- builder.create.run(builder.consume)
    } yield ()

/**
 * The companion object for the StreamRunnerServiceImpl class.
 */
object StreamRunnerServiceCdm:

  type Environemnt = TableManager
    & StreamGraphBuilder
    & StreamLifetimeService
  
  /**
   * The ZLayer for the stream runner service.
   */
  val layer: ZLayer[Environemnt, Nothing, StreamRunnerService] =
    ZLayer {
      for {
        builder <- ZIO.service[StreamGraphBuilder]
        lifetimeService <- ZIO.service[StreamLifetimeService]
        tableManager <- ZIO.service[TableManager]
      } yield new StreamRunnerServiceCdm(builder, lifetimeService, tableManager)
    }
