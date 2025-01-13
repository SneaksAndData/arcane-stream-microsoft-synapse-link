package com.sneaksanddata.arcane.microsoft_synapse_link
package services.data_providers.microsoft_synapse_link

import com.sneaksanddata.arcane.framework.services.app.base.{StreamLifetimeService, StreamRunnerService}
import com.sneaksanddata.arcane.framework.services.streaming.base.StreamGraphBuilder

import zio.{ZIO, ZLayer}

/**
 * A service that can be used to run a stream.
 *
 * @param builder The stream graph builder.
 * @param lifetimeService The stream lifetime service.
 */
private class StreamRunnerServiceCdm(builder: StreamGraphBuilder, lifetimeService: StreamLifetimeService) extends StreamRunnerService:

  /**
   * Runs the stream.
   *
   * @return A ZIO effect that represents the stream.
   */
  def run: ZIO[Any, Throwable, Unit] = 
    lifetimeService.start()
    builder.create.run(builder.consume)

/**
 * The companion object for the StreamRunnerServiceImpl class.
 */
object StreamRunnerServiceImpl:

  /**
   * The ZLayer for the stream runner service.
   */
  val layer: ZLayer[StreamGraphBuilder & StreamLifetimeService, Nothing, StreamRunnerService] =
    ZLayer {
      for {
        builder <- ZIO.service[StreamGraphBuilder]
        lifetimeService <- ZIO.service[StreamLifetimeService]
      } yield new StreamRunnerServiceCdm(builder, lifetimeService)
    }
