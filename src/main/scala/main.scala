package com.sneaksanddata.arcane.microsoft_synapse_link

import models.app.MicrosoftSynapseLinkPluginStreamContext

import com.sneaksanddata.arcane.framework.extensions.ZExtensions.*
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.models.app.PluginStreamContext
import com.sneaksanddata.arcane.framework.plugins.LayerAssemblies
import com.sneaksanddata.arcane.framework.plugins.synapse.Services
import com.sneaksanddata.arcane.framework.services.app.base.StreamRunnerService
import com.sneaksanddata.arcane.framework.services.app.{GenericStreamRunnerService, StreamGraphResolver}
import com.sneaksanddata.arcane.framework.services.streaming.base.StreamingGraphBuilder
import com.sneaksanddata.arcane.framework.services.synapse.base.SynapseLinkStreamingSource
import zio.*
import zio.logging.backend.SLF4J

object main extends ZIOAppDefault:

  override val bootstrap: ZLayer[Any, Nothing, Unit] = Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  val appLayer: ZIO[StreamRunnerService, Throwable, Unit] = for
    _            <- zlog("Application starting")
    streamRunner <- ZIO.service[StreamRunnerService]
    _            <- streamRunner.run
  yield ()

  val synapseLinkReaderLayer: ZLayer[PluginStreamContext, Throwable, SynapseLinkStreamingSource] =
    SynapseLinkStreamingSource.getLayer(context =>
      context.asInstanceOf[MicrosoftSynapseLinkPluginStreamContext].source.configuration
    )

  private lazy val streamRunner = appLayer.provide(
    Services.synapseLinkSourceLayer,
    LayerAssemblies.frameworkPipelineServicesLayer,
    LayerAssemblies.frameworkStagingServicesLayer,
    MicrosoftSynapseLinkPluginStreamContext.layer,
    synapseLinkReaderLayer,
    GenericStreamRunnerService.layer,
    StreamGraphResolver.composedLayer
  )

  @main
  def run: ZIO[Any, Throwable, Unit] = streamRunner.handleAppFailure(exit)
