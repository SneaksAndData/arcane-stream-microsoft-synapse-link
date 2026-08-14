package com.sneaksanddata.arcane.microsoft_synapse_link
package common

import main.{appLayer, synapseLinkReaderLayer}
import models.app.MicrosoftSynapseLinkPluginStreamContext

import com.sneaksanddata.arcane.framework.plugins.LayerAssemblies
import com.sneaksanddata.arcane.framework.plugins.synapse.Services
import com.sneaksanddata.arcane.framework.services.app.{GenericStreamRunnerService, StreamGraphResolver}
import com.sneaksanddata.arcane.framework.testkit.appbuilder.TestAppBuilder.buildTestApp
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig
import zio.{ZIO, ZLayer}

import java.time.Duration

/** Common utilities for tests.
  */
object Common:

  /** Builds the test application from the provided layers.
    * @param streamContextLayer
    *   The stream context layer.
    * @return
    *   The test application.
    */
  def getTestApp(
      runTimeout: Duration,
      streamContextLayer: ZLayer[
        Any,
        Nothing,
        MicrosoftSynapseLinkPluginStreamContext & DatagramSocketConfig & MetricsConfig & DatadogPublisherConfig
      ]
  ): ZIO[Any, Throwable, Unit] =
    buildTestApp(
      appLayer,
      streamContextLayer
    )(
      Services.synapseLinkSourceLayer,
      LayerAssemblies.frameworkPipelineServicesLayer,
      LayerAssemblies.frameworkStagingServicesLayer,

      // had to move these as they are Throwable instead of Nothing.
      synapseLinkReaderLayer,
      GenericStreamRunnerService.layer,
      StreamGraphResolver.composedLayer
    )
