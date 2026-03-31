package com.sneaksanddata.arcane.microsoft_synapse_link
package models.app

import com.sneaksanddata.arcane.framework.models.app.{DefaultPluginStreamContext, PluginStreamContext}
import com.sneaksanddata.arcane.framework.models.settings.observability.DefaultObservabilitySettings
import com.sneaksanddata.arcane.framework.models.settings.sink.DefaultSinkSettings
import com.sneaksanddata.arcane.framework.models.settings.staging.DefaultStagingSettings
import com.sneaksanddata.arcane.framework.models.settings.streaming.{
  DefaultStreamModeSettings,
  DefaultThroughputSettings
}
import upickle.ReadWriter
import upickle.implicits.key
import zio.ZLayer
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig

/** The specification for the stream.
  */
case class MicrosoftSynapseLinkPluginStreamContext(
    @key("observability") private val observabilityIn: DefaultObservabilitySettings,
    @key("staging") private val stagingIn: DefaultStagingSettings,
    @key("streamMode") private val streamModeIn: DefaultStreamModeSettings,
    @key("sink") private val sinkIn: DefaultSinkSettings,
    @key("throughput") private val throughputIn: DefaultThroughputSettings,
    override val source: MicrosoftSynapseLinkSourceSettings
) extends DefaultPluginStreamContext(observabilityIn, stagingIn, streamModeIn, sinkIn, throughputIn) derives ReadWriter:
  // TODO: should be implemented when Operator supports overrides
  override def merge(other: Option[PluginStreamContext]): PluginStreamContext = this

object MicrosoftSynapseLinkPluginStreamContext:
  def apply(value: String): MicrosoftSynapseLinkPluginStreamContext =
    PluginStreamContext[MicrosoftSynapseLinkPluginStreamContext](value)

  lazy val layer
      : ZLayer[Any, Throwable, PluginStreamContext & DatagramSocketConfig & MetricsConfig & DatadogPublisherConfig] =
    PluginStreamContext.getLayer[MicrosoftSynapseLinkPluginStreamContext]
