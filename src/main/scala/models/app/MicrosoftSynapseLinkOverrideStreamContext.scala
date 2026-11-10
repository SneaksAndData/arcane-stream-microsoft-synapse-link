package com.sneaksanddata.arcane.microsoft_synapse_link
package models.app

import com.sneaksanddata.arcane.framework.models.app.DefaultOverrideStreamContext
import com.sneaksanddata.arcane.framework.models.settings.observability.DefaultOverrideObservabilitySettings
import com.sneaksanddata.arcane.framework.models.settings.sink.DefaultOverrideSinkSettings
import com.sneaksanddata.arcane.framework.models.settings.staging.DefaultOverrideStagingSettings
import com.sneaksanddata.arcane.framework.models.settings.streaming.{
  DefaultOverrideStreamModeSettings,
  DefaultOverrideThroughputSettings
}
import upickle.ReadWriter
import upickle.implicits.key

/** The partial stream override provided by the Arcane operator via the environment variable.
  */
case class MicrosoftSynapseLinkOverrideStreamContext(
    @key("observability") private val observabilityIn: Option[DefaultOverrideObservabilitySettings] = None,
    @key("staging") private val stagingIn: Option[DefaultOverrideStagingSettings] = None,
    @key("streamMode") private val streamModeIn: Option[DefaultOverrideStreamModeSettings] = None,
    @key("sink") private val sinkIn: Option[DefaultOverrideSinkSettings] = None,
    @key("throughput") private val throughputIn: Option[DefaultOverrideThroughputSettings] = None,
    override val source: Option[OverrideMicrosoftSynapseLinkSourceSettings] = None
) extends DefaultOverrideStreamContext(streamModeIn, sinkIn, stagingIn, observabilityIn, throughputIn)
    derives ReadWriter
