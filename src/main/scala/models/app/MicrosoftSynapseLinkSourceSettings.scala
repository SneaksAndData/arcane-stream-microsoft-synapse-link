package com.sneaksanddata.arcane.microsoft_synapse_link
package models.app

import com.sneaksanddata.arcane.framework.models.settings.DefaultFieldSelectionRuleSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.synapse.DefaultMicrosoftSynapseLinkConnectionSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.{DefaultSourceBufferingSettings, StreamSourceSettings}
import upickle.ReadWriter

case class MicrosoftSynapseLinkSourceSettings(
    override val buffering: DefaultSourceBufferingSettings,
    override val fieldSelectionRule: DefaultFieldSelectionRuleSettings,
    override val configuration: DefaultMicrosoftSynapseLinkConnectionSettings
) extends StreamSourceSettings derives ReadWriter:
  override type SourceSettingsType = DefaultMicrosoftSynapseLinkConnectionSettings
