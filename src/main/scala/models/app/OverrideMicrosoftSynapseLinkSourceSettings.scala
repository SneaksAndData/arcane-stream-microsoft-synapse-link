package com.sneaksanddata.arcane.microsoft_synapse_link
package models.app

import com.sneaksanddata.arcane.framework.models.settings.sources.synapse.DefaultMicrosoftSynapseLinkConnectionSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.modification.DefaultOverrideDataRowModificationSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.{
  DefaultSourceBufferingSettings,
  OverrideStreamSourceSettings
}
import upickle.ReadWriter

/** A partial override of `MicrosoftSynapseLinkSourceSettings` provided by the Arcane operator via the environment
  * variable. Not applied yet, see the TODO note on `MicrosoftSynapseLinkPluginStreamContext.merge`.
  */
case class OverrideMicrosoftSynapseLinkSourceSettings(
    override val configuration: Option[DefaultMicrosoftSynapseLinkConnectionSettings] = None,
    override val buffering: Option[DefaultSourceBufferingSettings] = None,
    override val modifications: Option[DefaultOverrideDataRowModificationSettings] = None
) extends OverrideStreamSourceSettings derives ReadWriter:
  override type SourceSettingsOverrideType = DefaultMicrosoftSynapseLinkConnectionSettings
