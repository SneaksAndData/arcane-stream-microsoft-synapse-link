package com.sneaksanddata.arcane.microsoft_synapse_link
package models.app

import com.sneaksanddata.arcane.framework.models.settings.Mergeable
import com.sneaksanddata.arcane.framework.models.settings.sources.modification.DefaultDataRowModificationSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.synapse.DefaultMicrosoftSynapseLinkConnectionSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.{DefaultSourceBufferingSettings, StreamSourceSettings}
import upickle.ReadWriter

case class MicrosoftSynapseLinkSourceSettings(
    override val buffering: DefaultSourceBufferingSettings,
    override val modifications: DefaultDataRowModificationSettings,
    override val configuration: DefaultMicrosoftSynapseLinkConnectionSettings
) extends StreamSourceSettings,
      Mergeable derives ReadWriter:
  override type SourceSettingsType = DefaultMicrosoftSynapseLinkConnectionSettings

  override type MergeableFrom = OverrideMicrosoftSynapseLinkSourceSettings
  override type MergeResult   = MicrosoftSynapseLinkSourceSettings

  override def merge(overrides: Option[MergeableFrom]): MergeResult =
    MicrosoftSynapseLinkSourceSettings(
      buffering = this.buffering.merge(overrides.flatMap(_.buffering)),
      modifications = this.modifications.merge(overrides.flatMap(_.modifications)),
      configuration
    )
