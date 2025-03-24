package com.sneaksanddata.arcane.microsoft_synapse_link
package services.data_providers.microsoft_synapse_link.base

import models.app.MicrosoftSynapseLinkStreamContext
import services.data_providers.microsoft_synapse_link.{MicrosoftSynapseLinkBackfillMergeDataProvider, MicrosoftSynapseLinkBackfillOverwriteDataProvider}

import com.sneaksanddata.arcane.framework.models.settings.{BackfillBehavior, BackfillSettings}
import com.sneaksanddata.arcane.framework.services.consumers.StagedBackfillOverwriteBatch
import zio.{Task, ZIO, ZLayer}

type BackfillBatchInFlight = StagedBackfillOverwriteBatch | Unit

/**
 * Provides the backfill batch if the stream is running in the backfill mode.
 */
trait MicrosoftSynapseLinkBackfillDataProvider:

  def requestBackfill: Task[BackfillBatchInFlight]
  
  
object MicrosoftSynapseLinkBackfillDataProvider:
  
  /**
   * The environment required for the MicrosoftSynapseLinkBackfillDataProvider.
   */
  type Environemnt = MicrosoftSynapseLinkStreamContext
    & MicrosoftSynapseLinkBackfillMergeDataProvider
    & MicrosoftSynapseLinkBackfillOverwriteDataProvider

  /**
   * The ZLayer for the stream runner service.
   */
  val layer: ZLayer[Environemnt, Nothing, MicrosoftSynapseLinkBackfillDataProvider] =
    ZLayer {
      for {
        settings <- ZIO.service[BackfillSettings]
        backfillDataProvider <- settings.backfillBehavior match
          case BackfillBehavior.Merge => ZIO.service[MicrosoftSynapseLinkBackfillMergeDataProvider]
          case BackfillBehavior.Overwrite => ZIO.service[MicrosoftSynapseLinkBackfillOverwriteDataProvider]
      } yield backfillDataProvider
    }


  type CompositeEnvironment = MicrosoftSynapseLinkStreamContext
    & MicrosoftSynapseLinkBackfillMergeDataProvider.Environment
    & MicrosoftSynapseLinkBackfillOverwriteDataProvider.Environment

  val compositeLayer: ZLayer[CompositeEnvironment, Nothing, MicrosoftSynapseLinkBackfillDataProvider] =
    MicrosoftSynapseLinkBackfillOverwriteDataProvider.layer >+> MicrosoftSynapseLinkBackfillMergeDataProvider.layer >>> layer
