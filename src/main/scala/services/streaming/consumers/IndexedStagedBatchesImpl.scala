package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.consumers

import com.sneaksanddata.arcane.framework.models.settings.{OptimizeSettings, OrphanFilesExpirationSettings, SnapshotExpirationSettings}
import com.sneaksanddata.arcane.framework.services.consumers.{ArchiveableBatch, MergeableBatch, StagedVersionedBatch}
import com.sneaksanddata.arcane.framework.services.merging.models.{JdbcOptimizationRequest, JdbcOrphanFilesExpirationRequest, JdbcSnapshotExpirationRequest}
import com.sneaksanddata.arcane.framework.services.streaming.base.{OptimizationRequestConvertable, OrphanFilesExpirationRequestConvertable, SnapshotExpirationRequestConvertable}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.IndexedStagedBatches

class IndexedStagedBatchesImpl(override val groupedBySchema: Iterable[StagedVersionedBatch & MergeableBatch & ArchiveableBatch],
                               override val batchIndex: Long)
  extends IndexedStagedBatches(groupedBySchema, batchIndex)
  with SnapshotExpirationRequestConvertable
  with OrphanFilesExpirationRequestConvertable
  with OptimizationRequestConvertable:

  override def getSnapshotExpirationRequest(settings: SnapshotExpirationSettings): JdbcSnapshotExpirationRequest = ???

  override def getOrphanFileExpirationRequest(settings: OrphanFilesExpirationSettings): JdbcOrphanFilesExpirationRequest = ???

  override def getOptimizationRequest(settings: OptimizeSettings): JdbcOptimizationRequest = ???
