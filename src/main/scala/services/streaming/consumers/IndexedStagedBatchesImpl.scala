package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.consumers

import com.sneaksanddata.arcane.framework.models.settings.{OptimizeSettings, OrphanFilesExpirationSettings, SnapshotExpirationSettings}
import com.sneaksanddata.arcane.framework.services.consumers.{MergeableBatch, StagedVersionedBatch}
import com.sneaksanddata.arcane.framework.services.merging.models.{JdbcOptimizationRequest, JdbcOrphanFilesExpirationRequest, JdbcSnapshotExpirationRequest}
import com.sneaksanddata.arcane.framework.services.streaming.base.{OptimizationRequestConvertable, OrphanFilesExpirationRequestConvertable, SnapshotExpirationRequestConvertable}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.IndexedStagedBatches

class IndexedStagedBatchesImpl(override val groupedBySchema: Iterable[StagedVersionedBatch & MergeableBatch],
                               override val batchIndex: Long)
  extends IndexedStagedBatches(groupedBySchema, batchIndex)
  with SnapshotExpirationRequestConvertable
  with OrphanFilesExpirationRequestConvertable
  with OptimizationRequestConvertable:

  override def getSnapshotExpirationRequest(settings: SnapshotExpirationSettings): JdbcSnapshotExpirationRequest =
    JdbcSnapshotExpirationRequest(groupedBySchema.head.targetTableName,
      settings.batchThreshold,
      settings.retentionThreshold,
      batchIndex)

  override def getOrphanFileExpirationRequest(settings: OrphanFilesExpirationSettings): JdbcOrphanFilesExpirationRequest =
    JdbcOrphanFilesExpirationRequest(groupedBySchema.head.targetTableName,
      settings.batchThreshold,
      settings.retentionThreshold,
      batchIndex)

  override def getOptimizationRequest(settings: OptimizeSettings): JdbcOptimizationRequest =
    JdbcOptimizationRequest(groupedBySchema.head.targetTableName,
      settings.batchThreshold,
      settings.fileSizeThreshold,
      batchIndex)
