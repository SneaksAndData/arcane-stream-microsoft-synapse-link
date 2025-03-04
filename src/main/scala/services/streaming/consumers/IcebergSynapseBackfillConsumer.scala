package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.consumers

import services.data_providers.microsoft_synapse_link.BackfillBatchInFlight

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.*
import com.sneaksanddata.arcane.framework.models.settings.TargetTableSettings
import com.sneaksanddata.arcane.framework.services.base.{DisposeServiceClient, MergeServiceClient, TableManager}
import com.sneaksanddata.arcane.framework.services.storage.services.AzureBlobStorageReader
import com.sneaksanddata.arcane.framework.services.streaming.base.BatchConsumer
import zio.stream.ZSink
import zio.{Schedule, Task, ZIO, ZLayer}

import java.time.Duration


class IcebergSynapseBackfillConsumer(mergeServiceClient: MergeServiceClient,
                                     disposeServiceClient: DisposeServiceClient,
                                     reader: AzureBlobStorageReader,
                                     targetTableSettings: TargetTableSettings,
                                     tableManager: TableManager)
  extends BatchConsumer[BackfillBatchInFlight]:

  private val retryPolicy = Schedule.exponential(Duration.ofSeconds(1)) && Schedule.recurs(10)

  /**
   * Returns the sink that consumes the batch.
   *
   * @return ZSink (stream sink for the stream graph).
   */
  override def consume: ZSink[Any, Throwable, BackfillBatchInFlight, Any, Unit] =
    ZSink.foreach(batch => consumeBackfillBatch(batch))


  private def consumeBackfillBatch(batch: BackfillBatchInFlight): Task[Unit] =
    for
      _ <- zlog(s"Consuming backfill batch $batch")
      _ <- tableManager.migrateSchema(batch.schema, targetTableSettings.targetTableFullName)
      _ <- mergeServiceClient.applyBatch(batch)
      _ <- disposeServiceClient.disposeBatch(batch)
      _ <- zlog(s"Target table has been overwritten")
    yield ()



object IcebergSynapseBackfillConsumer:

  /**
   * Factory method to create IcebergConsumer
   *
   * @param streamContext  The stream context.
   * @param sinkSettings   The stream sink settings.
   * @param catalogWriter  The catalog writer.
   * @param schemaProvider The schema provider.
   * @return The initialized IcebergConsumer instance
   */
  def apply(mergeServiceClient: MergeServiceClient,
            disposeServiceClient: DisposeServiceClient,
            reader: AzureBlobStorageReader,
            targetTableSettings: TargetTableSettings,
            tableManager: TableManager): IcebergSynapseBackfillConsumer =
    new IcebergSynapseBackfillConsumer(mergeServiceClient, disposeServiceClient, reader, targetTableSettings, tableManager)

  /**
   * The required environment for the IcebergConsumer.
   */
  type Environment = MergeServiceClient
    & DisposeServiceClient
    & AzureBlobStorageReader
    & TargetTableSettings
    & TableManager

  /**
   * The ZLayer that creates the IcebergConsumer.
   */
  val layer: ZLayer[Environment, Nothing, IcebergSynapseBackfillConsumer] =
    ZLayer {
      for
        mergeServiceClient <- ZIO.service[MergeServiceClient]
        disposeServiceClient <- ZIO.service[DisposeServiceClient]
        reader <- ZIO.service[AzureBlobStorageReader]
        settings <- ZIO.service[TargetTableSettings]
        tableManager <-  ZIO.service[TableManager]
      yield IcebergSynapseBackfillConsumer(mergeServiceClient, disposeServiceClient, reader, settings, tableManager)
    }
