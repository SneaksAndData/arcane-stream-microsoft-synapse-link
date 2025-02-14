package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.consumers

import extensions.DataRowExtensions.schema
import models.app.streaming.{SourceCleanupRequest, SourceCleanupResult}
import models.app.{MicrosoftSynapseLinkStreamContext, TargetTableSettings}
import services.clients.{BatchArchivationResult, JdbcConsumer}
import services.data_providers.microsoft_synapse_link.{AzureBlobStorageReaderZIO, DataStreamElement, SynapseLinkBackfillBatchInFlight}
import services.streaming.consumers.IcebergSynapseConsumer.{getTableName, toStagedBatch}

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.*
import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.querygen.OverwriteQuery
import com.sneaksanddata.arcane.framework.models.{ArcaneSchema, DataRow, MergeKeyField}
import com.sneaksanddata.arcane.framework.services.base.SchemaProvider
import com.sneaksanddata.arcane.framework.services.consumers.{StagedBackfillBatch, StagedVersionedBatch, SynapseLinkMergeBatch}
import com.sneaksanddata.arcane.framework.services.lakehouse.base.IcebergCatalogSettings
import com.sneaksanddata.arcane.framework.services.lakehouse.{CatalogWriter, given_Conversion_ArcaneSchema_Schema}
import com.sneaksanddata.arcane.framework.services.streaming.base.{BatchConsumer, BatchProcessor}
import com.sneaksanddata.arcane.framework.services.streaming.consumers.{IcebergStreamingConsumer, StreamingConsumer}
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import org.apache.zookeeper.proto.DeleteRequest
import zio.stream.{ZPipeline, ZSink}
import zio.{Chunk, Schedule, Task, ZIO, ZLayer}

import java.time.format.DateTimeFormatter
import java.time.{Duration, ZoneOffset, ZonedDateTime}
import java.util.UUID


class IcebergSynapseBackfillConsumer(overwriteConsumer: JdbcConsumer[OverwriteQuery], reader: AzureBlobStorageReaderZIO)
  extends BatchConsumer[SynapseLinkBackfillBatchInFlight]:

  private val retryPolicy = Schedule.exponential(Duration.ofSeconds(1)) && Schedule.recurs(10)

  /**
   * Returns the sink that consumes the batch.
   *
   * @return ZSink (stream sink for the stream graph).
   */
  override def consume: ZSink[Any, Throwable, SynapseLinkBackfillBatchInFlight, Any, Unit] =
    ZSink.foreach(batch => consumeBackfillBatch(batch))


  private def consumeBackfillBatch(batchInFlight: SynapseLinkBackfillBatchInFlight): Task[Unit] =
    val (batch, cleanupRequests) = batchInFlight
    for
      _ <- zlog(s"Consuming backfill batch $batch")
      _ <- overwriteConsumer.applyBatch(batch)
      _ <- zlog(s"Target table has been overwritten")
      _ <- ZIO.foreach(cleanupRequests)(cleanupRequest => reader.markForDeletion(cleanupRequest.prefix))
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
  def apply(mergeConsumer: JdbcConsumer[OverwriteQuery], reader: AzureBlobStorageReaderZIO): IcebergSynapseBackfillConsumer =
    new IcebergSynapseBackfillConsumer(mergeConsumer, reader)

  /**
   * The required environment for the IcebergConsumer.
   */
  type Environment = JdbcConsumer[OverwriteQuery]
    & AzureBlobStorageReaderZIO

  /**
   * The ZLayer that creates the IcebergConsumer.
   */
  val layer: ZLayer[Environment, Nothing, IcebergSynapseBackfillConsumer] =
    ZLayer {
      for
        jdbcConsumer <- ZIO.service[JdbcConsumer[OverwriteQuery]]
        reader <- ZIO.service[AzureBlobStorageReaderZIO]
      yield IcebergSynapseBackfillConsumer(jdbcConsumer, reader)
    }
