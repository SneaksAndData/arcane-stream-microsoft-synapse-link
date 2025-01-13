package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.consumers

import com.sneaksanddata.arcane.framework.models.{ArcaneSchema, DataRow}
import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings.SinkSettings
import com.sneaksanddata.arcane.framework.services.base.SchemaProvider
import com.sneaksanddata.arcane.framework.services.consumers.{BatchApplicationResult, StagedVersionedBatch, SynapseLinkMergeBatch}
import com.sneaksanddata.arcane.framework.services.lakehouse.CatalogWriter
import com.sneaksanddata.arcane.framework.services.streaming.base.BatchProcessor
import com.sneaksanddata.arcane.framework.services.streaming.consumers.{IcebergStreamingConsumer, StreamingConsumer}

import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import org.slf4j.{Logger, LoggerFactory}
import zio.stream.{ZPipeline, ZSink}
import zio.{Chunk, Task, ZIO, ZLayer}

import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}
import IcebergSynapseConsumer.toStagedBatch
import IcebergSynapseConsumer.getTableName
import com.sneaksanddata.arcane.framework.services.lakehouse.given_Conversion_ArcaneSchema_Schema

class IcebergSynapseConsumer(streamContext: StreamContext,
                               sinkSettings: SinkSettings,
                               catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
                               schemaProvider: SchemaProvider[ArcaneSchema],
                               mergeProcessor: BatchProcessor[StagedVersionedBatch, StagedVersionedBatch],
                               archivationProcessor: BatchProcessor[StagedVersionedBatch, BatchArchivationResult])
  extends StreamingConsumer:

  private val logger: Logger = LoggerFactory.getLogger(classOf[IcebergStreamingConsumer])

  /**
   * Returns the sink that consumes the batch.
   *
   * @return ZSink (stream sink for the stream graph).
   */
  override def consume: ZSink[Any, Throwable, Chunk[DataRow], Any, Unit] =
    writeStagingTable >>> mergeProcessor.process >>> archivationProcessor.process >>> logResults


  private def logResults: ZSink[Any, Throwable, BatchArchivationResult, Nothing, Unit] = ZSink.foreach { e =>
    logger.info(s"Received the batch $e from the streaming source")
    ZIO.unit
  }

  private def writeStagingTable = ZPipeline[Chunk[DataRow]]()
    .mapAccum(0L) { (acc, chunk) => (acc + 1, (chunk, acc.getTableName(streamContext.streamId))) }
    .mapZIO({
      case (rows, tableName) => writeWithWriter(rows, tableName)
    })


  private def writeWithWriter(rows: Chunk[DataRow], name: String): Task[StagedVersionedBatch] =
    for
      arcaneSchema <- ZIO.fromFuture(implicit ec => schemaProvider.getSchema)
      table <- ZIO.fromFuture(implicit ec => catalogWriter.write(rows, name, arcaneSchema))
    yield table.toStagedBatch(arcaneSchema, sinkSettings.sinkLocation, Map())


object IcebergSynapseConsumer:

  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss")

  extension (batchNumber: Long) def getTableName(streamId: String): String =
    s"${streamId}_${ZonedDateTime.now(ZoneOffset.UTC).format(formatter)}_$batchNumber"

  extension (table: Table) def toStagedBatch(batchSchema: ArcaneSchema,
                                             targetName: String,
                                             partitionValues: Map[String, List[String]]): StagedVersionedBatch =
    val batchName = table.name().split('.').last
    SynapseLinkMergeBatch(batchName, batchSchema, targetName, partitionValues)


  /**
   * Factory method to create IcebergConsumer
   *
   * @param streamContext  The stream context.
   * @param sinkSettings   The stream sink settings.
   * @param catalogWriter  The catalog writer.
   * @param schemaProvider The schema provider.
   * @return The initialized IcebergConsumer instance
   */
  def apply(streamContext: StreamContext,
            sinkSettings: SinkSettings,
            catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
            schemaProvider: SchemaProvider[ArcaneSchema],
            mergeProcessor: BatchProcessor[StagedVersionedBatch, StagedVersionedBatch],
            archivationProcessor: BatchProcessor[StagedVersionedBatch, BatchArchivationResult]): IcebergSynapseConsumer =
    new IcebergSynapseConsumer(streamContext, sinkSettings, catalogWriter, schemaProvider, mergeProcessor, archivationProcessor)

  /**
   * The required environment for the IcebergConsumer.
   */
  type Environment = SchemaProvider[ArcaneSchema]
    & CatalogWriter[RESTCatalog, Table, Schema]
    & BatchProcessor[StagedVersionedBatch, StagedVersionedBatch]
    & StreamContext
    & SinkSettings
    & BatchProcessor[StagedVersionedBatch, BatchArchivationResult]

  /**
   * The ZLayer that creates the IcebergConsumer.
   */
  val layer: ZLayer[Environment, Nothing, StreamingConsumer] =
    ZLayer {
      for
        streamContext <- ZIO.service[StreamContext]
        sinkSettings <- ZIO.service[SinkSettings]
        catalogWriter <- ZIO.service[CatalogWriter[RESTCatalog, Table, Schema]]
        schemaProvider <- ZIO.service[SchemaProvider[ArcaneSchema]]
        mergeProcessor <- ZIO.service[BatchProcessor[StagedVersionedBatch, StagedVersionedBatch]]
        archivationProcessor <- ZIO.service[BatchProcessor[StagedVersionedBatch, BatchArchivationResult]]
      yield consumers.IcebergSynapseConsumer(streamContext, sinkSettings, catalogWriter, schemaProvider, mergeProcessor, archivationProcessor)
    }
