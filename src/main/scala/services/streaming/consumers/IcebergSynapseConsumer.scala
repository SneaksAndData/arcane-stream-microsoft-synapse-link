package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.consumers

import models.app.{MicrosoftSynapseLinkStreamContext, TargetTableSettings}
import services.clients.BatchArchivationResult
import services.streaming.consumers.IcebergSynapseConsumer.{getTableName, toStagedBatch}

import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.{ArcaneSchema, DataRow}
import com.sneaksanddata.arcane.framework.services.base.SchemaProvider
import com.sneaksanddata.arcane.framework.services.consumers.{StagedVersionedBatch, SynapseLinkMergeBatch}
import com.sneaksanddata.arcane.framework.services.lakehouse.base.IcebergCatalogSettings
import com.sneaksanddata.arcane.framework.services.lakehouse.{CatalogWriter, given_Conversion_ArcaneSchema_Schema}
import com.sneaksanddata.arcane.framework.services.streaming.base.BatchProcessor
import com.sneaksanddata.arcane.framework.services.streaming.consumers.{IcebergStreamingConsumer, StreamingConsumer}
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import org.slf4j.{Logger, LoggerFactory}
import zio.stream.{ZPipeline, ZSink}
import zio.{Chunk, Task, ZIO, ZLayer}

import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}

class IcebergSynapseConsumer(streamContext: MicrosoftSynapseLinkStreamContext,
                             icebergCatalogSettings: IcebergCatalogSettings,
                             sinkSettings: TargetTableSettings,
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


  private def logResults: ZSink[Any, Throwable, BatchArchivationResult, Any, Unit] = ZSink.foreach { e =>
    for _ <- ZIO.log(s"Received the batch $e from the streaming source")
    yield ()
  }

  private def writeStagingTable = ZPipeline[Chunk[DataRow]]()
    .mapAccum(0L) { (acc, chunk) => (acc + 1, (chunk, acc.getTableName(streamContext.stagingTableNamePrefix))) }
    .mapZIO({
      case (rows, tableName) => writeWithWriter(rows, tableName)
    })


  private def writeWithWriter(rows: Chunk[DataRow], name: String): Task[StagedVersionedBatch] =
    for
      arcaneSchema <- ZIO.fromFuture(implicit ec => schemaProvider.getSchema)
      table <- ZIO.fromFuture(implicit ec => catalogWriter.write(rows, name, arcaneSchema))
    yield table.toStagedBatch(
      icebergCatalogSettings.namespace,
      icebergCatalogSettings.warehouse,
      arcaneSchema,
      sinkSettings.targetTableFullName,
      Map())


object IcebergSynapseConsumer:

  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss")

  extension (batchNumber: Long) def getTableName(streamId: String): String =
    s"${streamId.replace('-', '_')}_${ZonedDateTime.now(ZoneOffset.UTC).format(formatter)}_$batchNumber"

  extension (table: Table) def toStagedBatch(namespace: String,
                                             warehouse: String,
                                             batchSchema: ArcaneSchema,
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
  def apply(streamContext: MicrosoftSynapseLinkStreamContext,
            icebergCatalogSettings: IcebergCatalogSettings,
            sinkSettings: TargetTableSettings,
            catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
            schemaProvider: SchemaProvider[ArcaneSchema],
            mergeProcessor: BatchProcessor[StagedVersionedBatch, StagedVersionedBatch],
            archivationProcessor: BatchProcessor[StagedVersionedBatch, BatchArchivationResult]): IcebergSynapseConsumer =
    new IcebergSynapseConsumer(streamContext, icebergCatalogSettings, sinkSettings, catalogWriter, schemaProvider, mergeProcessor, archivationProcessor)

  /**
   * The required environment for the IcebergConsumer.
   */
  type Environment = SchemaProvider[ArcaneSchema]
    & CatalogWriter[RESTCatalog, Table, Schema]
    & BatchProcessor[StagedVersionedBatch, StagedVersionedBatch]
    & MicrosoftSynapseLinkStreamContext
    & TargetTableSettings
    & BatchProcessor[StagedVersionedBatch, BatchArchivationResult]
    & IcebergCatalogSettings

  /**
   * The ZLayer that creates the IcebergConsumer.
   */
  val layer: ZLayer[Environment, Nothing, StreamingConsumer] =
    ZLayer {
      for
        streamContext <- ZIO.service[MicrosoftSynapseLinkStreamContext]
        icebergCatalogSettings <- ZIO.service[IcebergCatalogSettings]
        sinkSettings <- ZIO.service[TargetTableSettings]
        catalogWriter <- ZIO.service[CatalogWriter[RESTCatalog, Table, Schema]]
        schemaProvider <- ZIO.service[SchemaProvider[ArcaneSchema]]
        mergeProcessor <- ZIO.service[BatchProcessor[StagedVersionedBatch, StagedVersionedBatch]]
        archivationProcessor <- ZIO.service[BatchProcessor[StagedVersionedBatch, BatchArchivationResult]]
      yield IcebergSynapseConsumer(streamContext, icebergCatalogSettings, sinkSettings, catalogWriter, schemaProvider, mergeProcessor, archivationProcessor)
    }
