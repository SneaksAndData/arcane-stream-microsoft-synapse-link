package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.consumers

import models.app.{MicrosoftSynapseLinkStreamContext, TargetTableSettings}
import services.clients.BatchArchivationResult
import services.streaming.consumers.IcebergSynapseConsumer.{getTableName, toStagedBatch}

import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.{ArcaneSchema, DataRow, Field, MergeKeyField}
import com.sneaksanddata.arcane.framework.services.base.SchemaProvider
import com.sneaksanddata.arcane.framework.services.consumers.{StagedVersionedBatch, SynapseLinkMergeBatch}
import com.sneaksanddata.arcane.framework.services.lakehouse.base.IcebergCatalogSettings
import com.sneaksanddata.arcane.framework.services.lakehouse.{CatalogWriter, given_Conversion_ArcaneSchema_Schema}
import com.sneaksanddata.arcane.framework.services.streaming.base.{BatchConsumer, BatchProcessor}
import com.sneaksanddata.arcane.framework.services.streaming.consumers.{IcebergStreamingConsumer, StreamingConsumer}
import com.sneaksanddata.arcane.microsoft_synapse_link.models.app.streaming.{SourceCleanupRequest, SourceCleanupResult}
import com.sneaksanddata.arcane.microsoft_synapse_link.services.data_providers.microsoft_synapse_link.DataStreamElement
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import org.apache.zookeeper.proto.DeleteRequest
import org.slf4j.{Logger, LoggerFactory}
import zio.stream.{ZPipeline, ZSink}
import zio.{Chunk, Schedule, Task, ZIO, ZLayer}

import java.time.format.DateTimeFormatter
import java.time.{Duration, ZoneOffset, ZonedDateTime}
import java.util.UUID

type InFlightBatch = ((Iterable[StagedVersionedBatch], Seq[SourceCleanupRequest]), Long)
type CompletedBatch = (Iterable[BatchArchivationResult], Seq[SourceCleanupRequest])
type PipelineResult = (Iterable[BatchArchivationResult], Seq[SourceCleanupResult])

class IcebergSynapseConsumer(streamContext: MicrosoftSynapseLinkStreamContext,
                             icebergCatalogSettings: IcebergCatalogSettings,
                             sinkSettings: TargetTableSettings,
                             catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
                             schemaProvider: SchemaProvider[ArcaneSchema],
                             mergeProcessor: BatchProcessor[InFlightBatch, InFlightBatch],
                             archivationProcessor: BatchProcessor[InFlightBatch, CompletedBatch],
                             sourceCleanupProcessor: BatchProcessor[CompletedBatch, PipelineResult])
  extends BatchConsumer[Chunk[DataStreamElement]]:

  private val logger: Logger = LoggerFactory.getLogger(classOf[IcebergStreamingConsumer])

  private val retryPolicy = Schedule.exponential(Duration.ofSeconds(1)) && Schedule.recurs(5)

  /**
   * Returns the sink that consumes the batch.
   *
   * @return ZSink (stream sink for the stream graph).
   */
  override def consume: ZSink[Any, Throwable, Chunk[DataStreamElement], Any, Unit] =
    writeStagingTable >>> mergeProcessor.process >>> archivationProcessor.process >>> sourceCleanupProcessor.process >>> logResults


  private def logResults: ZSink[Any, Throwable, PipelineResult, Any, Unit] = ZSink.foreach {
    case (arch, results) =>
      ZIO.log(s"Processing completed: ${arch}") *>
        ZIO.foreach(results)(src => ZIO.log(s"Source cleanup completed for ${src.prefix}: ${src.success}"))
  }

  private def writeStagingTable = ZPipeline[Chunk[DataStreamElement]]()
    .mapZIO(elements =>
        val groupedBySchema = elements.withFilter(e => e.isInstanceOf[DataRow]).map(e => e.asInstanceOf[DataRow]).groupBy(row => extractSchema(row))
        val deleteRequests = elements.withFilter(e => e.isInstanceOf[SourceCleanupRequest]).map(e => e.asInstanceOf[SourceCleanupRequest])
        val batchesZIO = ZIO.foreach(groupedBySchema)({ case (schema, rows) => writeDataRows(rows, schema) })
        batchesZIO.map(b => (b.values, deleteRequests))
    )
    .zipWithIndex


  private def writeDataRows(rows: Chunk[DataRow], arcaneSchema: ArcaneSchema): Task[(ArcaneSchema, StagedVersionedBatch)] =
    val tableName = getTableName(streamContext.stagingTableNamePrefix)
    for
      table <- ZIO.fromFuture(implicit ec => catalogWriter.write(rows, tableName, arcaneSchema))
      batch = table.toStagedBatch(icebergCatalogSettings.namespace, icebergCatalogSettings.warehouse, arcaneSchema, sinkSettings.targetTableFullName, Map())
    yield (arcaneSchema, batch)

  private def extractSchema(row: DataRow): ArcaneSchema =
    row.foldRight(ArcaneSchema.empty()) {
      case (cell, schema) if cell.name == MergeKeyField.name => schema ++ Seq(MergeKeyField)
      case (cell, schema) => schema ++ Seq(Field(cell.name, cell.Type))
    }

object IcebergSynapseConsumer:

  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss")

  def getTableName(streamId: String): String =
    s"${streamId}_${ZonedDateTime.now(ZoneOffset.UTC).format(formatter)}_${UUID.randomUUID().toString}".replace('-', '_')

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
            mergeProcessor: BatchProcessor[InFlightBatch, InFlightBatch],
            archivationProcessor: BatchProcessor[InFlightBatch, CompletedBatch],
            sourceCleanupProcessor: BatchProcessor[CompletedBatch, PipelineResult]): IcebergSynapseConsumer =
    new IcebergSynapseConsumer(streamContext, icebergCatalogSettings, sinkSettings, catalogWriter, schemaProvider, mergeProcessor, archivationProcessor, sourceCleanupProcessor)

  /**
   * The required environment for the IcebergConsumer.
   */
  type Environment = SchemaProvider[ArcaneSchema]
    & CatalogWriter[RESTCatalog, Table, Schema]
    & BatchProcessor[InFlightBatch, InFlightBatch]
    & MicrosoftSynapseLinkStreamContext
    & TargetTableSettings
    & BatchProcessor[InFlightBatch, CompletedBatch]
    & IcebergCatalogSettings
    & BatchProcessor[CompletedBatch, PipelineResult]

  /**
   * The ZLayer that creates the IcebergConsumer.
   */
  val layer: ZLayer[Environment, Nothing, IcebergSynapseConsumer] =
    ZLayer {
      for
        streamContext <- ZIO.service[MicrosoftSynapseLinkStreamContext]
        icebergCatalogSettings <- ZIO.service[IcebergCatalogSettings]
        sinkSettings <- ZIO.service[TargetTableSettings]
        catalogWriter <- ZIO.service[CatalogWriter[RESTCatalog, Table, Schema]]
        schemaProvider <- ZIO.service[SchemaProvider[ArcaneSchema]]
        mergeProcessor <- ZIO.service[BatchProcessor[InFlightBatch, InFlightBatch]]
        archivationProcessor <- ZIO.service[BatchProcessor[InFlightBatch, CompletedBatch]]
        sourceCleanupProcessor <- ZIO.service[BatchProcessor[CompletedBatch, PipelineResult]]
      yield IcebergSynapseConsumer(streamContext, icebergCatalogSettings, sinkSettings, catalogWriter, schemaProvider, mergeProcessor, archivationProcessor, sourceCleanupProcessor)
    }
