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
import com.sneaksanddata.arcane.framework.services.streaming.base.{BatchConsumer, BatchProcessor}
import com.sneaksanddata.arcane.framework.services.streaming.consumers.{IcebergStreamingConsumer, StreamingConsumer}
import com.sneaksanddata.arcane.microsoft_synapse_link.models.app.streaming.{SourceCleanupRequest, SourceCleanupResult}
import com.sneaksanddata.arcane.microsoft_synapse_link.services.data_providers.microsoft_synapse_link.DataStreamElement
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import org.apache.zookeeper.proto.DeleteRequest
import org.slf4j.{Logger, LoggerFactory}
import zio.stream.{ZPipeline, ZSink}
import zio.{Chunk, Task, ZIO, ZLayer}

import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}

type InFlightBatch = ((StagedVersionedBatch, Seq[SourceCleanupRequest]), Long)
type CompletedBatch = (BatchArchivationResult, Seq[SourceCleanupRequest])
type PiplineResult = (BatchArchivationResult, Seq[SourceCleanupResult])

class IcebergSynapseConsumer(streamContext: MicrosoftSynapseLinkStreamContext,
                             icebergCatalogSettings: IcebergCatalogSettings,
                             sinkSettings: TargetTableSettings,
                             catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
                             schemaProvider: SchemaProvider[ArcaneSchema],
                             mergeProcessor: BatchProcessor[InFlightBatch, InFlightBatch],
                             archivationProcessor: BatchProcessor[InFlightBatch, CompletedBatch],
                             sourceCleanupProcessor: BatchProcessor[CompletedBatch, PiplineResult])
  extends BatchConsumer[Chunk[DataStreamElement]]:

  private val logger: Logger = LoggerFactory.getLogger(classOf[IcebergStreamingConsumer])

  /**
   * Returns the sink that consumes the batch.
   *
   * @return ZSink (stream sink for the stream graph).
   */
  override def consume: ZSink[Any, Throwable, Chunk[DataStreamElement], Any, Unit] =
    writeStagingTable >>> mergeProcessor.process >>> archivationProcessor.process >>> sourceCleanupProcessor.process >>> logResults


  private def logResults: ZSink[Any, Throwable, PiplineResult, Any, Unit] = ZSink.foreach {
    case (arch, results) =>
      ZIO.log(s"Processing completed: ${arch}") *>
        ZIO.foreach(results)(src => ZIO.log(s"Source cleanup completed for ${src.prefix}: ${src.success}"))
  }

  private def writeStagingTable = ZPipeline[Chunk[DataStreamElement]]()
    .mapAccum(0L) { (acc, chunk) => (acc + 1, (chunk, acc.getTableName(streamContext.stagingTableNamePrefix))) }
    .mapZIO({
      case (elements, tableName) => writeDataRows(elements, tableName)
    })
    .zipWithIndex


  private def writeDataRows(elements: Chunk[DataStreamElement], name: String): Task[(StagedVersionedBatch, Seq[SourceCleanupRequest])] =
    for
      arcaneSchema <- ZIO.fromFuture(implicit ec => schemaProvider.getSchema)
      rows = elements.withFilter(e => e.isInstanceOf[DataRow]).map(e => e.asInstanceOf[DataRow])
      deleteRequests = elements.withFilter(e => e.isInstanceOf[SourceCleanupRequest]).map(e => e.asInstanceOf[SourceCleanupRequest])
      table <- ZIO.fromFuture(implicit ec => catalogWriter.write(rows, name, arcaneSchema))
      batch = table.toStagedBatch( icebergCatalogSettings.namespace, icebergCatalogSettings.warehouse, arcaneSchema, sinkSettings.targetTableFullName, Map())
    yield (batch, deleteRequests)


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
            mergeProcessor: BatchProcessor[InFlightBatch, InFlightBatch],
            archivationProcessor: BatchProcessor[InFlightBatch, CompletedBatch],
            sourceCleanupProcessor: BatchProcessor[CompletedBatch, PiplineResult]): IcebergSynapseConsumer =
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
    & BatchProcessor[CompletedBatch, PiplineResult]

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
        sourceCleanupProcessor <- ZIO.service[BatchProcessor[CompletedBatch, PiplineResult]]
      yield IcebergSynapseConsumer(streamContext, icebergCatalogSettings, sinkSettings, catalogWriter, schemaProvider, mergeProcessor, archivationProcessor, sourceCleanupProcessor)
    }
