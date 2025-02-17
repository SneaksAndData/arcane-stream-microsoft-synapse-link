package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.processors

import extensions.DataRowExtensions.schema
import models.app.streaming.SourceCleanupRequest
import models.app.{MicrosoftSynapseLinkStreamContext, TargetTableSettings}
import services.data_providers.microsoft_synapse_link.DataStreamElement
import services.streaming.consumers.{InFlightBatch, IncomingBatch}
import services.streaming.processors.StagingTableProcessor.{getTableName, toStagedBatch}

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.models.{ArcaneSchema, DataRow, MergeKeyField}
import com.sneaksanddata.arcane.framework.services.consumers.{StagedVersionedBatch, SynapseLinkMergeBatch}
import com.sneaksanddata.arcane.framework.services.lakehouse.base.IcebergCatalogSettings
import com.sneaksanddata.arcane.framework.services.lakehouse.{CatalogWriter, given_Conversion_ArcaneSchema_Schema}
import com.sneaksanddata.arcane.framework.services.streaming.base.BatchProcessor
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.stream.ZPipeline
import zio.{Chunk, Schedule, Task, ZIO, ZLayer}

import java.time.format.DateTimeFormatter
import java.time.{Duration, ZoneOffset, ZonedDateTime}
import java.util.UUID

class StagingTableProcessor(streamContext: MicrosoftSynapseLinkStreamContext,
                            icebergCatalogSettings: IcebergCatalogSettings,
                            catalogWriter: CatalogWriter[RESTCatalog, Table, Schema])
  extends BatchProcessor[IncomingBatch, InFlightBatch]:

  private val retryPolicy = Schedule.exponential(Duration.ofSeconds(1)) && Schedule.recurs(10)

  override def process: ZPipeline[Any, Throwable, IncomingBatch, InFlightBatch] = ZPipeline[IncomingBatch]()
    .mapZIO{
      case (elements, target) =>
        val groupedBySchema = elements.withFilter(e => e.isInstanceOf[DataRow]).map(e => e.asInstanceOf[DataRow]).groupBy(_.schema)
        val deleteRequests = elements.withFilter(e => e.isInstanceOf[SourceCleanupRequest]).map(e => e.asInstanceOf[SourceCleanupRequest])
        val batchResults = ZIO.foreach(groupedBySchema) {
          case (schema, rows) => writeDataRows(rows, schema, target)
        }
        batchResults.map(b => (b.values, deleteRequests))
    }
    .zipWithIndex


  private def writeDataRows(rows: Chunk[DataRow], arcaneSchema: ArcaneSchema, target: String): Task[(ArcaneSchema, StagedVersionedBatch)] =
    val tableWriterEffect =
      zlog("Attempting to write data to staging table") *>
        ZIO.fromFuture(implicit ec => catalogWriter.write(rows, streamContext.stagingTableNamePrefix.getTableName, arcaneSchema))
    for
      table <- tableWriterEffect.retry(retryPolicy)
      batch = table.toStagedBatch(icebergCatalogSettings.namespace, icebergCatalogSettings.warehouse, arcaneSchema, target, Map())
    yield (arcaneSchema, batch)


object StagingTableProcessor:

  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss")
  
  extension (stagingTablePrefix: String) def getTableName: String =
    s"${stagingTablePrefix}__${ZonedDateTime.now(ZoneOffset.UTC).format(formatter)}_${UUID.randomUUID().toString}".replace('-', '_')

  extension (table: Table) def toStagedBatch(namespace: String,
                                             warehouse: String,
                                             batchSchema: ArcaneSchema,
                                             targetName: String,
                                             partitionValues: Map[String, List[String]]): StagedVersionedBatch =
    val batchName = table.name().split('.').last
    SynapseLinkMergeBatch(batchName, batchSchema, targetName, partitionValues)

  def apply(streamContext: MicrosoftSynapseLinkStreamContext,
            icebergCatalogSettings: IcebergCatalogSettings,
            catalogWriter: CatalogWriter[RESTCatalog, Table, Schema]): StagingTableProcessor =
    new StagingTableProcessor(streamContext, icebergCatalogSettings, catalogWriter)


  type Environment = MicrosoftSynapseLinkStreamContext
    & IcebergCatalogSettings
    & CatalogWriter[RESTCatalog, Table, Schema]


  val layer: ZLayer[Environment, Nothing, StagingTableProcessor] =
    ZLayer {
      for
        streamContext <- ZIO.service[MicrosoftSynapseLinkStreamContext]
        icebergCatalogSettings <- ZIO.service[IcebergCatalogSettings]
        catalogWriter <- ZIO.service[CatalogWriter[RESTCatalog, Table, Schema]]
      yield StagingTableProcessor(streamContext, icebergCatalogSettings, catalogWriter)
    }
