package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.processors

import models.app.streaming.SourceCleanupRequest

import com.sneaksanddata.arcane.microsoft_synapse_link.services.data_providers.microsoft_synapse_link.DataStreamElement
import zio.{Chunk, ZIO}
import zio.stream.ZPipeline

class WriteStagingTableProcessor


  def process = ZPipeline[Chunk[DataStreamElement]]()
    .mapZIO(elements =>
      val groupedBySchema = elements.withFilter(e => e.isInstanceOf[DataRow]).map(e => e.asInstanceOf[DataRow]).groupBy(_.schema)
      val deleteRequests = elements.withFilter(e => e.isInstanceOf[SourceCleanupRequest]).map(e => e.asInstanceOf[SourceCleanupRequest])
      val batchResults = ZIO.foreach(groupedBySchema){
        case (schema, rows) => writeDataRows(rows, schema)
      }
      batchResults.map(b => (b.values, deleteRequests))
    )
    .zipWithIndex
  
  private def writeDataRows(rows: Chunk[DataRow], arcaneSchema: ArcaneSchema): Task[(ArcaneSchema, StagedVersionedBatch)] =
    val tableWriterEffect =
      zlog("Attempting to write data to staging table") *>
        ZIO.fromFuture(implicit ec => catalogWriter.write(rows, streamContext.stagingTableNamePrefix.getTableName, arcaneSchema))
    for
      table <- tableWriterEffect.retry(retryPolicy)
      batch = table.toStagedBatch(icebergCatalogSettings.namespace, icebergCatalogSettings.warehouse, arcaneSchema, sinkSettings.targetTableFullName, Map())
    yield (arcaneSchema, batch)
