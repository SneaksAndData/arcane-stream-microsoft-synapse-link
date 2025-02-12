package com.sneaksanddata.arcane.microsoft_synapse_link
package services.data_providers.microsoft_synapse_link

import com.sneaksanddata.arcane.framework.models.{ArcaneSchema, DataRow}
import com.sneaksanddata.arcane.framework.services.consumers.{StagedBackfillBatch, SynapseLinkBackfillBatch}
import com.sneaksanddata.arcane.framework.services.lakehouse.{CatalogWriter, given_Conversion_ArcaneSchema_Schema}
import com.sneaksanddata.arcane.framework.services.mssql.MsSqlConnection.BackfillBatch
import com.sneaksanddata.arcane.framework.services.streaming.base.BackfillDataProvider
import com.sneaksanddata.arcane.microsoft_synapse_link.models.app.{MicrosoftSynapseLinkStreamContext, ParallelismSettings, TargetTableSettings}
import com.sneaksanddata.arcane.microsoft_synapse_link.models.app.streaming.SourceCleanupRequest
import com.sneaksanddata.arcane.microsoft_synapse_link.services.streaming.processors.CdmGroupingProcessor
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.stream.{ZPipeline, ZStream}
import zio.{Chunk, Task, UIO, URIO, ZIO}
import com.sneaksanddata.arcane.microsoft_synapse_link.extensions.DataRowExtensions.schema
import com.sneaksanddata.arcane.microsoft_synapse_link.services.app.TableManager
import com.sneaksanddata.arcane.microsoft_synapse_link.services.data_providers.microsoft_synapse_link.MicrosoftSynapseLinkDataProvider.getTableName

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.UUID

class MicrosoftSynapseLinkDataProvider(cdmTableStream: CdmTableStream,
                                       streamContext: MicrosoftSynapseLinkStreamContext,
                                       parallelismSettings: ParallelismSettings,
                                       processor: CdmGroupingProcessor,
                                       catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
                                       tableManager: TableManager,
                                       sinkSettings: TargetTableSettings):

  def requestBackfill: Unit =
    val backfillStream = cdmTableStream
      .getPrefixesFromBeginning
      .mapZIOPar(parallelismSettings.parallelism)(blob => cdmTableStream.getStream(blob))
      .flatMap(reader => cdmTableStream.getData(reader))
      .via(processor.process)
      .zipWithIndex
      .via(writeStagingTable)

    backfillStream.concat(ZStream.fromZIO(createBackfillBatch(streamContext.stagingTableNamePrefix)))
//      .via(writeStagingTable)


  private def writeStagingTable = ZPipeline[(Chunk[DataStreamElement], Long)]()
    .map { case (elements, index) =>
      val rows = elements.withFilter(e => e.isInstanceOf[DataRow]).map(e => e.asInstanceOf[DataRow])
      val deleteRequests = elements.withFilter(e => e.isInstanceOf[SourceCleanupRequest]).map(e => e.asInstanceOf[SourceCleanupRequest])
      (rows, deleteRequests, index)
    }
    .mapZIO({
      case (rows, deleteRequests, 0) => createBackfillTable(rows) *> ZIO.succeed(deleteRequests)
      case (rows, deleteRequests, _) => appendDataRows(rows) *> ZIO.succeed(deleteRequests)
    })

  private def createBackfillTable(rows: Chunk[DataRow]): Task[Unit] =
    val groupedBySchema = rows.groupBy(_.schema)
    val schemas = groupedBySchema.keys.toList.sortBy(s => s.length).zipWithIndex
    for
      applicationResults <- ZIO.foreach(schemas){ case (currentSchema, index) =>
        if index == 0 then
          ZIO.fromFuture { implicit ec =>
              catalogWriter.write(groupedBySchema(currentSchema), streamContext.stagingTableNamePrefix.getTableName, currentSchema)
          }
        else
          tableManager.migrateSchema(currentSchema, streamContext.stagingTableNamePrefix.getTableName)
            *> ZIO.fromFuture( implicit ec => catalogWriter.append(groupedBySchema(currentSchema), streamContext.stagingTableNamePrefix.getTableName, currentSchema))
      }
    yield ()

  private def appendDataRows(rows: Chunk[DataRow]): Task[Unit] =
    val groupedBySchema = rows.groupBy(_.schema)
    val schemas = groupedBySchema.keys.toList.sortBy(s => s.length).zipWithIndex
    for
      applicationResults <- ZIO.foreach(schemas){ case (currentSchema, index) =>
          tableManager.migrateSchema(currentSchema, streamContext.stagingTableNamePrefix.getTableName)
            *> ZIO.fromFuture( implicit ec => catalogWriter.append(groupedBySchema(currentSchema), streamContext.stagingTableNamePrefix.getTableName, currentSchema))
      }
    yield ()

  private def createBackfillBatch(str: String): Task[StagedBackfillBatch] =
    val tableName = str.getTableName
    for schema <- tableManager.getTargetSchema(tableName)
    yield SynapseLinkBackfillBatch(tableName, schema, sinkSettings.targetTableFullName)

object MicrosoftSynapseLinkDataProvider:

  extension (stagingTablePrefix: String) def getTableName: String =
    s"${stagingTablePrefix}__backfill".replace('-', '_')
