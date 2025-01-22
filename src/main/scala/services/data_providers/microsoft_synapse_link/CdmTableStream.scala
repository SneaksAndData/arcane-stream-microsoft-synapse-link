package com.sneaksanddata.arcane.microsoft_synapse_link
package services.data_providers.microsoft_synapse_link

import models.app.{AzureConnectionSettings, ParallelismSettings}

import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.cdm.CSVParser.{parseCsvLine, replaceQuotedNewlines}
import com.sneaksanddata.arcane.framework.models.cdm.{SimpleCdmEntity, given_Conversion_SimpleCdmEntity_ArcaneSchema, given_Conversion_String_ArcaneSchema_DataRow}
import com.sneaksanddata.arcane.framework.models.{ArcaneSchema, DataRow}
import com.sneaksanddata.arcane.framework.services.cdm.CdmTableSettings
import com.sneaksanddata.arcane.framework.services.storage.models.azure.AdlsStoragePath
import com.sneaksanddata.arcane.framework.services.storage.models.base.StoredBlob
import org.slf4j.{Logger, LoggerFactory}
import zio.stream.{ZPipeline, ZStream}
import zio.{Chunk, Schedule, Task, ZIO, ZLayer}

import java.io.{IOException, Reader}
import java.time.{Duration, OffsetDateTime, ZoneOffset}

class CdmTableStream(
                      name: String,
                      storagePath: AdlsStoragePath,
                      entityModel: SimpleCdmEntity,
                      reader: AzureBlobStorageReaderZIO,
                      parallelismSettings: ParallelismSettings,
                      streamContext: StreamContext):
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  private val defaultFromYears: Int = 1
  private val schema: ArcaneSchema = implicitly(entityModel)
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Read top-level virtual directories to allow pre-filtering blobs
   * @param startDate Baseline date to start search from
   * @return A list of yyyy-MM-ddTHH prefixes to apply as filters
   */
  private def getListPrefixes(startDate: Option[OffsetDateTime], endDate: Option[OffsetDateTime] = None): Seq[String] =
    System.out.println(s"Getting prefixes for $name starting from $startDate to $endDate")
    val currentMoment = endDate.getOrElse(OffsetDateTime.now(ZoneOffset.UTC).plusHours(1))
    val startMoment = startDate.getOrElse(currentMoment.minusYears(defaultFromYears))
    Iterator.iterate(startMoment)(_.plusHours(1))
      .takeWhile(_.toEpochSecond < currentMoment.toEpochSecond)
      .map { moment =>
        val monthString = s"00${moment.getMonth.getValue}".takeRight(2)
        val dayString = s"00${moment.getDayOfMonth}".takeRight(2)
        val hourString = s"00${moment.getHour}".takeRight(2)
        s"${moment.getYear}-$monthString-${dayString}T$hourString"
      }.to(LazyList)

  /**
   * Read a table snapshot, taking optional start time. Lowest precision available is 1 hour
   * @param startDate Folders from Synapse export to include in the snapshot, based on the start date provided. If not provided, ALL folders from now - defaultFromYears will be included
   * @param endDate Date to stop at when looking for prefixes. In production use None for this value to always look data up to current moment.
   * @return A stream of rows for this table
   */
  def snapshotPrefixes(lookBackInterval: Duration): ZStream[Any, Throwable, StoredBlob] =
    val lookbackStream = ZStream.fromZIO(reader.getFirstBlob(storagePath + "/"))
        .flatMap(startDate => {
          ZStream.fromIterable(getListPrefixes(Some(startDate)))
            .flatMap(prefix => reader.streamPrefixes(storagePath + prefix))
            .flatMap(prefix => reader.streamPrefixes(storagePath + prefix.name + name + "/"))
            .filter(blob => blob.name.endsWith(".csv"))
        })

    val repeatStream = reader.getRootPrefixes(storagePath, lookBackInterval)
      .flatMap(prefix => reader.streamPrefixes(storagePath + prefix.name + name + "/"))
      .filter(blob => blob.name.endsWith(".csv"))
      .repeat(Schedule.spaced(Duration.ofSeconds(90)))

    if streamContext.IsBackfilling then lookbackStream else repeatStream

  def getStream(blob: StoredBlob): ZIO[Any, IOException, Reader] =
    reader.getBlobContent(storagePath + blob.name).mapError(e => new IOException(s"Failed to get blob content: ${e.getMessage}", e))

  def getData(javaStream: Reader): ZStream[Any, IOException, DataRow] =
      val stream = ZStream.fromReader(javaStream)
      stream
          .via(ZPipeline.mapChunks(chars => Chunk.single(new String(chars.toArray))))
          .via(ZPipeline.splitLines)
          .map(content => replaceQuotedNewlines(content))
          .map(implicitly[DataRow](_, schema))

object CdmTableStream:
  type Environment = AzureConnectionSettings
    & CdmTableSettings
    & AzureBlobStorageReaderZIO
    & CdmSchemaProvider
    & ParallelismSettings
    & StreamContext

  def apply(settings: CdmTableSettings,
            entityModel: SimpleCdmEntity,
            reader: AzureBlobStorageReaderZIO,
            parallelismSettings: ParallelismSettings,
            streamContext: StreamContext): CdmTableStream = new CdmTableStream(
    name = settings.name,
    storagePath = AdlsStoragePath(settings.rootPath).get,
    entityModel = entityModel,
    reader = reader,
    parallelismSettings = parallelismSettings,
    streamContext = streamContext
  )


  /**
   * The ZLayer that creates the CdmDataProvider.
   */
  val layer: ZLayer[Environment, Throwable, CdmTableStream] =
    ZLayer {
      for {
        _ <- ZIO.log("Creating the CDM data provider")
        connectionSettings <- ZIO.service[AzureConnectionSettings]
        tableSettings <- ZIO.service[CdmTableSettings]
        reader <- ZIO.service[AzureBlobStorageReaderZIO]
        schemaProvider <- ZIO.service[CdmSchemaProvider]
        parSettings <- ZIO.service[ParallelismSettings]
        l <- ZIO.fromFuture(_ => schemaProvider.getEntity)
        sc <- ZIO.service[StreamContext]
      } yield CdmTableStream(tableSettings, l, reader, parSettings, sc)
    }

