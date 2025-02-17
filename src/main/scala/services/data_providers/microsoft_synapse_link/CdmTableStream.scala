package com.sneaksanddata.arcane.microsoft_synapse_link
package services.data_providers.microsoft_synapse_link

import models.app.streaming.SourceCleanupRequest
import models.app.{AzureConnectionSettings, ParallelismSettings, TargetTableSettings}
import services.data_providers.microsoft_synapse_link.CdmTableStream.withSchema

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.*
import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.cdm.given_Conversion_String_ArcaneSchema_DataRow
import com.sneaksanddata.arcane.framework.models.{ArcaneSchema, DataRow}
import com.sneaksanddata.arcane.framework.services.base.SchemaProvider
import com.sneaksanddata.arcane.framework.services.cdm.CdmTableSettings
import com.sneaksanddata.arcane.framework.services.storage.models.azure.{AdlsStoragePath, AzureBlobStorageReader}
import com.sneaksanddata.arcane.framework.services.storage.models.base.StoredBlob
import com.sneaksanddata.arcane.microsoft_synapse_link.services.app.{FieldsFilteringService, TableManager}
import com.sneaksanddata.arcane.framework.services.streaming.base.BackfillDataProvider
import zio.stream.ZStream
import zio.{Schedule, Task, ZIO, ZLayer}

import java.io.{BufferedReader, IOException}
import java.time.{Duration, OffsetDateTime, ZoneOffset}
import java.util.regex.Matcher
import scala.util.matching.Regex

type DataStreamElement = DataRow | SourceCleanupRequest

type BlobStream = ZStream[Any, Throwable, StoredBlob]

type SchemaEnrichedBlobStream = ZStream[Any, Throwable, SchemaEnrichedBlob]

case class SchemaEnrichedBlob(blob: StoredBlob, schemaProvider: SchemaProvider[ArcaneSchema])

case class MetadataEnrichedReader(javaStream: BufferedReader, filePath: AdlsStoragePath, schemaProvider: SchemaProvider[ArcaneSchema])

case class SchemaEnrichedContent[TContent](content: TContent, schema: ArcaneSchema)

class CdmTableStream(name: String,
                      storagePath: AdlsStoragePath,
                      zioReader: AzureBlobStorageReaderZIO,
                      reader: AzureBlobStorageReader,
                      parallelismSettings: ParallelismSettings,
                      streamContext: StreamContext,
                      tableManager: TableManager,
                      targetTableSettings: TargetTableSettings):

  /**
   * Read a table snapshot, taking optional start time. Lowest precision available is 1 hour
   *
   * @param lookBackInterval      The look back interval to start from
   * @param changeCaptureInterval Interval to capture changes
   * @return A stream of rows for this table
   */
  def getPrefixesFromBeginning: ZStream[Any, Throwable, SchemaEnrichedBlob] =
    ZStream.fromZIO(zioReader.getFirstBlob(storagePath)).flatMap( startDate =>
      val streamTask = ZIO.attempt(enrichWithSchema(zioReader.getRootPrefixes(storagePath, startDate)))
      ZStream.fromZIO(dropLast(streamTask))
        .flatMap(x => ZStream.fromIterable(x))
        .flatMap(seb => zioReader.streamPrefixes(storagePath + seb.blob.name).withSchema(seb.schemaProvider))
        .filter(seb => seb.blob.name.endsWith(s"/$name/"))
        .flatMap(seb => zioReader.streamPrefixes(storagePath + seb.blob.name).withSchema(seb.schemaProvider))
        .filter(seb => seb.blob.name.endsWith(".csv"))
    )


  /**
   * Read a table snapshot, taking optional start time. Lowest precision available is 1 hour
   *
   * @param lookBackInterval      The look back interval to start from
   * @param changeCaptureInterval Interval to capture changes
   * @return A stream of rows for this table
   */
  def snapshotPrefixes(lookBackInterval: Duration, changeCaptureInterval: Duration): ZStream[Any, Throwable, SchemaEnrichedBlob] =
    val initialPrefixes = getRootDropPrefixes(storagePath, Some(lookBackInterval)).flatMap(s => s.runCollect)
    val firstStream = ZStream.fromZIO(initialPrefixes)
      .flatMap(x => ZStream.fromIterable(x))
      .flatMap(seb => zioReader.streamPrefixes(storagePath + seb.blob.name).withSchema(seb.schemaProvider))
      .filter(seb => seb.blob.name.endsWith(s"/$name/"))
      .flatMap(seb => zioReader.streamPrefixes(storagePath + seb.blob.name).withSchema(seb.schemaProvider))
      .filter(seb => seb.blob.name.endsWith(".csv"))



    val repeatStream = ZStream.fromZIO(dropLast(getRootDropPrefixes(storagePath, None)))
      .flatMap(x => ZStream.fromIterable(x))
      .flatMap(seb => zioReader.streamPrefixes(storagePath + seb.blob.name).withSchema(seb.schemaProvider))
      .filter(seb => seb.blob.name.endsWith(s"/$name/"))
      .flatMap(seb => zioReader.streamPrefixes(storagePath + seb.blob.name).withSchema(seb.schemaProvider))
      .filter(seb => seb.blob.name.endsWith(".csv"))
      .repeat(Schedule.spaced(changeCaptureInterval))

    firstStream.concat(repeatStream)


  private def dropLast(streamTask: Task[SchemaEnrichedBlobStream]): ZIO[Any, Throwable, Seq[SchemaEnrichedBlob]] =
    for stream <- streamTask
        blobs <- stream.runCollect
        _ <- ZIO.log(s"Dropping last element from from the blobs stream: ${if blobs.nonEmpty then blobs.last.blob.name else "empty"}")
    yield if blobs.nonEmpty then blobs.dropRight(1) else blobs

  private def getRootDropPrefixes(storageRoot: AdlsStoragePath, lookBackInterval: Option[Duration]): Task[SchemaEnrichedBlobStream] =
    val getPrefixesTask = lookBackInterval match
      case Some(interval) => ZIO.succeed(zioReader.getRootPrefixes(storageRoot, interval))
      case None => tableManager
        .getLastUpdateTime(targetTableSettings.targetTableFullName)
        .map(lastUpdate => zioReader.getRootPrefixes(storagePath,lastUpdate))

    getPrefixesTask.map(stream => enrichWithSchema(stream))

  private def enrichWithSchema(stream: ZStream[Any, Throwable, StoredBlob]): ZStream[Any, Throwable, SchemaEnrichedBlob] =
    stream.filterZIO(prefix => zioReader.blobExists(storagePath + prefix.name + "model.json")).map(prefix => {
      val schemaProvider = CdmSchemaProvider(reader, (storagePath + prefix.name).toHdfsPath, name)
      SchemaEnrichedBlob(prefix, schemaProvider)
    })


  def getStream(seb: SchemaEnrichedBlob): ZIO[Any, IOException, MetadataEnrichedReader] =
    zioReader.getBlobContent(storagePath + seb.blob.name)
      .map(javaReader => MetadataEnrichedReader(javaReader, storagePath + seb.blob.name, seb.schemaProvider))
      .mapError(e => new IOException(s"Failed to get blob content: ${e.getMessage}", e))

  def tryGetContinuation(stream: BufferedReader, quotes: Int, accum: StringBuilder): ZIO[Any, Throwable, String] =
    if quotes % 2 == 0 then
      ZIO.succeed(accum.toString())
    else
      for {
        line <- ZIO.attemptBlocking(Option(stream.readLine()))
        continuation <- tryGetContinuation(stream, quotes + line.getOrElse("").count(_ == '"'), accum.append(line.map(l => s"\n$l").getOrElse("")))
      }
      yield continuation

  def getLine(stream: BufferedReader): ZIO[Any, Throwable, Option[String]] =
    for {
      dataLine <- ZIO.attemptBlocking(Option(stream.readLine()))
      continuation <- tryGetContinuation(stream, dataLine.getOrElse("").count(_ == '"'), new StringBuilder())
    }
    yield {
      dataLine match
        case None => None
        case Some(dataLine) if dataLine == "" => None
        case Some(dataLine) => Some(s"$dataLine\n$continuation")
    }

  private def replaceQuotedNewlines(csvLine: String): String = {
    val regex = new Regex("\"[^\"]*(?:\"\"[^\"]*)*\"")
    regex.replaceSomeIn(csvLine, m => Some(Matcher.quoteReplacement(m.matched.replace("\n", "")))).replace("\r", "")
  }

  def getData(streamData: MetadataEnrichedReader): ZStream[Any, IOException, DataStreamElement] =
      ZStream.acquireReleaseWith(ZIO.attempt(streamData.javaStream))(stream => ZIO.succeed(stream.close()))
        .flatMap(javaReader => ZStream.repeatZIO(getLine(javaReader)))
        .takeWhile(_.isDefined)
        .map(_.get)
        .map(_.replace("\n", ""))
        .mapZIO(content => ZIO.fromFuture(sc => streamData.schemaProvider.getSchema).map(schema => SchemaEnrichedContent(content, schema)))
        .mapZIO(sec => ZIO.attempt(implicitly[DataRow](sec.content, sec.schema)))
        .mapError(e => new IOException(s"Failed to parse CSV content: ${e.getMessage} from file: ${streamData.filePath} with", e))
        .concat(ZStream.succeed(SourceCleanupRequest(streamData.filePath)))
        .zipWithIndex
        .flatMap({
          case (e: SourceCleanupRequest, index: Long) => zlogStream(s"Received $index lines frm ${streamData.filePath}, completed file I/O") *> ZStream.succeed(e)
          case (r: DataRow, _) => ZStream.succeed(r)
        })

object CdmTableStream:

  extension (stream: ZStream[Any, Throwable, StoredBlob]) def withSchema(schemaProvider: SchemaProvider[ArcaneSchema]): SchemaEnrichedBlobStream =
    stream.map(blob => SchemaEnrichedBlob(blob, schemaProvider))

  type Environment = AzureConnectionSettings
    & CdmTableSettings
    & AzureBlobStorageReaderZIO
    & AzureBlobStorageReader
    & ParallelismSettings
    & StreamContext
    & TableManager
    & TargetTableSettings

  def apply(settings: CdmTableSettings,
            zioReader: AzureBlobStorageReaderZIO,
            reader: AzureBlobStorageReader,
            parallelismSettings: ParallelismSettings,
            streamContext: StreamContext,
            tableManager: TableManager,
            targetTableSettings: TargetTableSettings): CdmTableStream = new CdmTableStream(
    settings.name,
    AdlsStoragePath(settings.rootPath).get,
    zioReader,
    reader,
    parallelismSettings,
    streamContext,
    tableManager,
    targetTableSettings)

  /**
   * The ZLayer that creates the CdmDataProvider.
   */
  val layer: ZLayer[Environment, Throwable, CdmTableStream] =
    ZLayer {
      for {
        _ <- zlog("Creating the CDM data provider")
        connectionSettings <- ZIO.service[AzureConnectionSettings]
        tableSettings <- ZIO.service[CdmTableSettings]
        readerZIO <- ZIO.service[AzureBlobStorageReaderZIO]
        reader <- ZIO.service[AzureBlobStorageReader]
        parSettings <- ZIO.service[ParallelismSettings]
        sc <- ZIO.service[StreamContext]
        tm <- ZIO.service[TableManager]
        tts <- ZIO.service[TargetTableSettings]
      } yield CdmTableStream(tableSettings, readerZIO, reader, parSettings, sc, tm, tts)
    }


  /**
   * Read top-level virtual directories to allow pre-filtering blobs
   *
   * @param startDate Baseline date to start search from
   * @return A list of yyyy-MM-ddTHH prefixes to apply as filters
   */
  def getListPrefixes(startDate: Option[OffsetDateTime]): Seq[String] =
    val defaultFromYears: Int = 1
    val currentMoment = OffsetDateTime.now(ZoneOffset.UTC).plusHours(1)
    val startMoment = startDate.getOrElse(currentMoment.minusYears(defaultFromYears))
    Iterator.iterate(startMoment)(_.plusHours(1))
      .takeWhile(_.toEpochSecond < currentMoment.toEpochSecond)
      .map { moment =>
        val monthString = s"00${moment.getMonth.getValue}".takeRight(2)
        val dayString = s"00${moment.getDayOfMonth}".takeRight(2)
        val hourString = s"00${moment.getHour}".takeRight(2)
        s"${moment.getYear}-$monthString-${dayString}T$hourString"
      }.to(LazyList)
