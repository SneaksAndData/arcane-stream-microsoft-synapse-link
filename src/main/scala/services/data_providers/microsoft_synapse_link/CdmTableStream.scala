package com.sneaksanddata.arcane.microsoft_synapse_link
package services.data_providers.microsoft_synapse_link

import models.app.streaming.SourceCleanupRequest
import models.app.{AzureConnectionSettings, ParallelismSettings}
import services.data_providers.microsoft_synapse_link.CdmTableStream.getListPrefixes
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.*

import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.cdm.{SimpleCdmEntity, given_Conversion_SimpleCdmEntity_ArcaneSchema, given_Conversion_String_ArcaneSchema_DataRow}
import com.sneaksanddata.arcane.framework.models.{ArcaneSchema, DataRow}
import com.sneaksanddata.arcane.framework.services.cdm.CdmTableSettings
import com.sneaksanddata.arcane.framework.services.storage.models.azure.AdlsStoragePath
import com.sneaksanddata.arcane.framework.services.storage.models.base.StoredBlob
import zio.stream.ZStream
import zio.{Schedule, ZIO, ZLayer}

import java.io.{BufferedReader, IOException}
import java.time.{Duration, OffsetDateTime, ZoneOffset}
import java.util.regex.Matcher
import scala.util.matching.Regex

type DataStreamElement = DataRow | SourceCleanupRequest

class CdmTableStream(
                      name: String,
                      storagePath: AdlsStoragePath,
                      entityModel: SimpleCdmEntity,
                      reader: AzureBlobStorageReaderZIO,
                      parallelismSettings: ParallelismSettings,
                      streamContext: StreamContext):
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  private val schema: ArcaneSchema = implicitly(entityModel)

  /**
   * Read a table snapshot, taking optional start time. Lowest precision available is 1 hour
   *
   * @param lookBackInterval      The look back interval to start from
   * @param changeCaptureInterval Interval to capture changes
   * @return A stream of rows for this table
   */
  def snapshotPrefixes(lookBackInterval: Duration, changeCaptureInterval: Duration): ZStream[Any, Throwable, StoredBlob] =
    val backfillStream = ZStream.fromZIO(reader.getFirstBlob(storagePath + "/"))
      .flatMap(startDate => {
        ZStream.fromIterable(getListPrefixes(Some(startDate)))
          .flatMap(prefix => reader.streamPrefixes(storagePath + prefix))
          .flatMap(prefix => reader.streamPrefixes(storagePath + prefix.name + name + "/"))
          .filter(blob => blob.name.endsWith(".csv"))
      })

    val repeatStream = reader.getRootPrefixes(storagePath, lookBackInterval)
      .flatMap(prefix => reader.streamPrefixes(storagePath + prefix.name + name))
      .filter(blob => blob.name.endsWith(s"/$name/"))
      .flatMap(prefix => reader.streamPrefixes(storagePath + prefix.name))
      .filter(blob => blob.name.endsWith(".csv"))
      .repeat(Schedule.spaced(changeCaptureInterval))

    if streamContext.IsBackfilling then backfillStream else repeatStream

  def getStream(blob: StoredBlob): ZIO[Any, IOException, (BufferedReader, AdlsStoragePath)] =
    reader.getBlobContent(storagePath + blob.name)
      .map(javaReader => (javaReader, storagePath + blob.name))
      .mapError(e => new IOException(s"Failed to get blob content: ${e.getMessage}", e))

  def tryGetContinuation(stream: BufferedReader, quotes: Int, accum: StringBuilder): ZIO[Any, Throwable, String] =
    if quotes % 2 == 0 then
      ZIO.succeed(accum.toString())
    else
      for {
        line <- ZIO.attemptBlocking(Option(stream.readLine()))
        continuation <- tryGetContinuation(stream, quotes + line.getOrElse("").count(_ == '"'), accum.append(s"\n$line"))
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

  def getData(streamData: (BufferedReader, AdlsStoragePath)): ZStream[Any, IOException, DataStreamElement] = streamData match
    case (javaStream, fileName) =>
      ZStream.acquireReleaseWith(ZIO.attempt(javaStream))(stream => ZIO.succeed(stream.close()))
        .tap(_ => zlog(s"Getting data from directory: $fileName"))
        .flatMap(javaReader => ZStream.repeatZIO(getLine(javaReader)))
        .takeWhile(_.isDefined)
        .map(_.get)
        .mapZIO(content => ZIO.attempt(replaceQuotedNewlines(content)))
        .mapZIO(content => ZIO.attempt(implicitly[DataRow](content, schema)))
        .mapError(e => new IOException(s"Failed to parse CSV content: ${e.getMessage} from file: $fileName", e))
        .concat(ZStream.succeed(SourceCleanupRequest(fileName)))

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
        _ <- zlog("Creating the CDM data provider")
        connectionSettings <- ZIO.service[AzureConnectionSettings]
        tableSettings <- ZIO.service[CdmTableSettings]
        reader <- ZIO.service[AzureBlobStorageReaderZIO]
        schemaProvider <- ZIO.service[CdmSchemaProvider]
        parSettings <- ZIO.service[ParallelismSettings]
        l <- ZIO.fromFuture(_ => schemaProvider.getEntity)
        sc <- ZIO.service[StreamContext]
      } yield CdmTableStream(tableSettings, l, reader, parSettings, sc)
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
