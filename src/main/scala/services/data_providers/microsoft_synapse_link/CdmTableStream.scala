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
import com.sneaksanddata.arcane.framework.services.storage.models.azure.{AdlsStoragePath, AzureBlobStorageReader}
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
                      zioReader: AzureBlobStorageReaderZIO,
                      reader: AzureBlobStorageReader,
                      parallelismSettings: ParallelismSettings,
                      streamContext: StreamContext):
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val schema: ArcaneSchema = implicitly(entityModel)

  /**
   * Read a table snapshot, taking optional start time. Lowest precision available is 1 hour
   *
   * @param lookBackInterval      The look back interval to start from
   * @param changeCaptureInterval Interval to capture changes
   * @return A stream of rows for this table
   */
  def snapshotPrefixes(lookBackInterval: Duration, changeCaptureInterval: Duration): ZStream[Any, Throwable, (StoredBlob, CdmSchemaProvider)] =
    getRootDropPrefixes(storagePath, lookBackInterval)
      .flatMap({
          case (prefix, schemaProvider) => zioReader.streamPrefixes(storagePath + prefix.name + name).zip(ZStream.succeed(schemaProvider))
        })
        .filter({
          case (blob, _) => blob.name.endsWith(s"/$name/")
        })
        .flatMap({
          case (prefix, schema) => zioReader.streamPrefixes(storagePath + prefix.name).zip(ZStream.succeed(schema))
        })
        .filter({
          case (blob, _) => blob.name.endsWith(".csv")
        })
        .repeat(Schedule.spaced(changeCaptureInterval))


  private def getRootDropPrefixes(storageRoot: AdlsStoragePath, lookBackInterval: Duration): ZStream[Any, Throwable, (StoredBlob, CdmSchemaProvider)] =
    for prefix <- zioReader.getRootPrefixes(storagePath, lookBackInterval).filterZIO(prefix => zioReader.blobExists(storagePath + prefix.name + "model.json"))
      schemaProvider = CdmSchemaProvider(reader, (storagePath + prefix.name).toHdfsPath, name)
    yield (prefix, schemaProvider)


  def getStream(blob: (StoredBlob, CdmSchemaProvider)): ZIO[Any, IOException, (BufferedReader, AdlsStoragePath, CdmSchemaProvider)] =
    zioReader.getBlobContent(storagePath + blob._1.name)
      .map(javaReader => (javaReader, storagePath + blob._1.name, blob._2))
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

  def getData(streamData: (BufferedReader, AdlsStoragePath)): ZStream[Any, IOException, DataStreamElement] = streamData match
    case (javaStream, fileName) =>
      ZStream.acquireReleaseWith(ZIO.attempt(javaStream))(stream => ZIO.succeed(stream.close()))
        .tap(_ => zlog(s"Getting data from directory: $fileName"))
        .flatMap(javaReader => ZStream.repeatZIO(getLine(javaReader)))
        .takeWhile(_.isDefined)
        .map(_.get)
        .mapZIO(content => ZIO.attempt(replaceQuotedNewlines(content)))
        .mapZIO(content => ZIO.fromFuture(sc => schemaProvider.getSchema).map(schema => (content, schema)))
        .mapZIO({
          case (content, schema) => ZIO.attempt(implicitly[DataRow](content, schema))
        })
        .mapError(e => new IOException(s"Failed to parse CSV content: ${e.getMessage} from file: $fileName", e))
        .concat(ZStream.succeed(SourceCleanupRequest(fileName)))
        .zipWithIndex
        .flatMap({
          case (e: SourceCleanupRequest, index: Long) => ZStream.log(s"Received $index lines frm $fileName, completed processing") *> ZStream.succeed(e)
          case (r: DataRow, _) => ZStream.succeed(r)
        })

object CdmTableStream:
  type Environment = AzureConnectionSettings
    & CdmTableSettings
    & AzureBlobStorageReaderZIO
    & AzureBlobStorageReader
    & CdmSchemaProvider
    & ParallelismSettings
    & StreamContext

  def apply(settings: CdmTableSettings,
            entityModel: SimpleCdmEntity,
            zioReader: AzureBlobStorageReaderZIO,
            reader: AzureBlobStorageReader,
            parallelismSettings: ParallelismSettings,
            streamContext: StreamContext): CdmTableStream = new CdmTableStream(
    name = settings.name,
    storagePath = AdlsStoragePath(settings.rootPath).get,
    entityModel = entityModel,
    zioReader = zioReader,
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
        readerZIO <- ZIO.service[AzureBlobStorageReaderZIO]
        reader <- ZIO.service[AzureBlobStorageReader]
        schemaProvider <- ZIO.service[CdmSchemaProvider]
        parSettings <- ZIO.service[ParallelismSettings]
        l <- ZIO.fromFuture(_ => schemaProvider.getEntity)
        sc <- ZIO.service[StreamContext]
      } yield CdmTableStream(tableSettings, l, readerZIO, reader, parSettings, sc)
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
