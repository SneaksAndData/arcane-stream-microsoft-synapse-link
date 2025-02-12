package com.sneaksanddata.arcane.microsoft_synapse_link
package services.data_providers.microsoft_synapse_link

import com.azure.core.credential.TokenCredential
import com.azure.core.http.rest.PagedResponse
import com.azure.core.util.BinaryData
import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.models.{BlobListDetails, ListBlobsOptions}
import com.azure.storage.blob.{BlobClient, BlobContainerClient, BlobServiceClientBuilder}
import com.azure.storage.common.StorageSharedKeyCredential
import com.azure.storage.common.policy.{RequestRetryOptions, RetryPolicyType}
import com.sneaksanddata.arcane.framework.services.storage.models.azure.AzureModelConversions.given_Conversion_BlobItem_StoredBlob
import com.sneaksanddata.arcane.framework.services.storage.models.azure.{AdlsStoragePath, AzureBlobStorageReaderSettings}
import com.sneaksanddata.arcane.framework.services.storage.models.base.StoredBlob
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.*
import models.app.streaming.{SourceCleanupResult, SourceDeletionResult}

import zio.stream.ZStream
import zio.{Chunk, Task, ZIO}

import java.io.{BufferedReader, InputStreamReader, Reader}
import java.time.format.DateTimeFormatter
import java.time.{Duration, OffsetDateTime, ZoneOffset}
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.*
import scala.language.{existentials, implicitConversions}
import scala.util.Try

/**
 * Blob reader implementation for Azure. Relies on the default credential chain if no added credentials are provided.
 * @param accountName Storage account name
 * @param tokenCredential Optional token credential provider
 * @param sharedKeyCredential Optional access key credential
 */
final class AzureBlobStorageReaderZIO(accountName: String, endpoint: Option[String], tokenCredential: Option[TokenCredential], sharedKeyCredential: Option[StorageSharedKeyCredential], settings: Option[AzureBlobStorageReaderSettings], deleteDryRun: Boolean):
  private val serviceClientSettings = settings.getOrElse(AzureBlobStorageReaderSettings())
  private lazy val defaultCredential = new DefaultAzureCredentialBuilder().build()
  private lazy val clientBuilder =
    val builder = (tokenCredential, sharedKeyCredential) match
      case (Some(credential), _) => new BlobServiceClientBuilder().credential(credential)
      case (None, Some(credential)) => new BlobServiceClientBuilder().credential(credential)
      case (None, None) => new BlobServiceClientBuilder().credential(defaultCredential)
    builder
      .endpoint(endpoint.getOrElse("https://$accountName.blob.core.windows.net/"))
      .retryOptions(RequestRetryOptions(RetryPolicyType.EXPONENTIAL, serviceClientSettings.httpMaxRetries, serviceClientSettings.httpRetryTimeout.toSeconds.toInt, serviceClientSettings.httpMinRetryDelay.toMillis, serviceClientSettings.httpMaxRetryDelay.toMillis, null))
    
  private lazy val serviceClient = clientBuilder.buildClient()
    
  private lazy val asyncServiceClient = clientBuilder.buildAsyncClient()

  private val defaultTimeout = Duration.ofSeconds(30)
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  private def getBlobClient(blobPath: AdlsStoragePath): BlobClient =
    require(blobPath.accountName == accountName, s"Account name in the path `${blobPath.accountName}` does not match account name `$accountName` initialized for this reader")
    serviceClient.getBlobContainerClient(blobPath.container).getBlobClient(blobPath.blobPrefix)

  private def getBlobContainerClient(blobPath: AdlsStoragePath): BlobContainerClient =
    serviceClient.getBlobContainerClient(blobPath.container)

  private val stringContentSerializer: Array[Byte] => String = _.map(_.toChar).mkString

  /**
   *
   * @param blobPath The path to the blob.
   * @param deserializer function to deserialize the content of the blob. Deserializes all content as String if not implementation is provided
   * @tparam Result The type of the result.
   *  @return The result of applying the function to the content of the blob.
   */
  def getBlobContent[Result](blobPath: AdlsStoragePath, deserializer: Array[Byte] => Result = stringContentSerializer): Task[BufferedReader] =
    val client = getBlobClient(blobPath)
    for
      _ <- zlog("Downloading blob content from data file: " + blobPath.toHdfsPath)
      stream <- ZIO.attemptBlocking { 
        val stream = client.openInputStream() 
        new BufferedReader(new InputStreamReader(stream))
      }
    yield stream

  def streamPrefixes(rootPrefix: AdlsStoragePath): ZStream[Any, Throwable, StoredBlob] =
    val client = serviceClient.getBlobContainerClient(rootPrefix.container)
    val listOptions = new ListBlobsOptions()
      .setPrefix(rootPrefix.blobPrefix)
      .setMaxResultsPerPage(serviceClientSettings.maxResultsPerPage)
      .setDetails(
        BlobListDetails()
          .setRetrieveMetadata(false)
          .setRetrieveDeletedBlobs(false)
          .setRetrieveVersions(false)
      )

    val publisher = client.listBlobsByHierarchy("/", listOptions, defaultTimeout).stream().toList.asScala.map(implicitly)
    ZStream.fromIterable(publisher)

  def blobExists(blobPath: AdlsStoragePath): Task[Boolean] =
    ZIO.attemptBlocking(getBlobClient(blobPath).exists())
      .flatMap(result => ZIO.logDebug(s"Blob ${blobPath.toHdfsPath} exists: $result") *> ZIO.succeed(result))

  def getFirstBlob(storagePath: AdlsStoragePath): Task[OffsetDateTime] =
    streamPrefixes(storagePath).runFold(OffsetDateTime.now(ZoneOffset.UTC)){ (date, blob) =>
      val current = interpretAsDate(blob).getOrElse(date)
      if current.isBefore(date) then current else date
    }

  private def interpretAsDate(blob: StoredBlob): Option[OffsetDateTime] =
    val name = blob.name.replaceAll("/$", "")
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ssX")
    Try(OffsetDateTime.parse(name, formatter)).toOption

  def getRootPrefixes(storagePath: AdlsStoragePath, startFrom: OffsetDateTime): ZStream[Any, Throwable, StoredBlob] =
    for _ <- zlogStream("Getting root prefixes stating from " + startFrom)
        list <- ZStream.succeed(CdmTableStream.getListPrefixes(Some(startFrom)))
        listZIO = ZIO.foreach(list)(prefix => ZIO.attemptBlocking { streamPrefixes(storagePath + prefix) })
        prefixes <- ZStream.fromIterableZIO(listZIO)
        zippedWithDate <- prefixes.map(blob => (interpretAsDate(blob), blob))
        eligibleToProcess <- zippedWithDate match
          case (Some(date), blob) if date.isAfter(startFrom) => ZStream.succeed(blob)
          case _ => ZStream.empty
    yield eligibleToProcess
    
  def getRootPrefixes(storagePath: AdlsStoragePath, lookBackInterval: Duration): ZStream[Any, Throwable, StoredBlob] =
    val startFrom = OffsetDateTime.now(ZoneOffset.UTC).minus(lookBackInterval)
    getRootPrefixes(storagePath, startFrom)
    
  def markForDeletion(fileName: AdlsStoragePath): ZIO[Any, Throwable, SourceCleanupResult] =
      val deleteMarker = fileName.copy(blobPrefix = fileName.blobPrefix + ".delete")
      zlog(s"Marking source file for deletion: $fileName with marker: $deleteMarker") *>
        ZIO.attemptBlocking {
            serviceClient.getBlobContainerClient(fileName.container)
              .getBlobClient(deleteMarker.blobPrefix)
              .upload(BinaryData.fromString(""), true)
        }
        .map(result => SourceCleanupResult(fileName, deleteMarker))

  def deleteBlob(fileName: AdlsStoragePath): ZIO[Any, Throwable, SourceDeletionResult] =
    if deleteDryRun then
      ZIO.log("Dry run: Deleting blob: " + fileName).map(_ => SourceDeletionResult(fileName, true))
    else
      ZIO.log("Deleting blob: " + fileName) *>
      ZIO.attemptBlocking(serviceClient.getBlobContainerClient(fileName.container).getBlobClient(fileName.blobPrefix).deleteIfExists())
         .map(result => SourceDeletionResult(fileName, result))

object AzureBlobStorageReaderZIO:

  /**
   * Create AzureBlobStorageReaderZIO for the account using StorageSharedKeyCredential and custom endpoint
   *
   * @param accountName Storage account name
   * @param endpoint Storage account endpoint                    
   * @param credential  StorageSharedKeyCredential (account key)
   * @return AzureBlobStorageReaderZIO instance
   */
  def apply(accountName: String, endpoint: String, credential: StorageSharedKeyCredential, deleteDryRun: Boolean): AzureBlobStorageReaderZIO = new AzureBlobStorageReaderZIO(accountName, Some(endpoint), None, Some(credential), None, deleteDryRun)


  private val defaultFromYears: Int = 1
  /**
   * Iterate by dates from the start date to the end date.
   * This method can be used to iterate over root prefixes in Azure Blob Storage if the prefixes are named by date.
   */
  extension (startDate: Option[OffsetDateTime]) def iterateByDates(endDate: Option[OffsetDateTime] = None): Seq[String] =
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
