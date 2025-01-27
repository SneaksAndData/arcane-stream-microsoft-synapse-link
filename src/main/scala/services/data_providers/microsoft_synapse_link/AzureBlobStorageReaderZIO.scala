package com.sneaksanddata.arcane.microsoft_synapse_link
package services.data_providers.microsoft_synapse_link

import com.azure.core.credential.TokenCredential
import com.azure.core.http.rest.PagedResponse
import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.models.{BlobListDetails, ListBlobsOptions}
import com.azure.storage.blob.{BlobClient, BlobContainerClient, BlobServiceClientBuilder}
import com.azure.storage.common.StorageSharedKeyCredential
import com.azure.storage.common.policy.{RequestRetryOptions, RetryPolicyType}
import com.sneaksanddata.arcane.framework.services.storage.models.azure.AzureModelConversions.given_Conversion_BlobItem_StoredBlob
import com.sneaksanddata.arcane.framework.services.storage.models.azure.{AdlsStoragePath, AzureBlobStorageReaderSettings}
import com.sneaksanddata.arcane.framework.services.storage.models.base.StoredBlob
import com.sneaksanddata.arcane.microsoft_synapse_link.models.app.streaming.SourceCleanupResult
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
final class AzureBlobStorageReaderZIO(accountName: String, endpoint: Option[String], tokenCredential: Option[TokenCredential], sharedKeyCredential: Option[StorageSharedKeyCredential], settings: Option[AzureBlobStorageReaderSettings] = None):
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
  def getBlobContent[Result](blobPath: AdlsStoragePath, deserializer: Array[Byte] => Result = stringContentSerializer): Task[Reader] =
    val client = getBlobClient(blobPath)
    for
      _ <- ZIO.log("Downloading blob content from data file: " + blobPath.toHdfsPath)
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


  def getFirstBlob(storagePath: AdlsStoragePath): Task[OffsetDateTime] =
    streamPrefixes(storagePath + "/").runFold(OffsetDateTime.now(ZoneOffset.UTC)){ (date, blob) =>
      val current = interpretAsDate(blob).getOrElse(date)
      if current.isBefore(date) then current else date
    }

  private def interpretAsDate(blob: StoredBlob): Option[OffsetDateTime] =
    val name = blob.name.replaceAll("/$", "")
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ssX")
    Try(OffsetDateTime.parse(name, formatter)).toOption

  def getRootPrefixes(storagePath: AdlsStoragePath, lookBackInterval: Duration): ZStream[Any, Throwable, StoredBlob] =
    for startFrom <- ZStream.succeed(OffsetDateTime.now(ZoneOffset.UTC).minus(lookBackInterval))
        _ <- ZStream.log("Getting root prefixes stating from " + startFrom)
        prefixes <- ZStream.fromZIO(ZIO.attemptBlocking { streamPrefixes(storagePath) })
        zippedWithDate <- prefixes.map(blob => (interpretAsDate(blob), blob))
        eligibleToProcess <- zippedWithDate match
          case (Some(date), blob) if date.isAfter(startFrom) => ZStream.succeed(blob)
          case _ => ZStream.empty
    yield eligibleToProcess
    
  def deleteSourceFile(fileName: String): ZIO[Any, Throwable, SourceCleanupResult] =
    for
      _ <- ZIO.log("Deleting source file: " + fileName)
      success <- ZIO.attemptBlocking {
        serviceClient.getBlobContainerClient(fileName).getBlobClient(fileName).deleteIfExists()
      }
    yield SourceCleanupResult(fileName, success)

object AzureBlobStorageReaderZIO:
  /**
   * Create AzureBlobStorageReaderZIO for the account using TokenCredential
   * @param accountName Storage account name
   * @param credential TokenCredential (accessToken provider)
   * @return AzureBlobStorageReaderZIO instance
   */
  def apply(accountName: String, credential: TokenCredential): AzureBlobStorageReaderZIO = new AzureBlobStorageReaderZIO(accountName, None, Some(credential), None)

  /**
   * Create AzureBlobStorageReaderZIO for the account using StorageSharedKeyCredential
   *
   * @param accountName Storage account name
   * @param credential  StorageSharedKeyCredential (account key)
   * @return AzureBlobStorageReaderZIO instance
   */
  def apply(accountName: String, credential: StorageSharedKeyCredential): AzureBlobStorageReaderZIO = new AzureBlobStorageReaderZIO(accountName, None, None, Some(credential))

  /**
   * Create AzureBlobStorageReaderZIO for the account using StorageSharedKeyCredential and custom endpoint
   *
   * @param accountName Storage account name
   * @param endpoint Storage account endpoint                    
   * @param credential  StorageSharedKeyCredential (account key)
   * @return AzureBlobStorageReaderZIO instance
   */
  def apply(accountName: String, endpoint: String, credential: StorageSharedKeyCredential): AzureBlobStorageReaderZIO = new AzureBlobStorageReaderZIO(accountName, Some(endpoint), None, Some(credential))

  /**
   * Create AzureBlobStorageReaderZIO for the account using default credential chain
   *
   * @param accountName Storage account name
   * @return AzureBlobStorageReaderZIO instance
   */
  def apply(accountName: String): AzureBlobStorageReaderZIO = new AzureBlobStorageReaderZIO(accountName, None, None, None)
