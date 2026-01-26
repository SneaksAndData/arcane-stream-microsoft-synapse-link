package com.sneaksanddata.arcane.microsoft_synapse_link
package integration

import com.azure.core.util.BinaryData
import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.common.StorageSharedKeyCredential
import com.sneaksanddata.arcane.framework.services.storage.services.azure.AzureBlobStorageReader
import zio.stream.ZStream
import zio.{Task, ZIO}

import java.sql.{Connection, DriverManager}
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import scala.util.Random

object Fixtures:
  private val azuriteCredential = new StorageSharedKeyCredential("devstoreaccount1", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
  private val serviceClient = new BlobServiceClientBuilder().credential(azuriteCredential).endpoint("http://localhost:10001/devstoreaccount1").buildClient()
  private val containerClient = serviceClient.getBlobContainerClient("cdm-e2e")

  val trinoConnectionString: String = sys.env("ARCANE_FRAMEWORK__MERGE_SERVICE_CONNECTION_URI")
  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ssX")
  
  def clearTarget(targetFullName: String): Unit =
    val trinoConnection = DriverManager.getConnection(trinoConnectionString)
    val query           = s"drop table if exists $targetFullName"
    val statement       = trinoConnection.createStatement()
    statement.executeUpdate(query)
    
  def uploadBatch(timestamp: OffsetDateTime, addDelete: Boolean): Task[Unit] =
    for
      batchFolderName <- ZIO.attempt(formatter.format(timestamp))
      batchContent <- ZIO.attempt(Random.shuffle(SynapseMetadata.fileContent.split("\n").toList).mkString("\n"))
      _ <- ZIO.attemptBlocking(containerClient.getBlobClient(s"$batchFolderName/model.json").upload(BinaryData.fromString(SynapseMetadata.modelJson)))
      _ <- ZIO.attemptBlocking(containerClient.getBlobClient(s"$batchFolderName/${SynapseMetadata.entityName}/${Random.between(2020, 2026).toString}").upload(BinaryData.fromString(batchContent)))
      _ <- ZIO.when(addDelete) {
        ZIO.attemptBlocking(containerClient.getBlobClient(s"$batchFolderName/${SynapseMetadata.entityName}/${Random.between(1, 10).toString}").upload(BinaryData.fromString(SynapseMetadata.deleteFileContent)))
      }
    yield ()