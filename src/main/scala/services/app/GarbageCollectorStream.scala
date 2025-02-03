package com.sneaksanddata.arcane.microsoft_synapse_link
package services.app

import models.app.contracts.GarbageCollectorSettings
import services.data_providers.microsoft_synapse_link.AzureBlobStorageReaderZIO
import services.data_providers.microsoft_synapse_link.AzureBlobStorageReaderZIO.iterateByDates

import com.sneaksanddata.arcane.framework.services.cdm.CdmTableSettings
import com.sneaksanddata.arcane.framework.services.storage.models.azure.AdlsStoragePath
import zio.{Task, ZIO, ZLayer}
import zio.stream.ZStream

trait GarbageCollectorStream:
  def run: Task[Unit]



class AzureBlobStorageGarbageCollector(storageService: AzureBlobStorageReaderZIO, settings: GarbageCollectorSettings)
  extends GarbageCollectorStream:

  def run: Task[Unit] =
    val rootPath = AdlsStoragePath(settings.rootPath).get
    ZStream.succeed(rootPath)
      .mapZIO(path => storageService.getFirstBlob(path))
      .flatMap(startDate => {
        ZStream.fromIterable(Some(startDate).iterateByDates())
          .flatMap(prefix => storageService.streamPrefixes(rootPath + prefix))
          .flatMap(prefix => storageService.streamPrefixes(rootPath + prefix.name + "/"))
      })
      .foreach(blob => ZIO.log(s"Deleting blob $blob") *> ZIO.unit)

object AzureBlobStorageGarbageCollector:
  def apply(storageService: AzureBlobStorageReaderZIO, settings: GarbageCollectorSettings): AzureBlobStorageGarbageCollector =
    new AzureBlobStorageGarbageCollector(storageService, settings)

  private type Environment = AzureBlobStorageReaderZIO & GarbageCollectorSettings

  val layer: ZLayer[Environment, Nothing, GarbageCollectorStream] =
    ZLayer {
      for
        storageService <- ZIO.service[AzureBlobStorageReaderZIO]
        settings <- ZIO.service[GarbageCollectorSettings]
      yield AzureBlobStorageGarbageCollector(storageService, settings)
    }
