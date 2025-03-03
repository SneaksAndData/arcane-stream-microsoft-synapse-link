package com.sneaksanddata.arcane.microsoft_synapse_link
package services.app

import models.app.contracts.GarbageCollectorSettings

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.services.storage.models.azure.AdlsStoragePath
import com.sneaksanddata.arcane.framework.services.storage.services.AzureBlobStorageReader
import com.sneaksanddata.arcane.framework.services.cdm.AzureBlobStorageReaderExtensions.getFirstDropDate
import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

trait GarbageCollectorStream:
  def run: Task[Unit]



class AzureBlobStorageGarbageCollector(storageService: AzureBlobStorageReader, settings: GarbageCollectorSettings)
  extends GarbageCollectorStream:

  private def deleteByDeleteMarkers(rootPath: AdlsStoragePath): Task[Unit] =
    for _ <- zlog("Deleting the source files marked for deletion")
        startDate <- storageService.getFirstDropDate(rootPath)
        _ <- zlog(s"First blob date: $startDate. Delete limit: ${settings.deleteLimit}")
        _ <- ZStream.fromIterable(Some(startDate).iterateByDates(Some(settings.deleteLimit)))
        .flatMap(prefix => storageService.streamPrefixes(rootPath + prefix))
        .flatMap(prefix => storageService.streamPrefixes(rootPath + prefix.name))
        .filterZIO(prefix => {
          for
            contents <- storageService.streamPrefixes(rootPath + prefix.name).runCollect
            groups = contents.groupBy(_.name.endsWith(AzureBlobStorageReaderZIO.deleteSuffix))
            filesCount = groups.getOrElse(false, Seq.empty).length
            deleteMarkersCount = groups.getOrElse(true, Seq.empty).length
            needDelete = filesCount == deleteMarkersCount
            _ <- zlog(s"Directory prefix: ${prefix.name} will be deleted: $needDelete. Files: $filesCount, delete markers: $deleteMarkersCount")
          yield needDelete
        })
        .runForeach(prefix => {
          val path = rootPath + prefix.name
          for filesToDelete <- storageService.streamPrefixes(path).runCollect
              _ <- ZIO.foreachDiscard(filesToDelete)(file => storageService.deleteBlob(AdlsStoragePath(path.accountName, path.container, file.name)))
              _ <- storageService.deleteBlob(path).map(result => zlog(s"Source directory $path was deleted: $result"))
          yield ()
        })
    yield ()

  private val ignoredFiles = Set("model.json", "Microsoft.Athena.TrickleFeedService/", "OptionsetMetadata/")
  private def deleteEmptyFolders(rootPath: AdlsStoragePath): Task[Unit] =
    for startDate <- storageService.getFirstDropDate(rootPath)
        _ <- ZStream.fromIterable(Some(startDate).iterateByDates())
          .flatMap(prefix => storageService.streamPrefixes(rootPath + prefix))
          .filterZIO(prefix => {
            for
              contents <- storageService.streamPrefixes(rootPath + prefix.name)
                .filter(f => !ignoredFiles.exists(e => f.name.endsWith(e))).runCollect

              _ <- zlog(s"Directory prefix: $prefix has ${contents.length} files (not included: $ignoredFiles)")
            yield contents.isEmpty
          })
          .runForeach(prefix => zlog(s"Deleting empty prefix $prefix") *> deleteFolderRecursively(rootPath + prefix.name))
    yield ()


  private def deleteFolderRecursively(blob: AdlsStoragePath): Task[Unit] =
    if ! blob.blobPrefix.endsWith("/")
    then
      storageService.breakLease(blob) *> storageService.deleteBlob(blob).map(_ => ())
    else
      for
        _ <- zlog(s"Deleting folder $blob")
        _ <- storageService.streamPrefixes(blob)
            .mapZIO(prefix => deleteFolderRecursively(blob.copy(blobPrefix = prefix.name)))
            .runCollect
        _ <- storageService.breakLease(blob)
        _ <- storageService.deleteBlob(blob)
      yield ()

  def run: Task[Unit] =
      val rootPath = AdlsStoragePath(settings.rootPath).get
      for _ <- zlog(s"root path: $rootPath")
          _ <- deleteByDeleteMarkers(rootPath)
          _ <- deleteEmptyFolders(rootPath)
      yield ()

object AzureBlobStorageGarbageCollector:
  def apply(storageService: AzureBlobStorageReader, settings: GarbageCollectorSettings): AzureBlobStorageGarbageCollector =
    new AzureBlobStorageGarbageCollector(storageService, settings)

  private type Environment = AzureBlobStorageReader & GarbageCollectorSettings

  val layer: ZLayer[Environment, Nothing, GarbageCollectorStream] =
    ZLayer {
      for
        storageService <- ZIO.service[AzureBlobStorageReader]
        settings <- ZIO.service[GarbageCollectorSettings]
      yield AzureBlobStorageGarbageCollector(storageService, settings)
    }
