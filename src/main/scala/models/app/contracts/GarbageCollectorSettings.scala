package com.sneaksanddata.arcane.microsoft_synapse_link
package models.app.contracts

import models.app.{AzureConnectionSettings, GraphExecutionSettings}

import zio.{System, ZIO, ZLayer}

import java.time.{OffsetDateTime, ZoneOffset}

trait GarbageCollectorSettings:
  val rootPath: String
  val deleteLimit: OffsetDateTime


class EnvironmentGarbageCollectorSettings(override val rootPath: String,
                                           override val deleteLimit: OffsetDateTime,
                                          override val sourceDeleteDryRun: Boolean)
  extends GarbageCollectorSettings with GraphExecutionSettings with AzureConnectionSettings:
  
  override val endpoint: String = sys.env("ARCANE_FRAMEWORK__STORAGE_ENDPOINT")
  override val container: String = sys.env("ARCANE_FRAMEWORK__STORAGE_CONTAINER")
  override val account: String = sys.env("ARCANE_FRAMEWORK__STORAGE_ACCOUNT")
  override val accessKey: String = sys.env("ARCANE_FRAMEWORK__STORAGE_ACCESS_KEY")


object EnvironmentGarbageCollectorSettings:
  def apply(rootPath: String, deleteLimit: OffsetDateTime, sourceDeleteDryRun: Boolean): EnvironmentGarbageCollectorSettings =
    new EnvironmentGarbageCollectorSettings(rootPath, deleteLimit, sourceDeleteDryRun)

  val layer: ZLayer[Any, SecurityException, GarbageCollectorSettings & GraphExecutionSettings & AzureConnectionSettings] =
    ZLayer {
      for rootPath <- System.env("ARCANE_GARBAGE_COLLECTOR_ROOT_PATH")
          deleteLimit <- System.env("ARCANE_GARBAGE_COLLECTOR_DELETE_LIMIT")
          sourceDeleteDryRun = sys.env.get("ARCANE_FRAMEWORK__SOURCE_DELETE_DRY_RUN").exists(v => v.toLowerCase == "true")
      yield (rootPath, deleteLimit) match
        case (Some(rp), Some(dt)) =>
          EnvironmentGarbageCollectorSettings(rp, OffsetDateTime.now(ZoneOffset.UTC).minusHours(dt.toLong), sourceDeleteDryRun)

        case (None, _) => throw new Exception("ARCANE_GARBAGE_COLLECTOR_ROOT_PATH is not set")
        case (_, None) => throw new Exception("ARCANE_GARBAGE_COLLECTOR_DELETE_LIMIT is not set")
    }
