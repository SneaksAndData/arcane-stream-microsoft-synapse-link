package com.sneaksanddata.arcane.microsoft_synapse_link
package models.app.contracts

import models.app.{AzureConnectionSettings, GraphExecutionSettings}

import zio.{System, ZIO, ZLayer}

trait GarbageCollectorSettings:
  val rootPath: String


class EnvironmentGarbageCollectorSettings(override val rootPath: String, override val sourceDeleteDryRun: Boolean = true)
  extends GarbageCollectorSettings with GraphExecutionSettings with AzureConnectionSettings:
  
  override val endpoint: String = sys.env("ARCANE_FRAMEWORK__STORAGE_ENDPOINT")
  override val container: String = sys.env("ARCANE_FRAMEWORK__STORAGE_CONTAINER")
  override val account: String = sys.env("ARCANE_FRAMEWORK__STORAGE_ACCOUNT")
  override val accessKey: String = sys.env("ARCANE_FRAMEWORK__STORAGE_ACCESS_KEY")


object EnvironmentGarbageCollectorSettings:
  def apply(rootPath: String): EnvironmentGarbageCollectorSettings = new EnvironmentGarbageCollectorSettings(rootPath)

  val layer: ZLayer[Any, SecurityException, GarbageCollectorSettings & GraphExecutionSettings & AzureConnectionSettings] =
    ZLayer {
      for rootPath <- System.env("ARCANE_GARBAGE_COLLECTOR_ROOT_PATH")
      yield rootPath match
        case Some(value) => EnvironmentGarbageCollectorSettings(value)
        case None => throw new Exception("ARCANE_GARBAGE_COLLECTOR_ROOT_PATH is not set")
    }
