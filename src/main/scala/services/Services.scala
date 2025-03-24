package com.sneaksanddata.arcane.microsoft_synapse_link
package services

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.services.lakehouse.IcebergS3CatalogWriter.toCatalogProperties
import com.sneaksanddata.arcane.framework.services.lakehouse.base.IcebergCatalogSettings
import org.apache.iceberg.rest.RESTCatalog
import zio.{ZIO, ZLayer}

import scala.jdk.CollectionConverters.*

/**
 * The object that contains ZIO layers for the third-party services.
 */
object Services:

  /**
   * The layer for the Azure Blob Storage Reader.
   */
  val restCatalog: ZLayer[IcebergCatalogSettings, Throwable, RESTCatalog] = ZLayer.scoped {
    for
      settings <- ZIO.service[IcebergCatalogSettings]
      catalog = RESTCatalog()
      initializedCatalog <- ZIO.fromAutoCloseable(initializeCatalog(catalog, settings))
    yield initializedCatalog
  }

  private def initializeCatalog(catalog: RESTCatalog, settings: IcebergCatalogSettings): ZIO[Any, Throwable, RESTCatalog] =
    val catalogName: String = java.util.UUID.randomUUID.toString
    for
      _ <- ZIO.attempt(catalog.initialize(catalogName, settings.toCatalogProperties.asJava))
      _ <- zlog("Catalog %s has been initialized", catalogName)
    yield catalog
