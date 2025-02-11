package com.sneaksanddata.arcane.microsoft_synapse_link
package services.data_providers.microsoft_synapse_link

import com.sneaksanddata.arcane.framework.models.ArcaneSchema
import com.sneaksanddata.arcane.framework.models.cdm.{SimpleCdmEntity, SimpleCdmModel, given_Conversion_SimpleCdmEntity_ArcaneSchema}
import com.sneaksanddata.arcane.framework.services.base.SchemaProvider
import com.sneaksanddata.arcane.framework.services.cdm.CdmTableSettings
import com.sneaksanddata.arcane.framework.services.mssql.given_CanAdd_ArcaneSchema
import com.sneaksanddata.arcane.framework.services.storage.models.azure.AzureBlobStorageReader

import zio.{ZIO, ZLayer}

import scala.concurrent.Future

class CdmSchemaProvider(azureBlobStorageReader: AzureBlobStorageReader, tableLocation: String, tableName: String)
  extends SchemaProvider[ArcaneSchema]:

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  override lazy val getSchema: Future[SchemaType] = getEntity.flatMap(toArcaneSchema)

  def getEntity: Future[SimpleCdmEntity] =
    SimpleCdmModel(tableLocation, azureBlobStorageReader).flatMap(_.entities.find(_.name == tableName) match
      case None => Future.failed(new Exception(s"Table $tableName not found in model $tableLocation"))
      case Some(entity) => Future.successful(entity)
    )

  override def empty: SchemaType = ArcaneSchema.empty()
  
  private def toArcaneSchema(simpleCdmModel: SimpleCdmEntity): Future[ArcaneSchema] = Future.successful(simpleCdmModel)

object CdmSchemaProvider:
  
  private type Environment = AzureBlobStorageReader & CdmTableSettings
  
  val layer: ZLayer[Environment, Nothing, CdmSchemaProvider] = 
      ZLayer {
        for
          context <- ZIO.service[CdmTableSettings]
          settings <- ZIO.service[AzureBlobStorageReader]
        yield CdmSchemaProvider(settings, context.rootPath, context.name)
      }
