package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.processors

import com.sneaksanddata.arcane.framework.models.{ArcaneSchema, DataRow}
import com.sneaksanddata.arcane.framework.models.settings.{FieldSelectionRule, FieldSelectionRuleSettings}
import com.sneaksanddata.arcane.framework.services.base.SchemaProvider
import com.sneaksanddata.arcane.framework.services.streaming.base.BatchProcessor
import zio.{ZIO, ZLayer}
import zio.stream.ZPipeline
import services.data_providers.microsoft_synapse_link.DataStreamElement

import com.sneaksanddata.arcane.microsoft_synapse_link.services.app.FieldsFilteringService

/**
 * The batch processor implementation that filters the fields of a DataRow.
 *
 * @param fieldSelectionRule The field selection rule.
 */
class FieldFilteringProcessor(fieldsFilteringService: FieldsFilteringService) extends BatchProcessor[DataStreamElement, DataStreamElement]:

  def process: ZPipeline[Any, Throwable, DataStreamElement, DataStreamElement] = ZPipeline.map {
    case row: DataRow => fieldsFilteringService.filter(row)
    case other => other
  }

object FieldFilteringProcessor:
  type Environment = FieldsFilteringService

  def apply(fieldSelectionRule: FieldsFilteringService): FieldFilteringProcessor = new FieldFilteringProcessor(fieldSelectionRule)

  /**
   * The ZLayer that creates the IcebergConsumer.
   */
  val layer: ZLayer[Environment, Nothing, FieldFilteringProcessor] =
    ZLayer {
      for fieldSelectionRule <- ZIO.service[FieldsFilteringService]
      yield FieldFilteringProcessor(fieldSelectionRule)
    }
