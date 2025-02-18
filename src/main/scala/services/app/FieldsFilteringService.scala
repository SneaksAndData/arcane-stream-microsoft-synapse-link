package com.sneaksanddata.arcane.microsoft_synapse_link
package services.app

import com.sneaksanddata.arcane.framework.models.{ArcaneSchema, DataCell, DataRow}
import com.sneaksanddata.arcane.framework.models.settings.{FieldSelectionRule, FieldSelectionRuleSettings}
import zio.{ZIO, ZLayer}

class FieldsFilteringService(fieldSelectionRule: FieldSelectionRuleSettings):

  def filter(schema: ArcaneSchema): ArcaneSchema = fieldSelectionRule.rule match
    case includeFields: FieldSelectionRule.IncludeFields => schema.filter(entry => includeFields.fields.contains(entry.name.toLowerCase()))
    case excludeFields: FieldSelectionRule.ExcludeFields  => schema.filter(entry => !excludeFields.fields.contains(entry.name.toLowerCase()))
    case _ => schema

  def filter(row: DataRow): DataRow = fieldSelectionRule.rule match
    case includeFields: FieldSelectionRule.IncludeFields => row.filter(entry => includeFields.fields.contains(entry.name.toLowerCase()))
    case excludeFields: FieldSelectionRule.ExcludeFields => row.filter(entry => !excludeFields.fields.contains(entry.name.toLowerCase()))
    case _ => row

object FieldsFilteringService:
  type Environment = FieldSelectionRuleSettings

  def apply(fieldSelectionRule: FieldSelectionRuleSettings): FieldsFilteringService = new FieldsFilteringService(fieldSelectionRule)

  /**
   * The ZLayer that creates the IcebergConsumer.
   */
  val layer: ZLayer[Environment, Nothing, FieldsFilteringService] =
    ZLayer {
      for fieldSelectionRule <- ZIO.service[FieldSelectionRuleSettings]
        yield FieldsFilteringService(fieldSelectionRule)
    }
