package com.sneaksanddata.arcane.microsoft_synapse_link
package extensions

import com.sneaksanddata.arcane.framework.models.{ArcaneSchema, DataRow, DatePartitionField, Field, MergeKeyField}

object DataRowExtensions:

  /**
   * Extension method to get the schema of a DataRow.
   */
  extension (row: DataRow) def schema: ArcaneSchema =
    row.foldLeft(ArcaneSchema.empty()) {
      case (schema, cell) if cell.name == MergeKeyField.name => schema ++ Seq(MergeKeyField)
      case (schema, cell) if cell.name == DatePartitionField.name => schema ++ Seq(DatePartitionField)
      case (schema, cell) => schema ++ Seq(Field(cell.name, cell.Type))
    }

