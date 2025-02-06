package com.sneaksanddata.arcane.microsoft_synapse_link
package extensions

import com.sneaksanddata.arcane.framework.models.{ArcaneSchema, ArcaneSchemaField}

object ArcaneSchemaExtensions:

  extension (targetSchema: ArcaneSchema) def getMissingFields(batches: ArcaneSchema): Seq[ArcaneSchemaField] =
    batches.filter { batchField =>
      !targetSchema.exists(targetField => targetField.name.toLowerCase() == batchField.name.toLowerCase()
        && targetField.fieldType == batchField.fieldType)
    }

