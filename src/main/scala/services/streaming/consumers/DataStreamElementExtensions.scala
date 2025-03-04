package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.consumers

import services.data_providers.microsoft_synapse_link.DataStreamElement

import com.sneaksanddata.arcane.framework.models.DataRow
import com.sneaksanddata.arcane.framework.services.streaming.base.MetadataEnrichedRowStreamElement

object DataStreamElementExtensions:
  
  given MetadataEnrichedRowStreamElement[DataStreamElement] with
    extension (element: DataStreamElement)
      def isDataRow: Boolean = element.isInstanceOf[DataRow]
      def toDataRow: DataRow = element.asInstanceOf[DataRow]
      
    extension (a: DataRow) def fromDataRow: DataStreamElement = a
  
