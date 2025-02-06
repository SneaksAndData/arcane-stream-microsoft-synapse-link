package com.sneaksanddata.arcane.microsoft_synapse_link
package models.app.streaming

import com.sneaksanddata.arcane.framework.services.storage.models.azure.AdlsStoragePath

case class SourceCleanupRequest(prefix: AdlsStoragePath)
case class SourceCleanupResult(blobName: AdlsStoragePath, deleteMarker: AdlsStoragePath)
