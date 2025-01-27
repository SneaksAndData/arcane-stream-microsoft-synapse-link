package com.sneaksanddata.arcane.microsoft_synapse_link
package models.app.streaming

case class SourceCleanupRequest(prefix: String)
case class SourceCleanupResult(prefix: String, success: Boolean)
