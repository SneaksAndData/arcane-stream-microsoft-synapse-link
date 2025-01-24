package com.sneaksanddata.arcane.microsoft_synapse_link
package services.app.logging

import com.sneaksanddata.arcane.framework.services.app.logging.SequenceEnricher
import com.sneaksanddata.arcane.framework.services.app.logging.base.Enricher
import upickle.default.*

import scala.annotation.targetName

/**
 * Enricher that reads a JSON object from an environment variable and adds its key-value pairs to the logger.
 *
 * @param key The environment variable key
 */
class StreamKindEnricher extends Enricher:
  private lazy val streamClass = sys.env.get("STREAMCONTEXT__STREAM_KIND")

  override def enrichLoggerWith(target: (String, String) => Unit): Unit =
    streamClass.foreach(v => target("streamKind", v))

  @targetName("append")
  override def ++(other: Enricher): Enricher = SequenceEnricher(Seq(this, other))

/**
 * Companion object for the JsonEnvironmentEnricher class.
 */
object StreamKindEnricher:

  /**
   * Factory method to create a JsonEnvironmentEnricher.
   *
   * @param key The environment variable key
   * @return The initialized JsonEnvironmentEnricher instance
   */
  def apply: StreamKindEnricher = new StreamKindEnricher
