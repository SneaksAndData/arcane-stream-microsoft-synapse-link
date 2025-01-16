package com.sneaksanddata.arcane.microsoft_synapse_link
package services.app.logging
import com.sneaksanddata.arcane.framework.services.app.logging.SequenceEnricher
import upickle.default.*
import com.sneaksanddata.arcane.framework.services.app.logging.base.Enricher

import scala.annotation.targetName

/**
 * Enricher that reads a JSON object from an environment variable and adds its key-value pairs to the logger.
 *
 * @param key The environment variable key
 */
class JsonEnvironmentEnricher(key: String) extends Enricher:
  private lazy val envDictionary: Map[String, String] = sys.env
    .get(key)
    .map(v => read[Map[String, String]](v))
    .getOrElse(Map.empty)

  override def enrichLoggerWith(target: (String, String) => Unit): Unit = envDictionary.foreach {
    case (k, v) => target(k, v)
  }

  @targetName("append")
  override def ++(other: Enricher): Enricher = SequenceEnricher(Seq(this, other))

/**
 * Companion object for the JsonEnvironmentEnricher class.
 */
object JsonEnvironmentEnricher:

  /**
   * Factory method to create a JsonEnvironmentEnricher.
   *
   * @param key The environment variable key
   * @return The initialized JsonEnvironmentEnricher instance
   */
  def apply(key: String): JsonEnvironmentEnricher = new JsonEnvironmentEnricher(key)
