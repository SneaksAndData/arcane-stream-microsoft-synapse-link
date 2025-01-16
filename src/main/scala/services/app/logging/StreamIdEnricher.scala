package com.sneaksanddata.arcane.microsoft_synapse_link
package services.app.logging

import com.sneaksanddata.arcane.framework.services.app.logging.SequenceEnricher
import com.sneaksanddata.arcane.framework.services.app.logging.base.Enricher
import upickle.default.*

import scala.annotation.targetName

class StreamIdEnricher extends Enricher:
  private lazy val streamClass = sys.env.get("STREAMCONTEXT__STREAM_ID")

  override def enrichLoggerWith(target: (String, String) => Unit): Unit =
    streamClass.foreach(v => target("streamId", v))

  @targetName("append")
  override def ++(other: Enricher): Enricher = SequenceEnricher(Seq(this, other))

object StreamIdEnricher:

  def apply: JsonEnvironmentEnricher = new StreamIdEnricher
