package com.sneaksanddata.arcane.microsoft_synapse_link
package common

import com.sneaksanddata.arcane.framework.services.app.base.{InterruptionToken, StreamLifetimeService}
import zio.{ULayer, ZLayer}

import java.time.Duration

/** A lifetime service that cancels the stream after a certain amount of time.
  * @param timeLimit
  *   The time limit after which the stream will be cancelled
  */
class TimeLimitLifetimeService(timeLimit: Duration) extends StreamLifetimeService with InterruptionToken:

  @volatile
  private var isCancelled = false
  private val startTime   = System.currentTimeMillis()

  /** @inheritdoc
    */
  override def cancelled: Boolean = startTime + timeLimit.toMillis < System.currentTimeMillis() || this.isCancelled

  /** @inheritdoc
    */
  override def cancel(): Unit = this.isCancelled = true

  /** @inheritdoc
    */
  override def start(): Unit = {
    /* does nothing */
  }

  /** @inheritdoc
    */
  override def interrupted: Boolean = isCancelled

object TimeLimitLifetimeService:
  /** Creates a new time limit lifetime service.
    * @param timeLimit
    *   The time limit after which the stream will be cancelled
    * @return
    *   The time limit lifetime service
    */
  def apply(timeLimit: Duration): TimeLimitLifetimeService = new TimeLimitLifetimeService(timeLimit)

  /** The ZLayer for the time limit lifetime service. Uses a time limit of 10 seconds.
    * @return
    *   The ZLayer for the time limit lifetime service
    */
  val layer: ULayer[TimeLimitLifetimeService] = ZLayer.succeed(TimeLimitLifetimeService(Duration.ofSeconds(110)))
