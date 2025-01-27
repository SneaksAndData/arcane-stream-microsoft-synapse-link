package com.sneaksanddata.arcane.microsoft_synapse_link
package services

import services.graph_builder.{BackfillDataGraphBuilder, VersionedDataGraphBuilder}

import com.sneaksanddata.arcane.framework.models.DataRow
import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.services.streaming.base.StreamGraphBuilder
import zio.{ZIO, ZLayer}

import java.time.OffsetDateTime

/**
 * Provides a layer that injects a stream graph builder resolved based on the stream context at runtime.
 */
object StreamGraphBuilderFactory:

  private type Environment = StreamContext
    & VersionedDataGraphBuilder.Environment

  val layer: ZLayer[Environment, Nothing, VersionedDataGraphBuilder] = ZLayer.fromZIO(getGraphBuilder)

  private def getGraphBuilder =
    for
      context <- ZIO.service[StreamContext]
      _ <- ZIO.log("Start the graph builder type resolution")
      builder <- VersionedDataGraphBuilder.layer
    yield builder

