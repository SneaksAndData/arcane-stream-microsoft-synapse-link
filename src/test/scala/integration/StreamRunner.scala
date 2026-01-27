package com.sneaksanddata.arcane.microsoft_synapse_link
package integration

import common.{Common, TimeLimitLifetimeService}
import integration.Fixtures.formatter
import models.app.MicrosoftSynapseLinkStreamContext
import models.app.contracts.StreamSpec

import org.scalatest.matchers.should.Matchers.should
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, ZIO, ZLayer}

import java.time.{Duration, Instant, OffsetDateTime, ZoneOffset}
import scala.language.postfixOps

object StreamRunner extends ZIOSpecDefault:

  private val sourceTableName = "StreamRunner"
  private val targetTableName = "iceberg.test.stream_dimensionattributelevelvalue"

  private val streamContextStr = s"""
    |
    | {
    |  "backfillBehavior": "overwrite",
    |  "backfillStartDate": "2026-01-01T00.00.00Z",
    |  "groupingIntervalSeconds": 1,
    |  "lookBackInterval": 21000,
    |  "tableProperties": {
    |    "partitionExpressions": [],
    |    "format": "PARQUET",
    |    "sortedBy": [],
    |    "parquetBloomFilterColumns": []
    |  },
    |  "rowsPerGroup": 1000,
    |  "sinkSettings": {
    |    "optimizeSettings": {
    |      "batchThreshold": 60,
    |      "fileSizeThreshold": "512MB"
    |    },
    |    "orphanFilesExpirationSettings": {
    |      "batchThreshold": 60,
    |      "retentionThreshold": "6h"
    |    },
    |    "snapshotExpirationSettings": {
    |      "batchThreshold": 60,
    |      "retentionThreshold": "6h"
    |    },
    |    "analyzeSettings": {
    |      "batchThreshold": 60,
    |      "includedColumns": []
    |    },
    |    "targetTableName": "$targetTableName"
    |  },
    |  "sourceSettings": {
    |    "baseLocation": "abfss://cdm-e2e@devstoreaccount1.dfs.core.windows.net/",
    |    "changeCaptureIntervalSeconds": 300,
    |    "name": "dimensionattributelevelvalue"
    |   },
    |  "stagingDataSettings": {
    |    "catalog": {
    |      "catalogName": "iceberg",
    |      "catalogUri": "http://localhost:20001/catalog",
    |      "schemaName": "test",
    |      "warehouse": "demo"
    |    },
    |    "maxRowsPerFile": 1,
    |    "tableNamePrefix": "staging_dimensionattributelevelvalue"
    |  },
    |  "fieldSelectionRule": {
    |    "ruleType": "all",
    |    "fields": []
    |  }
    |}
    |
    |""".stripMargin

  private val parsedSpec = StreamSpec.fromString(streamContextStr)

  private val streamingStreamContext = new MicrosoftSynapseLinkStreamContext(parsedSpec):
    override val IsBackfilling: Boolean = false

  private val backfillStreamContext = new MicrosoftSynapseLinkStreamContext(parsedSpec):
    override val IsBackfilling: Boolean = true

  private val streamingStreamContextLayer = ZLayer.succeed[MicrosoftSynapseLinkStreamContext](streamingStreamContext)
    ++ ZLayer.succeed(DatagramSocketConfig("/var/run/datadog/dsd.socket"))
    ++ ZLayer.succeed(MetricsConfig(Duration.ofMillis(100)))
    ++ ZLayer.succeed(DatadogPublisherConfig())

  private val backfillStreamContextLayer = ZLayer.succeed[MicrosoftSynapseLinkStreamContext](backfillStreamContext)
    ++ ZLayer.succeed(DatagramSocketConfig("/var/run/datadog/dsd.socket"))
    ++ ZLayer.succeed(MetricsConfig(Duration.ofMillis(100)))
    ++ ZLayer.succeed(DatadogPublisherConfig())

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("StreamRunner")(
    test("backfill, stream, backfill and stream again successfully") {
      for
        _         <- Fixtures.clearTarget(targetTableName)
        _         <- Fixtures.clearSource
        startTime <- ZIO.succeed(OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC))
        // Upload 2 batches and backfill the table
        _ <- ZIO.foreach(1 to 2) { index =>
          if index == 1 then Fixtures.uploadBatch(startTime.minusHours(index), false, true, false)
          else Fixtures.uploadBatch(startTime.minusHours(index), false, false, false)
        }
        backfillRunner <- Common.buildTestApp(TimeLimitLifetimeService.layer, backfillStreamContextLayer).fork
        result         <- backfillRunner.join.timeout(Duration.ofSeconds(30)).exit

        backfilledCount <- Common
          .getData(
            backfillStreamContext.targetTableFullName,
            "Id, versionnumber",
            Common.StrStrDecoder
          )
          .map(_.size)

        backfilledWatermark <- Common.getWatermark(backfillStreamContext.targetTableFullName.split('.').last)

        streamingRunner <- Common.buildTestApp(TimeLimitLifetimeService.layer, streamingStreamContextLayer).fork
        // drop some updates + inserts. 2 new rows should be inserted, row with id = 5b4bc74e-2132-4d8e-8572-48ce4260f182 - updated
        _ <- Fixtures.uploadBatch(startTime.minusMinutes(15), true, false, true)
        // drop no-op updates and a DELETE. Record with id = 50bff458-d47a-4924-804b-31c0a83108e6 should disappear
        _ <- Fixtures.uploadBatch(startTime.minusMinutes(10), true, false, false)
        // expected rows then will be initial 5 + 0 - 1 + 2
        // drop some no-op updates and stamp the new change log. This will be merged without actual updates
        _ <- Fixtures.uploadBatch(startTime.minusMinutes(5), false, true, false)

        streamingResult <- streamingRunner.join.timeout(Duration.ofSeconds(30)).exit

        currentRows <- Common.getData(
          streamingStreamContext.targetTableFullName,
          "Id, versionnumber",
          Common.StrStrDecoder
        )

        streamedWatermark <- Common.getWatermark(streamingStreamContext.targetTableFullName.split('.').last)

      // TODO: verify watermarks
      yield assertTrue(result.isSuccess) implies assertTrue(backfilledCount == 5) implies assertTrue(
        backfilledWatermark.version == s"${formatter.format(startTime.minusHours(1))}Z"
      ) implies assertTrue(currentRows.size == 5 - 1 + 2) implies assertTrue(
        !currentRows.exists(_._1 == "50bff458-d47a-4924-804b-31c0a83108e6")
      ) implies assertTrue(
        currentRows.find(_._1 == "5b4bc74e-2132-4d8e-8572-48ce4260f182").map(_._2).getOrElse("") == "2111000012"
      ) implies assertTrue(
        streamedWatermark.version == s"${formatter.format(startTime.minusMinutes(5))}Z"
      )
    }
  ) @@ timeout(zio.Duration.fromSeconds(180)) @@ TestAspect.withLiveClock
