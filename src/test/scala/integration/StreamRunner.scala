package com.sneaksanddata.arcane.microsoft_synapse_link
package integration

import common.Common
import integration.Fixtures.formatter
import models.app.MicrosoftSynapseLinkPluginStreamContext

import com.sneaksanddata.arcane.framework.services.synapse.versioning.SynapseWatermark
import com.sneaksanddata.arcane.framework.testkit.verifications.FrameworkVerificationUtilities.{
  StrStrDecoder,
  clearTarget,
  getWatermark,
  readTarget
}
import com.sneaksanddata.arcane.framework.testkit.zioutils.ZKit.runOrFail
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

  private val targetTableName = "iceberg.test.stream_dimensionattributelevelvalue"

  private val streamContextStr =
    s"""
         {
       |  "backfillJobTemplateRef": {
       |    "apiGroup": "streaming.sneaksanddata.com",
       |    "kind": "StreamingJobTemplate",
       |    "name": "arcane-stream-synapse-large-job"
       |  },
       |  "jobTemplateRef": {
       |    "apiGroup": "streaming.sneaksanddata.com",
       |    "kind": "StreamingJobTemplate",
       |    "name": "arcane-stream-synapse-standard-job"
       |  },
       |  "observability": {
       |    "metricTags": {}
       |  },
       |  "staging": {
       |    "table": {
       |      "stagingTablePrefix": "staging_synapse_test",
       |      "maxRowsPerFile": 10000,
       |      "stagingCatalogName": "iceberg",
       |      "stagingSchemaName": "test",
       |      "isUnifiedSchema": false
       |    },
       |    "icebergCatalog": {
       |      "catalogProperties": {},
       |      "catalogUri": "http://localhost:20001/catalog",
       |      "namespace": "test",
       |      "warehouse": "demo",
       |      "maxCatalogInstanceLifetime": "3600 second"
       |    }
       |  },
       |  "streamMode": {
       |    "backfill": {
       |      "backfillBehavior": "Overwrite",
       |      "backfillStartDate": "2026-01-01T00:00:00Z"
       |    },
       |    "changeCapture": {
       |      "changeCaptureInterval": "5 second",
       |      "changeCaptureJitterVariance": 0.1,
       |      "changeCaptureJitterSeed": 0
       |    }
       |  },
       |  "sink": {
       |    "mergeServiceClient": {
       |      "extraConnectionParameters": {
       |        "clientTags": "test"
       |      },
       |      "queryRetryMode": "Never",
       |      "queryRetryBaseDuration": "100 millisecond",
       |      "queryRetryOnMessageContents": [],
       |      "queryRetryScaleFactor": 0.1,
       |      "queryRetryMaxAttempts": 3
       |    },
       |    "targetTableProperties": {
       |      "format": "PARQUET",
       |      "sortedBy": [],
       |      "parquetBloomFilterColumns": []
       |    },
       |    "targetTableFullName": "$targetTableName",
       |    "maintenanceSettings": {
       |      "targetOptimizeSettings": {
       |        "batchThreshold": 60,
       |        "fileSizeThreshold": "512MB"
       |      },
       |      "targetOrphanFilesExpirationSettings": {
       |        "batchThreshold": 60,
       |        "retentionThreshold": "6h"
       |      },
       |      "targetSnapshotExpirationSettings": {
       |        "batchThreshold": 60,
       |        "retentionThreshold": "6h"
       |      },
       |      "targetAnalyzeSettings": {
       |        "includedColumns": [],
       |        "batchThreshold": 60
       |      }
       |    },
       |    "icebergCatalog": {
       |      "catalogProperties": {},
       |      "catalogUri": "http://localhost:20001/catalog",
       |      "namespace": "test",
       |      "warehouse": "demo",
       |      "maxCatalogInstanceLifetime": "3600 second"
       |    }
       |  },
       |  "throughput": {
       |    "shaperImpl": {
       |      "memoryBound": {
       |        "meanStringTypeSizeEstimate": 50,
       |        "meanObjectTypeSizeEstimate": 4096,
       |        "burstEstimateDivisionFactor": 2,
       |        "rateEstimateDivisionFactor": 2,
       |        "chunkCostScale": 1,
       |        "chunkCostMax": 2,
       |        "tableRowCountWeight": 0.05,
       |        "tableSizeWeight": 0.05,
       |        "tableSizeScaleFactor": 1
       |      }
       |    },
       |    "advisedRatePeriod": "1 second",
       |    "advisedChunksBurst": 1,
       |    "advisedChunkSize": 1,
       |    "advisedRateChunks": 1
       |  },
       |  "source": {
       |    "configuration": {
       |      "entityName": "dimensionattributelevelvalue",
       |      "baseLocation": "abfss://cdm-e2e@devstoreaccount1.dfs.core.windows.net/",
       |      "storageConnection": {
       |        "accountName": "devstoreaccount1",
       |        "endpoint": "http://localhost:10001/devstoreaccount1",
       |        "httpClient": {
       |          "httpMaxRetries": 3,
       |          "httpMaxRetryDelay": "1 second",
       |          "httpMinRetryDelay": "100 millisecond",
       |          "httpRetryTimeout": "1 minute",
       |          "maxResultsPerPage": 10000
       |        },
       |        "credentialType": {
       |          "sharedKey": {
       |            "accessKey": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
       |          }
       |        }
       |      }
       |    },
       |    "buffering": {
       |      "enabled": false,
       |      "strategy": {}
       |    },
       |    "fieldSelectionRule": {
       |      "essentialFields": [],
       |      "rule":{
       |        "all": {}
       |      },
       |      "isServerSide": false
       |    }
       |  }
       |}""".stripMargin

  private val streamContext = MicrosoftSynapseLinkPluginStreamContext(streamContextStr)

  private val streamContextLayer = ZLayer.succeed[MicrosoftSynapseLinkPluginStreamContext](streamContext)

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("StreamRunner")(
    test("backfill and then stream changes successfully") {
      for
        _         <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL", "true")
        _         <- clearTarget(targetTableName)
        _         <- Fixtures.clearSource
        startTime <- ZIO.succeed(OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC))
        // Upload 2 batches and backfill the table
        _ <- ZIO.foreach(1 to 2) { index =>
          if index == 1 then Fixtures.uploadBatch(startTime.minusHours(index), false, true, false)
          else Fixtures.uploadBatch(startTime.minusHours(index), false, false, false)
        }
        backfillRunner <- Common.getTestApp(Duration.ofSeconds(45), streamContextLayer).fork
        _              <- backfillRunner.runOrFail(Duration.ofSeconds(30))

        backfilledCount <- readTarget(
          streamContext.sink.targetTableFullName,
          "Id, versionnumber",
          StrStrDecoder
        )
          .map(_.size)

        backfilledWatermark <- getWatermark(streamContext.sink.targetTableFullName.split('.').last)(
          SynapseWatermark.rw
        )

        _ <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL", "false")

        streamingRunner <- Common.getTestApp(Duration.ofSeconds(45), streamContextLayer).fork
        // drop some updates + inserts. 2 new rows should be inserted, row with id = 5b4bc74e-2132-4d8e-8572-48ce4260f182 - updated
        _ <- Fixtures.uploadBatch(startTime.minusMinutes(15), true, false, true)
        // drop no-op updates and a DELETE. Record with id = 50bff458-d47a-4924-804b-31c0a83108e6 should disappear
        _ <- Fixtures.uploadBatch(startTime.minusMinutes(10), true, false, false)
        // expected rows then will be initial 5 + 0 - 1 + 2
        // drop some no-op updates and stamp the new change log. This will be merged without actual updates
        _ <- Fixtures.uploadBatch(startTime.minusMinutes(5), false, true, false)

        _ <- streamingRunner.runOrFail(Duration.ofSeconds(30))

        currentRows <- readTarget(
          streamContext.sink.targetTableFullName,
          "Id, versionnumber",
          StrStrDecoder
        )

        streamedWatermark <- getWatermark(streamContext.sink.targetTableFullName.split('.').last)(
          SynapseWatermark.rw
        )
      yield assertTrue(backfilledCount == 5) implies assertTrue(
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
