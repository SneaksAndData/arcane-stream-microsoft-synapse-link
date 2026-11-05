package com.sneaksanddata.arcane.microsoft_synapse_link
package integration

import common.Common
import integration.Fixtures.formatter
import models.app.MicrosoftSynapseLinkPluginStreamContext

import com.sneaksanddata.arcane.framework.models.schemas.ArcaneType.{LongType, StringType}
import com.sneaksanddata.arcane.framework.models.schemas.{ArcaneSchema, IndexedField, IndexedMergeKeyField}
import com.sneaksanddata.arcane.framework.services.synapse.versioning.SynapseWatermark
import com.sneaksanddata.arcane.framework.testkit.setups.FrameworkTestSetup.prepareWatermark
import com.sneaksanddata.arcane.framework.testkit.verifications.FrameworkVerificationUtilities.{
  StrStrDecoder,
  clearTarget,
  getWatermark,
  readTarget
}
import com.sneaksanddata.arcane.framework.testkit.zioutils.ZKit.{liveSeed, runOrFail}
import com.sneaksanddata.arcane.framework.utils.HashUtils
import org.scalatest.matchers.should.Matchers.should
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, ZIO, ZLayer}

import java.sql.DriverManager
import java.time.{Duration, Instant, OffsetDateTime, ZoneOffset}
import scala.language.postfixOps
import scala.util.Random

object StreamRunner extends ZIOSpecDefault:

  private val targetTableName          = "iceberg.test.stream_dimensionattributelevelvalue"
  private val migrationTargetTableName = "iceberg.test.stream_dimensionattributelevelvalue_migration"

  private def getStreamContextStr(targetTableName: String) =
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
       |      "maxRowsPerFile": 10000,
       |      "stagingCatalogName": "iceberg",
       |      "stagingSchemaName": "test",
       |      "isUnifiedSchema": false
       |    },
       |    "icebergCatalog": {
       |      "catalogProperties": {},
       |      "catalogUri": "http://localhost:20001/catalog",
       |      "namespace": "test",
       |      "warehouse": "demo"
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
       |      "changeCaptureJitterSeed": 0,
       |      "changeCaptureRangeLimit": 10
       |    }
       |  },
       |  "sink": {
       |    "mergeServiceClient": {
       |      "connectionUrl": "jdbc:trino://localhost:8080",
       |      "credentialType": {
       |        "basic": {}
       |      },
       |      "extraConnectionParameters": {
       |        "clientTags": "test"
       |      },
       |      "queryRetryMode": {
       |        "never": {}
       |      },
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
       |      "warehouse": "demo"
       |    }
       |  },
       |  "throughput": {
       |    "shaperImpl": {
       |      "memoryBound": {
       |        "fallbackStringTypeSizeEstimate": 50,
       |        "objectTypeSizeEstimate": 4096,
       |        "chunkCostScale": 1,
       |        "chunkCostMax": 2,
       |        "tableRowCountWeight": 0.02,
       |        "tableSizeWeight": 0.02,
       |        "tableSizeScaleFactor": 1,
       |        "chunkSizeCap": 1000000,
       |        "maxStatisticsAge": 604800
       |      }
       |    },
       |    "advisedRate": "10000 per 15 second",
       |    "advisedBurst": 1000,
       |    "advisedChunkSize": 1
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
       |    "modifications": {"supported": {}}
       |  }
       |}""".stripMargin

  // `Id` is not emitted to the sink, it is only hashed into ARCANE_MERGE_KEY
  private def mergeKey(id: String): String = HashUtils.murmur3(id)

  private def getStreamContext(targetTableName: String) =
    MicrosoftSynapseLinkPluginStreamContext(getStreamContextStr(targetTableName))

  private def getStreamContextLayer(targetTableName: String) =
    val context = getStreamContext(targetTableName)
    ZLayer.succeed[MicrosoftSynapseLinkPluginStreamContext](context) ++ ZLayer
      .succeed[DatagramSocketConfig](context) ++ ZLayer
      .succeed[MetricsConfig](context) ++ ZLayer.succeed(DatadogPublisherConfig())

  // sink table layout produced before `Id` and `versionnumber` became technical fields
  private val legacySchema = ArcaneSchema(
    Seq(
      IndexedField(name = "Id", fieldType = StringType, fieldId = 1),
      IndexedField(name = "versionnumber", fieldType = LongType, fieldId = 2),
      IndexedMergeKeyField(3)
    )
  )

  private val legacyRows = List(("legacy-id-1", 111L), ("legacy-id-2", 222L))

  private def seedLegacyRows(table: String) = ZIO.scoped {
    for
      connection <- ZIO.attempt(DriverManager.getConnection(Fixtures.trinoConnectionString))
      statement  <- ZIO.attempt(connection.createStatement())
      values = legacyRows.map { case (id, version) => s"('$id', $version, 'stale-key')" }.mkString(", ")
      _ <- ZIO.attemptBlocking(
        statement.execute(s"INSERT INTO $table (Id, versionnumber, ARCANE_MERGE_KEY) VALUES $values")
      )
    yield ()
  }

  private def readColumns(table: String) = ZIO.scoped {
    for
      connection <- ZIO.attempt(DriverManager.getConnection(Fixtures.trinoConnectionString))
      statement  <- ZIO.attempt(connection.createStatement())
      resultSet  <- ZIO.fromAutoCloseable(ZIO.attemptBlocking(statement.executeQuery(s"DESCRIBE $table")))
      columns <- ZIO.attempt(
        Iterator.continually(resultSet).takeWhile(_.next()).map(_.getString(1).toLowerCase).toList
      )
    yield columns
  }

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("StreamRunner")(
    test("migrate legacy sink table to ARCANE_MERGE_KEY and ARCANE_VERSION") {
      for
        migrationContext <- ZIO.succeed(getStreamContext(migrationTargetTableName))
        migrationContextLayer = getStreamContextLayer(migrationTargetTableName)
        _ <- clearTarget(migrationTargetTableName)
        _ <- Fixtures.clearSource
        _ <- prepareWatermark(migrationTargetTableName.split('.').last, legacySchema, SynapseWatermark.epoch)
        _ <- seedLegacyRows(migrationTargetTableName)

        columnsBefore <- readColumns(migrationTargetTableName)

        _        <- TestSystem.putEnv("STREAMCONTEXT__RUNMODE", "migrate")
        migrator <- Common.getTestApp(Duration.ofSeconds(45), migrationContextLayer).fork
        _        <- migrator.runOrFail(Duration.ofSeconds(30))
        _        <- TestSystem.putEnv("STREAMCONTEXT__RUNMODE", "stream")

        columnsAfter <- readColumns(migrationTargetTableName)
        migrated <- readTarget(
          migrationContext.sink.targetTableFullName,
          "ARCANE_MERGE_KEY, ARCANE_VERSION",
          StrStrDecoder
        )
      yield assertTrue(columnsBefore.contains("id") && columnsBefore.contains("versionnumber")) implies
        assertTrue(!columnsBefore.contains("arcane_version")) implies
        assertTrue(!columnsAfter.contains("id") && !columnsAfter.contains("versionnumber")) implies
        assertTrue(columnsAfter.contains("arcane_merge_key") && columnsAfter.contains("arcane_version")) implies
        assertTrue(migrated.sorted == legacyRows.map { case (id, version) => (mergeKey(id), version.toString) }.sorted)
    },
    test("backfill and then stream changes successfully") {
      for
        streamContext      = getStreamContext(targetTableName)
        streamContextLayer = getStreamContextLayer(targetTableName)
        _ <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL", "true")
        _ <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL_ID", Random.alphanumeric.take(10).mkString("").toLowerCase)
        _ <- clearTarget(targetTableName)
        _ <- Fixtures.clearSource
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
          "ARCANE_MERGE_KEY, ARCANE_VERSION",
          StrStrDecoder
        )
          .map(_.size)

        backfilledWatermark <- getWatermark(streamContext.sink.targetTableFullName.split('.').last)(using
          SynapseWatermark.rw
        )

        _ <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL", "false")
        _ <- TestSystem.putEnv("STREAMCONTEXT__BACKFILL_ID", "")

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
          "ARCANE_MERGE_KEY, ARCANE_VERSION",
          StrStrDecoder
        )

        streamedWatermark <- getWatermark(streamContext.sink.targetTableFullName.split('.').last)(using
          SynapseWatermark.rw
        )
      yield assertTrue(backfilledCount == 5) implies assertTrue(
        backfilledWatermark.version == s"${formatter.format(startTime.minusHours(1))}Z"
      ) implies assertTrue(currentRows.size == 5 - 1 + 2) implies assertTrue(
        !currentRows.exists(_._1 == mergeKey("50bff458-d47a-4924-804b-31c0a83108e6"))
      ) implies assertTrue(
        currentRows
          .find(_._1 == mergeKey("5b4bc74e-2132-4d8e-8572-48ce4260f182"))
          .map(_._2)
          .getOrElse("") == "2111000012"
      ) implies assertTrue(
        streamedWatermark.version == s"${formatter.format(startTime.minusMinutes(5))}Z"
      )
    }
  ) @@ timeout(zio.Duration.fromSeconds(180)) @@ TestAspect.withLiveClock @@ TestAspect.sequential @@ TestAspect.before(
    liveSeed
  )
