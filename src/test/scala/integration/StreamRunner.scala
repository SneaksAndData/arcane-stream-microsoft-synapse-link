package com.sneaksanddata.arcane.microsoft_synapse_link
package integration

import com.sneaksanddata.arcane.framework.services.mssql.*
import com.sneaksanddata.arcane.framework.services.mssql.base.ConnectionOptions
import com.sneaksanddata.arcane.microsoft_synapse_link.common.{Common, TimeLimitLifetimeService}
import com.sneaksanddata.arcane.microsoft_synapse_link.models.app.MicrosoftSynapseLinkStreamContext
import com.sneaksanddata.arcane.microsoft_synapse_link.models.app.contracts.StreamSpec
import org.scalatest.matchers.should.Matchers.should
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig
import zio.test.TestAspect.timeout
import zio.test.*
import zio.{Scope, Unsafe, ZIO, ZLayer}

import java.time.Duration
import scala.language.postfixOps

object StreamRunner extends ZIOSpecDefault:

  private val sourceTableName = "StreamRunner"
  private val targetTableName = "iceberg.test.stream_dimensionattributelevelvalue"

  private val streamContextStr = s"""
    |
    | {
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
    |    baseLocation: abfss://cdm-e2e.dfs.core.windows.net/
    |    changeCaptureIntervalSeconds: 300
    |    name: dimensionattributelevelvalue
    |   },
    |  "stagingDataSettings": {
    |    "catalog": {
    |      "catalogName": "iceberg",
    |      "catalogUri": "http://localhost:20001/catalog",
    |      "namespace": "test",
    |      "warehouse": "demo"
    |    },
    |    "maxRowsPerFile": 1,
    |    "tableNamePrefix": "staging_$targetTableName"
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
        sourceConnection <- ZIO.succeed(Fixtures.getConnection)
        // Testing the stream runner in the streaming mode
        insertRunner <- Common.buildTestApp(TimeLimitLifetimeService.layer, streamingStreamContextLayer).fork

        _ <- Common.waitForData[(Int, String)](
          streamingStreamContext.targetTableFullName,
          "Id, Name",
          Common.IntStrDecoder,
          streamingData.length
        )
        _ <- insertRunner.await.timeout(Duration.ofSeconds(10))

        afterStream <- Common.getData(streamingStreamContext.targetTableFullName, "Id, Name", Common.IntStrDecoder)

        // Testing the stream runner in the backfill mode
        backfillRunner <- Common.buildTestApp(TimeLimitLifetimeService.layer, backfillStreamContextLayer).fork
        _              <- Common.insertData(sourceConnection, parsedSpec.sourceSettings.table, backfillData)
        _ <- Common.waitForData[(Int, String)](
          streamingStreamContext.targetTableFullName,
          "Id, Name",
          Common.IntStrDecoder,
          backfillData.length + streamingData.length
        )

        _ <- backfillRunner.await.timeout(Duration.ofSeconds(10))

        afterBackfill <- Common.getData(streamingStreamContext.targetTableFullName, "Id, Name", Common.IntStrDecoder)

        // Testing the update and delete operations
        deleteUpdateRunner <- Common.buildTestApp(TimeLimitLifetimeService.layer, streamingStreamContextLayer).fork
        _                  <- Common.updateData(sourceConnection, parsedSpec.sourceSettings.table, updatedData)
        _                  <- ZIO.sleep(Duration.ofSeconds(1))
        _                  <- Common.deleteData(sourceConnection, parsedSpec.sourceSettings.table, deletedData)
        _ <- Common.waitForData[(Int, String)](
          streamingStreamContext.targetTableFullName,
          "Id, Name",
          Common.IntStrDecoder,
          resultData.length
        )

        _ <- deleteUpdateRunner.await.timeout(Duration.ofSeconds(10))

        afterUpdateDelete <- Common.getData(
          streamingStreamContext.targetTableFullName,
          "Id, Name",
          Common.IntStrDecoder
        )
      yield assertTrue(afterStream.sorted == streamingData.sorted) implies assertTrue(
        afterBackfill.sorted == (streamingData ++ backfillData).sorted
      ) implies assertTrue(afterUpdateDelete.sorted == resultData.sorted)
    }
  ) @@ timeout(zio.Duration.fromSeconds(180)) @@ TestAspect.withLiveClock
