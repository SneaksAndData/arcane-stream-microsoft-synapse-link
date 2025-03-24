package com.sneaksanddata.arcane.microsoft_synapse_link
package services.data_providers.microsoft_synapse_link

import models.app.*
import services.streaming.consumers.DataStreamElementExtensions.given_MetadataEnrichedRowStreamElement_DataStreamElement
import services.streaming.consumers.IndexedStagedBatchesImpl
import services.streaming.processors.{CdmGroupingProcessor, FieldFilteringProcessor}

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.models.settings.*
import com.sneaksanddata.arcane.framework.services.base.TableManager
import com.sneaksanddata.arcane.framework.services.consumers.*
import com.sneaksanddata.arcane.framework.services.lakehouse.base.{CatalogWriter, CatalogWriterBuilder, IcebergCatalogSettings}
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClient
import com.sneaksanddata.arcane.framework.services.streaming.base.HookManager
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.streaming.{DisposeBatchProcessor, MergeBatchProcessor}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.StagingProcessor
import com.sneaksanddata.arcane.microsoft_synapse_link.services.data_providers.microsoft_synapse_link.base.{BackfillBatchInFlight, MicrosoftSynapseLinkBackfillDataProvider}
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.{Task, ZIO, ZLayer}

class MicrosoftSynapseLinkBackfillMergeDataProvider(cdmTableStream: CdmTableStream,
                                                        groupingProcessor: CdmGroupingProcessor,
                                                        fieldFilteringProcessor: FieldFilteringProcessor,
                                                        backfillSettings: BackfillSettings,
                                                        disposeBatchProcessor: DisposeBatchProcessor,
                                                        stagingProcessor: StagingProcessor,
                                                        mergeProcessor: MergeBatchProcessor,
                                                        hookManager: HookManager) extends MicrosoftSynapseLinkBackfillDataProvider:

  def requestBackfill: Task[BackfillBatchInFlight] =
    for
      _ <- backfillSettings.backfillBehavior match
        case BackfillBehavior.Merge => ZIO.unit
        case BackfillBehavior.Overwrite => ZIO.die(new IllegalArgumentException("Running backfill overwrite in overwrite runner"))
      _ <- zlog(s"Starting backfill process. Backfill behavior: ${backfillSettings.backfillBehavior}")
      cleanupRequests <- backfillStream.runDrain
      _ <- zlog("Backfill process completed")
    yield ()

  private def backfillStream =
    cdmTableStream.getPrefixesFromDate(backfillSettings.backfillStartDate.getOrElse(throw new IllegalArgumentException("Backfill start date is not set")))
      .mapZIO(blob => cdmTableStream.getStream(blob))
      .flatMap(reader => cdmTableStream.getData(reader))
      .via(fieldFilteringProcessor.process)
      .via(groupingProcessor.process)
      .via(stagingProcessor.process(toInFlightBatch, hookManager.onBatchStaged))
      .via(mergeProcessor.process)
      .via(disposeBatchProcessor.process)

  def toInFlightBatch(batches: Iterable[StagedVersionedBatch & MergeableBatch], index: Long, others: Any): MergeBatchProcessor#BatchType =
    new IndexedStagedBatchesImpl(batches, index)

object MicrosoftSynapseLinkBackfillMergeDataProvider:

  type Environment = CdmTableStream
    & CdmGroupingProcessor
    & FieldFilteringProcessor
    & BackfillSettings
    & DisposeBatchProcessor
    & HookManager
    & MergeBatchProcessor
    & StagingProcessor

  def apply(cdmTableStream: CdmTableStream,
            groupingProcessor: CdmGroupingProcessor,
            fieldFilteringProcessor: FieldFilteringProcessor,
            backfillSettings: BackfillSettings,
            disposeBatchProcessor: DisposeBatchProcessor,
            stagingProcessor: StagingProcessor,
            mergeProcessor: MergeBatchProcessor,
            hookManager: HookManager): MicrosoftSynapseLinkBackfillMergeDataProvider =
    new MicrosoftSynapseLinkBackfillMergeDataProvider(
      cdmTableStream,
      groupingProcessor,
      fieldFilteringProcessor,
      backfillSettings,
      disposeBatchProcessor,
      stagingProcessor,
      mergeProcessor,
      hookManager)

  def layer: ZLayer[Environment, Nothing, MicrosoftSynapseLinkBackfillMergeDataProvider] =
    ZLayer {
      for
        cdmTableStream <- ZIO.service[CdmTableStream]
        groupingProcessor <- ZIO.service[CdmGroupingProcessor]
        fieldFilteringProcessor <- ZIO.service[FieldFilteringProcessor]
        backfillSettings <- ZIO.service[BackfillSettings]
        disposeBatchProcessor <- ZIO.service[DisposeBatchProcessor]
        stagingProcessor <- ZIO.service[StagingProcessor]
        mergeProcessor <- ZIO.service[MergeBatchProcessor]
        hookManager <- ZIO.service[HookManager]
      yield MicrosoftSynapseLinkBackfillMergeDataProvider(
        cdmTableStream,
        groupingProcessor,
        fieldFilteringProcessor,
        backfillSettings,
        disposeBatchProcessor,
        stagingProcessor,
        mergeProcessor,
        hookManager)
    }

