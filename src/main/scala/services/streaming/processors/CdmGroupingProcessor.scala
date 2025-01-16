package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.processors

import com.sneaksanddata.arcane.framework.models.ArcaneType.*
import com.sneaksanddata.arcane.framework.models.settings.GroupingSettings
import com.sneaksanddata.arcane.framework.models.{ArcaneType, DataCell, DataRow}
import com.sneaksanddata.arcane.framework.services.streaming.base.BatchProcessor

import zio.stream.ZPipeline
import zio.{Chunk, ZIO, ZLayer}

import scala.concurrent.duration.Duration

/**
 * The batch processor implementation that converts a lazy DataBatch to a Chunk of DataRow.
 * @param groupingSettings The grouping settings.
 */
class CdmGroupingProcessor(groupingSettings: GroupingSettings, typeAlignmentProcessor: TypeAlignmentService)
  extends BatchProcessor[LazyList[DataRow], Chunk[DataRow]]:

  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  def process: ZPipeline[Any, Throwable, LazyList[DataRow], Chunk[DataRow]] = ZPipeline
    .map[LazyList[DataRow], Chunk[DataRow]](list => Chunk.fromIterable(list))
    .flattenChunks
    .map(typeAlignmentProcessor.alignTypes)
    .groupedWithin(groupingSettings.rowsPerGroup, groupingSettings.groupingInterval)
    .mapZIO(logBatchSize)

  private def logBatchSize(batch: Chunk[DataRow]): ZIO[Any, Nothing, Chunk[DataRow]] =
    for _ <- ZIO.log(s"Received batch with ${batch.size} rows from streaming source") yield batch
    
/**
 * The companion object for the LazyOutputDataProcessor class.
 */
object CdmGroupingProcessor:
  
  type Environment = GroupingSettings 
    & TypeAlignmentService

  /**
   * The ZLayer that creates the LazyOutputDataProcessor.
   */
  val layer: ZLayer[Environment, Nothing, CdmGroupingProcessor] =
    ZLayer {
      for
        settings <- ZIO.service[GroupingSettings]
        tas <- ZIO.service[TypeAlignmentService]
      yield CdmGroupingProcessor(settings, tas)
    }

  def apply(groupingSettings: GroupingSettings, typeAlignmentService: TypeAlignmentService): CdmGroupingProcessor =
    require(groupingSettings.rowsPerGroup > 0, "Rows per group must be greater than 0")
    require(!groupingSettings.groupingInterval.equals(Duration.Zero), "groupingInterval must be greater than 0")
    new CdmGroupingProcessor(groupingSettings, typeAlignmentService)
