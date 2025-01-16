package com.sneaksanddata.arcane.microsoft_synapse_link
package services.streaming.processors

import com.sneaksanddata.arcane.framework.models.ArcaneType.*
import com.sneaksanddata.arcane.framework.models.settings.GroupingSettings
import com.sneaksanddata.arcane.framework.models.{ArcaneType, DataCell, DataRow}
import com.sneaksanddata.arcane.framework.services.streaming.base.BatchProcessor
import zio.stream.ZPipeline
import zio.{Chunk, ZIO, ZLayer}
import zio.Chunk
import zio.stream.ZPipeline

import java.time.{Instant, LocalDateTime, OffsetDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.sql.Timestamp


/**
 * The service that processes a DataRow and aligns the types of the cells.
 * This operation is necessary to ensure that the data is correctly processed by the stream graph.
 * See the Microsoft Synapse documentation for more information:
 * https://learn.microsoft.com/en-us/power-apps/maker/data-platform/export-data-lake-faq
 */
trait TypeAlignmentService:
  def alignTypes(data: DataRow): DataRow

private class TypeAlignmentServiceImpl extends TypeAlignmentService:

  override def alignTypes(row: DataRow): DataRow = row map { cell =>
    DataCell(cell.name, cell.Type, convertType(cell.name, cell.Type, cell.value))
  }

  private def convertType(cellName: String, arcaneType: ArcaneType, value: Any): Any =
    value match
      case None => null
      case Some(v) => convertSome(cellName, arcaneType, v)

  private def convertSome(cellName: String, arcaneType: ArcaneType, value: Any): Any = arcaneType match
    case LongType => value.toString.toLong
    case ByteArrayType => value.toString.getBytes
    case BooleanType => value.toString.toBoolean
    case StringType => value.toString
    case DateType => java.sql.Date.valueOf(value.toString)
    case TimestampType => convertToTimeStamp(cellName, value)
    case DateTimeOffsetType => java.time.OffsetDateTime.parse(value.toString)
    case BigDecimalType => BigDecimal(value.toString)
    case DoubleType => value.toString.toDouble
    case IntType => value.toString.toInt
    case FloatType => value.toString.toFloat
    case ShortType => value.toString.toShort
    case TimeType => java.sql.Time.valueOf(value.toString)

private def convertToTimeStamp(columnName: String, value: Any): LocalDateTime = value match
  case timestampValue: String =>
    columnName match
      case "SinkCreatedOn" | "SinkModifiedOn" =>
        // format  from MS docs: M/d/yyyy H:mm:ss tt
        // example from MS docs: 6/28/2021 4:34:35 PM
        LocalDateTime.parse(timestampValue, DateTimeFormatter.ofPattern("M/d/yyyy h:mm:ss a"))
      case "CreatedOn" =>
        // format  from MS docs: yyyy-MM-dd'T'HH:mm:ss.sssssssXXX
        // example from MS docs: 2018-05-25T16:21:09.0000000+00:00
        LocalDateTime.ofInstant(OffsetDateTime.parse(timestampValue, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant,
          ZoneId.systemDefault())
      case _ =>
        // format  from MS docs: yyyy-MM-dd'T'HH:mm:ss'Z'
        // example from MS docs: 2021-06-25T16:21:12Z
        LocalDateTime.ofInstant(Instant.parse(timestampValue), ZoneId.systemDefault())
  case _ => throw new IllegalArgumentException(s"Invalid timestamp type: ${value.getClass}")

object TypeAlignmentService:
  val layer: ZLayer[Any, Nothing, TypeAlignmentService] = ZLayer.succeed(new TypeAlignmentServiceImpl)

  def apply(): TypeAlignmentService = new TypeAlignmentServiceImpl
