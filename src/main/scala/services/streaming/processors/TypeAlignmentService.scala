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

import java.time.format.DateTimeFormatter

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

private def convertToTimeStamp(value: Any, columnName: String): java.sql.Timestamp = value match
  case v: String =>
    columnName match
      case "SinkCreatedOn" | "SinkModifiedOn" =>
        val formatter = DateTimeFormatter.ofPattern("M/d/yyyy h:mm:ss a")
        val localDateTime = LocalDateTime.parse(dateTimeString, formatter)
        val timestamp = Timestamp.valueOf(localDateTime)
      case "CreatedOn" =>
        val localDateTime = LocalDateTime.parse(dateTimeString, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        val timestamp = Timestamp.valueOf(localDateTime)
      case _ =>
        val localDateTime = LocalDateTime.parse(dateTimeString, DateTimeFormatter.ISO_INSTANT)
        val timestamp = Timestamp.valueOf(localDateTime)
  case _ => throw new IllegalArgumentException(s"Invalid timestamp type: ${value.getClass}")
  
object TypeAlignmentService:
  val layer: ZLayer[Any, Nothing, TypeAlignmentService] = ZLayer.succeed(new TypeAlignmentServiceImpl)

  def apply(): TypeAlignmentService = new TypeAlignmentServiceImpl
