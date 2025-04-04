//package com.sneaksanddata.arcane.microsoft_synapse_link
//
//
//import com.sneaksanddata.arcane.framework.models.{ArcaneType, DataCell}
//import org.scalatest.flatspec.AnyFlatSpec
//import org.scalatest.matchers.must.Matchers
//import org.scalatest.matchers.should.Matchers.should
//import org.scalatest.prop.TableDrivenPropertyChecks.*
//
//
//class TypeAlignmentServiceTests extends AnyFlatSpec with Matchers {
//
//  private val validTimeStamps = Table(
//    "line",
//    List(DataCell("timestamp", ArcaneType.TimestampType, Some("2020-01-01T00:00:00.000Z"))),
//    List(DataCell("timestamp", ArcaneType.TimestampType, Some("1900-01-01T00:00:00.0000000"))),
//    List(DataCell("timestamp", ArcaneType.DateTimeOffsetType, Some("0001-01-03T00:00:00.0000000"))),
//    List(DataCell("timestamp", ArcaneType.DateTimeOffsetType, Some("2023-08-15T02:45:20.0000000+00:00"))),
//  )
//
//
//  it should "Should parse valid timestamps" in {
//    val tas = TypeAlignmentServiceImpl()
//    forAll (validTimeStamps) { line =>
//      noException should be thrownBy tas.alignTypes(line)
//    }
//  }
//
//}
