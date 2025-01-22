package com.sneaksanddata.arcane.framework
package models

import com.sneaksanddata.arcane.microsoft_synapse_link.services.streaming.processors.{TypeAlignmentService, TypeAlignmentServiceImpl}
import models.cdm.CSVParser
import org.scalatest.flatspec.{AnyFlatSpec, AsyncFlatSpec}
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks.*

import java.time.LocalDateTime


class TypeAlignmentServiceTests extends AnyFlatSpec with Matchers {

  private val validTimeStamps = Table(
    ("line", "result"),
    (List(DataCell("timestamp", ArcaneType.TimestampType, Some("2020-01-01T00:00:00.000Z"))), LocalDateTime.now),
    (List(DataCell("timestamp", ArcaneType.TimestampType, Some("1900-01-01T00:00:00.0000000"))), LocalDateTime.now),
  )


  it should "Should parse valid timestamps" in {
    val tas = TypeAlignmentServiceImpl()
    forAll (validTimeStamps) { (line, result) =>
      tas.alignTypes(line) //should equal(result)
    }
  }

}
