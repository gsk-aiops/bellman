package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.Date.NOW

import org.scalatest.flatspec.AnyFlatSpec

class DateParserSpec extends AnyFlatSpec {

  "NOW parser" should "return NOW type" in {
    val p =
      fastparse.parse(
        """(now)""",
        DateParser.nowParen(_)
      )
    p.get.value match {
      case NOW() =>
        succeed
      case _ => fail
    }
  }
}
