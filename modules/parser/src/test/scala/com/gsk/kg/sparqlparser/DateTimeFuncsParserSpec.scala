package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.DateTimeFunc.NOW

import org.scalatest.flatspec.AnyFlatSpec

class DateTimeFuncsParserSpec extends AnyFlatSpec {

  "NOW parser" should "return NOW type" in {
    val p =
      fastparse.parse(
        """(now)""",
        DateTimeFuncsParser.nowParen(_)
      )
    p.get.value match {
      case NOW() =>
        succeed
      case _ => fail
    }
  }
}
