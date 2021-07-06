package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.DateTimeFunc.NOW
import com.gsk.kg.sparqlparser.DateTimeFunc.YEAR
import com.gsk.kg.sparqlparser.StringVal.STRING
import com.gsk.kg.sparqlparser.StringVal.VARIABLE

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

  "YEAR parser with string" should "return YEAR type" in {
    val p =
      fastparse.parse(
        """(year "x")""",
        DateTimeFuncsParser.yearParen(_)
      )
    p.get.value match {
      case YEAR(STRING("x")) =>
        succeed
      case _ => fail
    }
  }

  "YEAR parser with variable" should "return YEAR type" in {
    val p =
      fastparse.parse(
        """(year ?d)""",
        DateTimeFuncsParser.yearParen(_)
      )
    p.get.value match {
      case YEAR(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }
}
