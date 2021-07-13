package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.DateTimeFunc._
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

  "MONTH parser with string" should "return MONTH type" in {
    val p =
      fastparse.parse(
        """(month "x")""",
        DateTimeFuncsParser.monthParen(_)
      )
    p.get.value match {
      case MONTH(STRING("x")) =>
        succeed
      case _ => fail
    }
  }

  "MONTH parser with variable" should "return MONTH type" in {
    val p =
      fastparse.parse(
        """(month ?d)""",
        DateTimeFuncsParser.monthParen(_)
      )
    p.get.value match {
      case MONTH(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }

  "DAY parser with string" should "return DAY type" in {
    val p =
      fastparse.parse(
        """(day "x")""",
        DateTimeFuncsParser.dayParen(_)
      )
    p.get.value match {
      case DAY(STRING("x")) =>
        succeed
      case _ => fail
    }
  }

  "DAY parser with variable" should "return DAY type" in {
    val p =
      fastparse.parse(
        """(day ?d)""",
        DateTimeFuncsParser.dayParen(_)
      )
    p.get.value match {
      case DAY(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }

  "HOUR parser with string" should "return HOUR type" in {
    val p =
      fastparse.parse(
        """(hours "x")""",
        DateTimeFuncsParser.hoursParen(_)
      )
    p.get.value match {
      case HOUR(STRING("x")) =>
        succeed
      case _ => fail
    }
  }

  "HOUR parser with variable" should "return HOUR type" in {
    val p =
      fastparse.parse(
        """(hours ?d)""",
        DateTimeFuncsParser.hoursParen(_)
      )
    p.get.value match {
      case HOUR(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }

  "MINUTES parser with string" should "return MINUTES type" in {
    val p =
      fastparse.parse(
        """(minutes "x")""",
        DateTimeFuncsParser.minutesParen(_)
      )
    p.get.value match {
      case MINUTES(STRING("x")) =>
        succeed
      case _ => fail
    }
  }

  "MINUTES parser with variable" should "return MINUTES type" in {
    val p =
      fastparse.parse(
        """(minutes ?d)""",
        DateTimeFuncsParser.minutesParen(_)
      )
    p.get.value match {
      case MINUTES(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }

  "SECONDS parser with string" should "return SECONDS type" in {
    val p =
      fastparse.parse(
        """(seconds "x")""",
        DateTimeFuncsParser.secondsParen(_)
      )
    p.get.value match {
      case SECONDS(STRING("x")) =>
        succeed
      case _ => fail
    }
  }

  "SECONDS parser with variable" should "return SECONDS type" in {
    val p =
      fastparse.parse(
        """(seconds ?d)""",
        DateTimeFuncsParser.secondsParen(_)
      )
    p.get.value match {
      case SECONDS(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }

  "TZ parser with string" should "return TZ type" in {
    val p =
      fastparse.parse(
        """(tz "x")""",
        DateTimeFuncsParser.tzParen(_)
      )
    p.get.value match {
      case TZ(STRING("x")) =>
        succeed
      case _ => fail
    }
  }

  "TZ parser with variable" should "return TZ type" in {
    val p =
      fastparse.parse(
        """(tz ?d)""",
        DateTimeFuncsParser.tzParen(_)
      )
    p.get.value match {
      case TZ(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }
}
