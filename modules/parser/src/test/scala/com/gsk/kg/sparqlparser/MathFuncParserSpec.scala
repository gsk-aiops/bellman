package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.MathFunc._
import com.gsk.kg.sparqlparser.StringVal._

import org.scalatest.flatspec.AnyFlatSpec

class MathFuncParserSpec extends AnyFlatSpec {

  "CEIL parser with string" should "return CEIL type" in {
    val p =
      fastparse.parse(
        """(ceil "x")""",
        MathFuncParser.ceilParen(_)
      )
    p.get.value match {
      case CEIL(STRING("x")) =>
        succeed
      case _ => fail
    }
  }

  "CEIL parser with variable" should "return CEIL type" in {
    val p =
      fastparse.parse(
        """(ceil ?d)""",
        MathFuncParser.ceilParen(_)
      )
    p.get.value match {
      case CEIL(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }

  "ROUND parser with string" should "return ROUND type" in {
    val p =
      fastparse.parse(
        """(round "x")""",
        MathFuncParser.roundParen(_)
      )
    p.get.value match {
      case ROUND(STRING("x")) =>
        succeed
      case _ => fail
    }
  }

  "ROUND parser with variable" should "return ROUND type" in {
    val p =
      fastparse.parse(
        """(round ?d)""",
        MathFuncParser.roundParen(_)
      )
    p.get.value match {
      case ROUND(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }

  "RAND parser" should "return RAND type" in {
    val p =
      fastparse.parse(
        """(rand)""",
        MathFuncParser.randParen(_)
      )
    p.get.value match {
      case RAND() =>
        succeed
      case _ => fail
    }
  }

  "ABS parser with string" should "return ABS type" in {
    val p =
      fastparse.parse(
        """(abs "x")""",
        MathFuncParser.absParen(_)
      )
    p.get.value match {
      case ABS(STRING("x")) =>
        succeed
      case _ => fail
    }
  }

  "ABS parser with variable" should "return ABS type" in {
    val p =
      fastparse.parse(
        """(abs ?d)""",
        MathFuncParser.absParen(_)
      )
    p.get.value match {
      case ABS(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }

  "FLOOR parser with string" should "return FLOOR type" in {
    val p =
      fastparse.parse(
        """(floor "x")""",
        MathFuncParser.floorParen(_)
      )
    p.get.value match {
      case FLOOR(STRING("x")) =>
        succeed
      case _ => fail
    }
  }

  "FLOOR parser with variable" should "return FLOOR type" in {
    val p =
      fastparse.parse(
        """(floor ?d)""",
        MathFuncParser.floorParen(_)
      )
    p.get.value match {
      case FLOOR(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }

}
