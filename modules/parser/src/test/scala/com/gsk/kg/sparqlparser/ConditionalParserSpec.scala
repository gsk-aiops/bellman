package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.Conditional._
import com.gsk.kg.sparqlparser.StringVal._

import org.scalatest.flatspec.AnyFlatSpec

class ConditionalParserSpec extends AnyFlatSpec {

  "Equals parser" should "return EQUALS type" in {
    val p =
      fastparse.parse("""(= ?d "Hello")""", ConditionalParser.equalsParen(_))
    p.get.value match {
      case EQUALS(VARIABLE("?d"), STRING("Hello", None)) => succeed
      case _                                             => fail
    }
  }

  "Not equals parser" should "return NEGATIVE(EQUALS) type" in {
    val p = fastparse.parse(
      """(!= ?d "Hello")""",
      ConditionalParser.notEqualsParen(_)
    )
    p.get.value match {
      case NEGATE(EQUALS(VARIABLE("?d"), STRING("Hello", None))) => succeed
      case _                                                     => fail
    }
  }

  "GT parser" should "return GT type" in {
    val p =
      fastparse.parse("""(> ?year "2015")""", ConditionalParser.gtParen(_))
    p.get.value match {
      case GT(VARIABLE("?year"), STRING("2015", None)) => succeed
      case _                                           => fail
    }
  }

  "LT parser" should "return LT type" in {
    val p =
      fastparse.parse("""(< ?year "2015")""", ConditionalParser.ltParen(_))
    p.get.value match {
      case LT(VARIABLE("?year"), STRING("2015", None)) => succeed
      case _                                           => fail
    }
  }

  "GTE parser" should "return GTE type" in {
    val p =
      fastparse.parse("""(>= ?year "2015")""", ConditionalParser.gteParen(_))
    p.get.value match {
      case GTE(VARIABLE("?year"), STRING("2015", None)) => succeed
      case _                                            => fail
    }
  }

  "LTE parser" should "return LTE type" in {
    val p =
      fastparse.parse("""(<= ?year "2015")""", ConditionalParser.lteParen(_))
    p.get.value match {
      case LTE(VARIABLE("?year"), STRING("2015", None)) => succeed
      case _                                            => fail
    }
  }
}
