package com.gsk.kg.sparqlparser

import org.scalatest.flatspec.AnyFlatSpec

class ArithmeticParserSpec extends AnyFlatSpec {

  "Add parser" should "return ADD type" in {
    val p =
      fastparse.parse("""(+ ?a ?b)""", ArithmeticParser.addParen(_))
    p.get.value match {
      case _ => succeed
      case _ => fail
    }
  }

  "Subtract parser" should "return SUBTRACT type" in {
    val p =
      fastparse.parse("""(- ?a ?b)""", ArithmeticParser.subtractParen(_))
    p.get.value match {
      case _ => succeed
      case _ => fail
    }
  }

  "Multiply parser" should "return MULTIPLY type" in {
    val p =
      fastparse.parse("""(* ?a ?b)""", ArithmeticParser.multiplyParen(_))
    p.get.value match {
      case _ => succeed
      case _ => fail
    }
  }

  "Divide parser" should "return DIVIDE type" in {
    val p =
      fastparse.parse("""(/ ?a ?b)""", ArithmeticParser.divideParen(_))
    p.get.value match {
      case _ => succeed
      case _ => fail
    }
  }
}
