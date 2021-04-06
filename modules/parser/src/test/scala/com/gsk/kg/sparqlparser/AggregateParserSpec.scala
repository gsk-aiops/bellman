package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.StringVal._

import org.scalatest.flatspec.AnyFlatSpec

class AggregateParserSpec extends AnyFlatSpec {

  "count" should "be parsed correctly" in {
    val s = "(count ?p1)"
    val p = fastparse.parse(s, AggregateParser.parser(_))
    p.get.value match {
      case Aggregate.COUNT(VARIABLE("?p1")) => succeed
      case _                                => fail
    }
  }

  "avg" should "be parsed correctly" in {
    val s = "(avg ?p1)"
    val p = fastparse.parse(s, AggregateParser.parser(_))
    p.get.value match {
      case Aggregate.AVG(VARIABLE("?p1")) => succeed
      case _                              => fail
    }
  }

  "max" should "be parsed correctly" in {
    val s = "(max ?p1)"
    val p = fastparse.parse(s, AggregateParser.parser(_))
    p.get.value match {
      case Aggregate.MAX(VARIABLE("?p1")) => succeed
      case _                              => fail
    }
  }

  "min" should "be parsed correctly" in {
    val s = "(min ?p1)"
    val p = fastparse.parse(s, AggregateParser.parser(_))
    p.get.value match {
      case Aggregate.MIN(VARIABLE("?p1")) => succeed
      case _                              => fail
    }
  }

  "sample" should "be parsed correctly" in {
    val s = "(sample ?p1)"
    val p = fastparse.parse(s, AggregateParser.parser(_))
    p.get.value match {
      case Aggregate.SAMPLE(VARIABLE("?p1")) => succeed
      case _                                 => fail
    }
  }

  "group_concat" should "be parsed correctly" in {
    val s = "(groupConcat ?p1 \",\")"
    val p = fastparse.parse(s, AggregateParser.parser(_))
    p.get.value match {
      case Aggregate.GROUP_CONCAT(VARIABLE("?p1"), ",") => succeed
      case _                                            => fail
    }
  }

}
