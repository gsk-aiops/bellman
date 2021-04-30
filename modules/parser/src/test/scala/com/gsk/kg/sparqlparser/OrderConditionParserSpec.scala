package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.BuiltInFunc.ISBLANK
import com.gsk.kg.sparqlparser.ConditionOrder.ASC
import com.gsk.kg.sparqlparser.ConditionOrder.DESC
import com.gsk.kg.sparqlparser.Conditional.OR
import com.gsk.kg.sparqlparser.StringVal.VARIABLE

import org.scalatest.wordspec.AnyWordSpec

class OrderConditionParserSpec extends AnyWordSpec {

  "Asc parser" should {

    "return ASC type" when {

      "no explicit order defined with variable" in {
        val p =
          fastparse.parse("""?name""", OrderConditionParser.ascParen(_))
        p.get.value match {
          case ASC(VARIABLE(v)) => succeed
          case _                => fail
        }
      }

      "no explicit order defined with condition" in {
        val p =
          fastparse.parse(
            """(|| (isBlank ?x) (isBlank ?emp))""",
            OrderConditionParser.ascParen(_)
          )
        p.get.value match {
          case ASC(OR(ISBLANK(a), ISBLANK(b))) => succeed
          case _                               => fail
        }
      }

      "explicit ASC order defined with variable" in {
        val p =
          fastparse.parse("""(asc ?name)""", OrderConditionParser.ascParen(_))
        p.get.value match {
          case ASC(VARIABLE(v)) => succeed
          case _                => fail
        }
      }

      "explicit ASC order defined with condition" in {
        val p =
          fastparse.parse(
            """(asc (|| (isBlank ?x) (isBlank ?emp)))""",
            OrderConditionParser.ascParen(_)
          )
        p.get.value match {
          case ASC(OR(ISBLANK(a), ISBLANK(b))) => succeed
          case _                               => fail
        }
      }
    }
  }

  "Desc parser" should {

    "return DESC type" when {

      "explicit DESC order defined with variable" in {
        val p =
          fastparse.parse("""(desc ?name)""", OrderConditionParser.descParen(_))
        p.get.value match {
          case DESC(VARIABLE(v)) => succeed
          case _                 => fail
        }
      }

      "explicit DESC order defined with condition" in {
        val p =
          fastparse.parse(
            """(desc (|| (isBlank ?x) (isBlank ?emp)))""",
            OrderConditionParser.descParen(_)
          )
        p.get.value match {
          case DESC(OR(ISBLANK(a), ISBLANK(b))) => succeed
          case _                                => fail
        }
      }
    }
  }
}

//(project (?name)
//(order (?name (desc ?emp))
//(bgp
//(triple ?x <http://xmlns.com/foaf/0.1/name> ?name)
//(triple ?x <http://xmlns.com/foaf/0.1/empId> ?emp)
//)))
//
//(project (?name)
//(order (?name (desc ?emp) (|| (isBlank ?x) (isBlank ?emp)))
//(bgp
//(triple ?x <http://xmlns.com/foaf/0.1/name> ?name)
//(triple ?x <http://xmlns.com/foaf/0.1/empId> ?emp)
//)))
//
//(project (?name)
//(order (?name ?emp (desc ?emp) (asc ?name) (|| (isBlank ?x) (isBlank ?emp)))
//(bgp
//(triple ?x <http://xmlns.com/foaf/0.1/name> ?name)
//(triple ?x <http://xmlns.com/foaf/0.1/empId> ?emp)
//)))
//
//(project (?name)
//(order (?name ?emp (desc ?emp) (asc ?name) (desc (|| (isBlank ?x) (isBlank ?emp))))
//(bgp
//(triple ?x <http://xmlns.com/foaf/0.1/name> ?name)
//(triple ?x <http://xmlns.com/foaf/0.1/empId> ?emp)
//)))
