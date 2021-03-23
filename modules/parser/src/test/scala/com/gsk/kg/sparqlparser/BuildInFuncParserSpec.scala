package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.BuildInFunc._
import com.gsk.kg.sparqlparser.StringVal._

import org.scalatest.flatspec.AnyFlatSpec

class BuildInFuncParserSpec extends AnyFlatSpec {
  "URI function with string" should "return URI type" in {
    val s = "(uri \"http://id.gsk.com/dm/1.0/\")"
    val p = fastparse.parse(s, BuildInFuncParser.parser(_))
    p.get.value match {
      case URI(STRING("http://id.gsk.com/dm/1.0/", _)) => succeed
      case _                                           => fail
    }
  }

  "URI function with variable" should "return URI type" in {
    val s = "(uri ?test)"
    val p = fastparse.parse(s, BuildInFuncParser.parser(_))
    p.get.value match {
      case URI(VARIABLE("?test")) => succeed
      case _                      => fail
    }
  }

  "CONCAT function" should "return CONCAT type" in {
    val s = "(concat \"http://id.gsk.com/dm/1.0/\" ?src)"
    val p = fastparse.parse(s, BuildInFuncParser.parser(_))
    p.get.value match {
      case CONCAT(STRING("http://id.gsk.com/dm/1.0/", _), VARIABLE("?src")) =>
        succeed
      case _ => fail
    }
  }

  "Nested URI and CONCAT" should "return proper nested type" in {
    val s = "(uri (concat \"http://id.gsk.com/dm/1.0/\" ?src))"
    val p = fastparse.parse(s, BuildInFuncParser.parser(_))
    p.get.value match {
      case URI(
            CONCAT(STRING("http://id.gsk.com/dm/1.0/", _), VARIABLE("?src"))
          ) =>
        succeed
      case _ => fail
    }
  }

  "str function" should "return STR type" in {
    val s = "(str ?d)"
    val p = fastparse.parse(s, BuildInFuncParser.parser(_))
    p.get.value match {
      case STR(i: StringLike) => succeed
      case _                  => fail
    }
  }

  "strafter function" should "return STRAFTER type" in {
    val s = "(strafter ( str ?d) \"#\")"
    val p = fastparse.parse(s, BuildInFuncParser.parser(_))
    p.get.value match {
      case STRAFTER(STR(VARIABLE(s1: String)), STRING(s2: String, _)) => succeed
      case _                                                          => fail
    }
  }

  "Deeply nested strafter function" should "return nested STRAFTER type" in {
    val s = "(uri (strafter (concat (str ?d) (str ?src)) \"#\"))"
    val p = fastparse.parse(s, BuildInFuncParser.parser(_))
    p.get.value match {
      case URI(
            STRAFTER(
              CONCAT(STR(VARIABLE(a1: String)), STR(VARIABLE(a2: String))),
              STRING("#", _)
            )
          ) =>
        succeed
      case _ => fail
    }
  }

  "strstarts function" should "return STRSTARTS type" in {
    val s = """(strstarts (str ?modelname) "ner:")"""
    val p = fastparse.parse(s, BuildInFuncParser.parser(_))
    p.get.value match {
      case STRSTARTS(STR(VARIABLE("?modelname")), STRING("ner:", None)) =>
        succeed
      case _ => fail
    }
  }

  "Regex parser" should "return REGEX type" in {
    val p =
      fastparse.parse("""(regex ?d "Hello")""", BuildInFuncParser.regexParen(_))
    p.get.value match {
      case REGEX(VARIABLE("?d"), STRING("Hello", None)) => succeed
      case _                                            => fail
    }
  }
}
