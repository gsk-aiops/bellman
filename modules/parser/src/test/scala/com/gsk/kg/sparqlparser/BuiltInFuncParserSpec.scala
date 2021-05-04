package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.BuiltInFunc._
import com.gsk.kg.sparqlparser.StringVal._

import org.scalatest.flatspec.AnyFlatSpec

class BuiltInFuncParserSpec extends AnyFlatSpec {
  "URI function with string" should "return URI type" in {
    val s = "(uri \"http://id.gsk.com/dm/1.0/\")"
    val p = fastparse.parse(s, BuiltInFuncParser.parser(_))
    p.get.value match {
      case URI(STRING("http://id.gsk.com/dm/1.0/", _)) => succeed
      case _                                           => fail
    }
  }

  "URI function with variable" should "return URI type" in {
    val s = "(uri ?test)"
    val p = fastparse.parse(s, BuiltInFuncParser.parser(_))
    p.get.value match {
      case URI(VARIABLE("?test")) => succeed
      case _                      => fail
    }
  }

  "CONCAT function" should "return CONCAT type" in {
    val s = "(concat \"http://id.gsk.com/dm/1.0/\" ?src)"
    val p = fastparse.parse(s, BuiltInFuncParser.parser(_))
    p.get.value match {
      case CONCAT(STRING("http://id.gsk.com/dm/1.0/", _), VARIABLE("?src")) =>
        succeed
      case _ => fail
    }
  }

  "Nested URI and CONCAT" should "return proper nested type" in {
    val s = "(uri (concat \"http://id.gsk.com/dm/1.0/\" ?src))"
    val p = fastparse.parse(s, BuiltInFuncParser.parser(_))
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
    val p = fastparse.parse(s, BuiltInFuncParser.parser(_))
    p.get.value match {
      case STR(i: StringLike) => succeed
      case _                  => fail
    }
  }

  "strafter function" should "return STRAFTER type" in {
    val s = "(strafter ( str ?d) \"#\")"
    val p = fastparse.parse(s, BuiltInFuncParser.parser(_))
    p.get.value match {
      case STRAFTER(STR(VARIABLE(s1: String)), STRING(s2: String, _)) => succeed
      case _                                                          => fail
    }
  }

  "Deeply nested strafter function" should "return nested STRAFTER type" in {
    val s = "(uri (strafter (concat (str ?d) (str ?src)) \"#\"))"
    val p = fastparse.parse(s, BuiltInFuncParser.parser(_))
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

  "strbefore function" should "return STRBEFORE type" in {
    val s = "(strbefore ( str ?d) \"#\")"
    val p = fastparse.parse(s, BuiltInFuncParser.parser(_))
    p.get.value match {
      case STRBEFORE(STR(VARIABLE(s1: String)), STRING(s2: String, _)) =>
        succeed
      case _ => fail
    }
  }

  "substr function without length" should "return SUBSTR type" in {
    val s = "(substr ?d 1)"
    val p = fastparse.parse(s, BuiltInFuncParser.parser(_))
    p.get.value match {
      case SUBSTR(VARIABLE(s1: String), NUM(s2: String), None) =>
        succeed
      case _ => fail
    }
  }

  "substr function with length" should "return SUBSTR type" in {
    val s = "(substr ?d 1 1)"
    val p = fastparse.parse(s, BuiltInFuncParser.parser(_))
    p.get.value match {
      case SUBSTR(
            VARIABLE(s1: String),
            NUM(s2: String),
            Some(NUM(s3: String))
          ) =>
        succeed
      case _ => fail
    }
  }

  "Deeply nested strbefore function" should "return nested STRBEFORE type" in {
    val s = "(uri (strbefore (concat (str ?d) (str ?src)) \"#\"))"
    val p = fastparse.parse(s, BuiltInFuncParser.parser(_))
    p.get.value match {
      case URI(
            STRBEFORE(
              CONCAT(STR(VARIABLE(a1: String)), STR(VARIABLE(a2: String))),
              STRING("#", _)
            )
          ) =>
        succeed
      case _ => fail
    }
  }

  "strends function" should "return STRENDS type" in {
    val s = """(strends (str ?modelname) "ner:")"""
    val p = fastparse.parse(s, BuiltInFuncParser.parser(_))
    p.get.value match {
      case STRENDS(STR(VARIABLE("?modelname")), STRING("ner:", None)) =>
        succeed
      case _ => fail
    }
  }

  "strstarts function" should "return STRSTARTS type" in {
    val s = """(strstarts (str ?modelname) "ner:")"""
    val p = fastparse.parse(s, BuiltInFuncParser.parser(_))
    p.get.value match {
      case STRSTARTS(STR(VARIABLE("?modelname")), STRING("ner:", None)) =>
        succeed
      case _ => fail
    }
  }

  "strdt function" should "return STRDT type" in {
    val s =
      """(strdt (concat "?country=" (strafter ?country "foaf/0.1/") "&" (str ?long) "&" (str ?lat)) <http://geo.org/coords>)"""
    val p = fastparse.parse(s, BuiltInFuncParser.parser(_))
    p.get.value match {
      case STRDT(e1, e2) =>
        succeed
      case _ =>
        fail
    }
  }

  "Regex parser" should "return REGEX type" in {
    val p =
      fastparse.parse("""(regex ?d "Hello")""", BuiltInFuncParser.regexParen(_))
    p.get.value match {
      case REGEX(VARIABLE("?d"), STRING("Hello", None), _) =>
        succeed
      case _ => fail
    }
  }

  "Regex with flags parser" should "return REGEX type" in {
    val p =
      fastparse.parse(
        """(regex ?d "Hello" "i")""",
        BuiltInFuncParser.regexWithFlagsParen(_)
      )
    p.get.value match {
      case REGEX(VARIABLE("?d"), STRING("Hello", None), STRING("i", None)) =>
        succeed
      case _ => fail
    }
  }

  "Replace parser" should "return REPLACE type" in {
    val p =
      fastparse.parse(
        """(replace ?d "Hello" "Olleh")""",
        BuiltInFuncParser.replaceParen(_)
      )
    p.get.value match {
      case REPLACE(
            VARIABLE("?d"),
            STRING("Hello", None),
            STRING("Olleh", None),
            _
          ) =>
        succeed
      case _ => fail
    }
  }

  "Replace with flags parser" should "return REPLACE type" in {
    val p =
      fastparse.parse(
        """(replace ?d "Hello" "Olleh" "i")""",
        BuiltInFuncParser.replaceWithFlagsParen(_)
      )
    p.get.value match {
      case REPLACE(
            VARIABLE("?d"),
            STRING("Hello", None),
            STRING("Olleh", None),
            STRING("i", None)
          ) =>
        succeed
      case _ => fail
    }
  }
}
