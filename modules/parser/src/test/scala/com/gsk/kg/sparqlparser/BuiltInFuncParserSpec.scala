package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.BuiltInFunc._
import com.gsk.kg.sparqlparser.StringVal._

import org.scalatest.flatspec.AnyFlatSpec

class BuiltInFuncParserSpec extends AnyFlatSpec {
  "URI function with string" should "return URI type" in {
    val s = "(uri \"http://id.gsk.com/dm/1.0/\")"
    val p = fastparse.parse(s, BuiltInFuncParser.parser(_))
    p.get.value match {
      case URI(STRING("http://id.gsk.com/dm/1.0/")) => succeed
      case _                                        => fail
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
      case CONCAT(
            STRING("http://id.gsk.com/dm/1.0/"),
            _
          ) =>
        succeed
      case _ => fail
    }
  }

  it should "return COCANT type when multiple arguments" in {
    val s = "(concat \"http://id.gsk.com/dm/1.0/\" ?src ?dst)"
    val p = fastparse.parse(s, BuiltInFuncParser.parser(_))
    p.get.value match {
      case CONCAT(
            STRING("http://id.gsk.com/dm/1.0/"),
            List(VARIABLE("?src"), VARIABLE("?dst"))
          ) =>
        succeed
      case _ => fail
    }
  }

  "Nested URI and CONCAT" should "return proper nested type" in {
    val s = "(uri (concat \"http://id.gsk.com/dm/1.0/\" ?src))"
    val p = fastparse.parse(s, BuiltInFuncParser.parser(_))
    p.get.value match {
      case URI(
            CONCAT(
              STRING("http://id.gsk.com/dm/1.0/"),
              _
            )
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
      case STRAFTER(STR(VARIABLE(s1: String)), STRING(s2: String)) => succeed
      case _                                                       => fail
    }
  }

  "Deeply nested strafter function" should "return nested STRAFTER type" in {
    val s = "(uri (strafter (concat (str ?d) (str ?src)) \"#\"))"
    val p = fastparse.parse(s, BuiltInFuncParser.parser(_))
    p.get.value match {
      case URI(
            STRAFTER(
              CONCAT(STR(VARIABLE(a1: String)), _),
              STRING("#")
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
      case STRBEFORE(STR(VARIABLE(s1: String)), STRING(s2: String)) =>
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
              CONCAT(
                STR(VARIABLE(a1)),
                List(STR(VARIABLE(a2)))
              ),
              STRING("#")
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
      case STRENDS(STR(VARIABLE("?modelname")), STRING("ner:")) =>
        succeed
      case _ => fail
    }
  }

  "strstarts function" should "return STRSTARTS type" in {
    val s = """(strstarts (str ?modelname) "ner:")"""
    val p = fastparse.parse(s, BuiltInFuncParser.parser(_))
    p.get.value match {
      case STRSTARTS(STR(VARIABLE("?modelname")), STRING("ner:")) =>
        succeed
      case _ => fail
    }
  }

  "strdt function" should "return STRDT type" in {
    val s =
      """(strdt ?c <http://geo.org#country>)"""
    val p = fastparse.parse(s, BuiltInFuncParser.parser(_))
    p.get.value match {
      case STRDT(VARIABLE("?c"), URIVAL("<http://geo.org#country>")) =>
        succeed
      case _ =>
        fail
    }
  }

  "strlen function" should "return STRLEN type" in {
    val p =
      fastparse.parse("""(strlen ?d)""", BuiltInFuncParser.strlenParen(_))
    p.get.value match {
      case STRLEN(VARIABLE("?d")) => succeed
      case _                      => fail
    }
  }

  "Regex parser" should "return REGEX type" in {
    val p =
      fastparse.parse("""(regex ?d "Hello")""", BuiltInFuncParser.regexParen(_))
    p.get.value match {
      case REGEX(VARIABLE("?d"), STRING("Hello"), _) =>
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
      case REGEX(VARIABLE("?d"), STRING("Hello"), STRING("i")) =>
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
            STRING("Hello"),
            STRING("Olleh"),
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
            STRING("Hello"),
            STRING("Olleh"),
            STRING("i")
          ) =>
        succeed
      case _ => fail
    }
  }

  "LCASE parser" should "return LCASE type" in {
    val p =
      fastparse.parse("""(lcase ?d)""", BuiltInFuncParser.lcaseParen(_))
    p.get.value match {
      case LCASE(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }

  "UCASE parser" should "return UCASE type" in {
    val p =
      fastparse.parse("""(ucase ?d)""", BuiltInFuncParser.ucaseParen(_))
    p.get.value match {
      case UCASE(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }

  "ISLITERAL parser" should "return ISLITERAL type" in {
    val p =
      fastparse.parse("""(isLiteral ?d)""", BuiltInFuncParser.isLiteralParen(_))
    p.get.value match {
      case ISLITERAL(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }

  "ISNUMERIC parser" should "return ISNUMERIC type" in {
    val p =
      fastparse.parse("""(isNumeric ?d)""", BuiltInFuncParser.isNumericParen(_))
    p.get.value match {
      case ISNUMERIC(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }

  "ENCODE_FOR_URI parser" should "return ENCODE_FOR_URI type" in {
    val p =
      fastparse.parse(
        """(encode_for_uri ?d)""",
        BuiltInFuncParser.encodeForURIParen(_)
      )
    p.get.value match {
      case ENCODE_FOR_URI(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }

  "LANG parser" should "return LANG type" in {
    val p =
      fastparse.parse("""(lang ?d)""", BuiltInFuncParser.langParen(_))
    p.get.value match {
      case LANG(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }

  "LANGMATCHES parser" should "return LANGMATCHES type" in {
    val p =
      fastparse.parse(
        """(langMatches ?d "FR")""",
        BuiltInFuncParser.langMatchesParen(_)
      )
    p.get.value match {
      case LANGMATCHES(VARIABLE("?d"), STRING("FR")) =>
        succeed
      case _ => fail
    }
  }

  "MD5 parser with variable" should "return MD5 type" in {
    val p =
      fastparse.parse(
        """(md5 ?d)""",
        BuiltInFuncParser.md5Paren(_)
      )
    p.get.value match {
      case MD5(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }

  "MD5 parser with string" should "return MD5 type" in {
    val p =
      fastparse.parse(
        """(md5 "x")""",
        BuiltInFuncParser.md5Paren(_)
      )
    p.get.value match {
      case MD5(STRING("x")) =>
        succeed
      case _ => fail
    }
  }

  "SHA1 parser with variable" should "return SHA1 type" in {
    val p =
      fastparse.parse(
        """(sha1 ?d)""",
        BuiltInFuncParser.sha1Paren(_)
      )
    p.get.value match {
      case SHA1(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }

  "SHA1 parser with string" should "return SHA1 type" in {
    val p =
      fastparse.parse(
        """(sha1 "x")""",
        BuiltInFuncParser.sha1Paren(_)
      )
    p.get.value match {
      case SHA1(STRING("x")) =>
        succeed
      case _ => fail
    }
  }

  "SHA256 parser with variable" should "return SHA256 type" in {
    val p =
      fastparse.parse(
        """(sha256 ?d)""",
        BuiltInFuncParser.sha256Paren(_)
      )
    p.get.value match {
      case SHA256(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }

  "SHA256 parser with string" should "return SHA256 type" in {
    val p =
      fastparse.parse(
        """(sha256 "x")""",
        BuiltInFuncParser.sha256Paren(_)
      )
    p.get.value match {
      case SHA256(STRING("x")) =>
        succeed
      case _ => fail
    }
  }

  "SHA384 parser with variable" should "return SHA384 type" in {
    val p =
      fastparse.parse(
        """(sha384 ?d)""",
        BuiltInFuncParser.sha384Paren(_)
      )
    p.get.value match {
      case SHA384(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }

  "SHA384 parser with string" should "return SHA384 type" in {
    val p =
      fastparse.parse(
        """(sha384 "x")""",
        BuiltInFuncParser.sha384Paren(_)
      )
    p.get.value match {
      case SHA384(STRING("x")) =>
        succeed
      case _ => fail
    }
  }

  "SHA512 parser with variable" should "return SHA512 type" in {
    val p =
      fastparse.parse(
        """(sha512 ?d)""",
        BuiltInFuncParser.sha512Paren(_)
      )
    p.get.value match {
      case SHA512(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }

  "SHA512 parser with string" should "return SHA512 type" in {
    val p =
      fastparse.parse(
        """(sha512 "x")""",
        BuiltInFuncParser.sha512Paren(_)
      )
    p.get.value match {
      case SHA512(STRING("x")) =>
        succeed
      case _ => fail
    }
  }

  "UUID parser" should "return UUID type" in {
    val p =
      fastparse.parse(
        """(uuid)""",
        BuiltInFuncParser.uuidParen(_)
      )
    p.get.value match {
      case UUID() =>
        succeed
      case _ => fail
    }
  }

  "CEIL parser with string" should "return CEIL type" in {
    val p =
      fastparse.parse(
        """(ceil "x")""",
        BuiltInFuncParser.ceilParen(_)
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
        BuiltInFuncParser.ceilParen(_)
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
        BuiltInFuncParser.roundParen(_)
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
        BuiltInFuncParser.roundParen(_)
      )
    p.get.value match {
      case ROUND(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }

  "ABS parser with string" should "return ABS type" in {
    val p =
      fastparse.parse(
        """(abs "x")""",
        BuiltInFuncParser.absParen(_)
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
        BuiltInFuncParser.absParen(_)
      )
    p.get.value match {
      case ABS(VARIABLE("?d")) =>
        succeed
      case _ => fail
    }
  }
}
