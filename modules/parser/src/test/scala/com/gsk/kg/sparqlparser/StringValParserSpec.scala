package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.StringVal._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StringValParserSpec extends AnyFlatSpec with Matchers {
  "parse the string literal with tag" should "create proper STRING cass class" in {
    val s = "\"abc\"@en"
    val p = fastparse.parse(s, StringValParser.tripleValParser(_))
    p.get.value match {
      case LANG_STRING("abc", "en") => succeed
      case _                        => fail
    }
    val s1 = "\"345\"^^<http://www.w3.org/2001/XMLSchema#xsd:integer>"
    val p1 = fastparse.parse(s1, StringValParser.tripleValParser(_))
    p1.get.value match {
      case DT_STRING(
            "345",
            "<http://www.w3.org/2001/XMLSchema#xsd:integer>"
          ) =>
        succeed
      case _ => fail
    }
    val s2 = "\"hello!\""
    val p2 = fastparse.parse(s2, StringValParser.tripleValParser(_))
    p2.get.value match {
      case STRING(s) => succeed
      case _         => fail
    }
  }

  "parse variable" should "create proper VARIABLE cass class" in {
    val s = "?ab_cd"
    val p = fastparse.parse(s, StringValParser.tripleValParser(_))
    p.get.value match {
      case VARIABLE("?ab_cd") => succeed
      case _                  => fail
    }
  }

  it should "work with blank node variables generated by Jena" in {
    val s = "??d"
    val p = fastparse.parse(s, StringValParser.tripleValParser(_))
    p.get.value match {
      case VARIABLE("??d") => succeed
      case _               => fail
    }
  }

  it should "work with aggregation variables generated by Jena" in {
    val s = "?1"
    val p = fastparse.parse(s, StringValParser.tripleValParser(_))
    p.get.value match {
      case VARIABLE("?1") => succeed
      case _              => fail
    }
  }

  "parse uri" should "create proper URIVAL cass class" in {
    val s = "<http://id.gsk.com/dm/1.0/>"
    val p = fastparse.parse(s, StringValParser.tripleValParser(_))
    p.get.value match {
      case URIVAL("<http://id.gsk.com/dm/1.0/>") => succeed
      case _                                     => fail
    }
  }

  "parse number" should "create proper NUM cass class" in {
    val s = "-123.456"
    val p = fastparse.parse(s, StringValParser.tripleValParser(_))
    p.get.value match {
      case NUM("-123.456") => succeed
      case _               => fail
    }
  }

  "parse blank node" should "create proper BLANK cass class" in {
    val s = "_:iamblank"
    val p = fastparse.parse(s, StringValParser.tripleValParser(_))
    p.get.value match {
      case BLANK("iamblank") => succeed
      case _                 => fail
    }
  }

  "parse boolean" should "create proper BOOL cass class" in {
    val s = "false"
    val p = fastparse.parse(s, StringValParser.tripleValParser(_))
    p.get.value match {
      case BOOL("false") => succeed
      case _             => fail
    }
  }

  "parse string" should "parse nonempty strings correctly" in {
    val strings: List[(String, StringVal.STRING)] =
      List(
        ("\"hello\"", StringVal.STRING("hello")),
        ("\"hello goodbye\"", StringVal.STRING("hello goodbye"))
      )

    strings foreach { case (str, expected) =>
      val p = fastparse.parse(str, StringValParser.plainString(_))

      p.get.value shouldEqual expected
    }
  }

  it should "parse strings containing whitespace correctly" in {
    val strings: List[(String, StringVal.STRING)] =
      List(
        ("\" hello\"", StringVal.STRING(" hello")),
        ("\" hello goodbye \"", StringVal.STRING(" hello goodbye "))
      )

    strings foreach { case (str, expected) =>
      val p = fastparse.parse(str, StringValParser.plainString(_))

      p.get.value shouldEqual expected
    }
  }

  "parse typed literal" should "parse nonempty typed literals correctly" in {
    val strings: List[(String, StringVal.DT_STRING)] =
      List(
        (
          "\"hello\"^^<http://example.org/string>",
          StringVal.DT_STRING("hello", "<http://example.org/string>")
        ),
        (
          "\"hello goodbye\"^^<http://example.org/string>",
          StringVal.DT_STRING("hello goodbye", "<http://example.org/string>")
        )
      )

    strings foreach { case (str, expected) =>
      val p = fastparse.parse(str, StringValParser.dataTypeString(_))

      p.get.value shouldEqual expected
    }
  }

  it should "parse strings containing whitespace correctly" in {
    val strings: List[(String, StringVal.DT_STRING)] =
      List(
        (
          "\" hello\"^^<http://example.org/string>",
          StringVal.DT_STRING(" hello", "<http://example.org/string>")
        ),
        (
          "\" hello goodbye \"^^<http://example.org/string>",
          StringVal.DT_STRING(" hello goodbye ", "<http://example.org/string>")
        )
      )

    strings foreach { case (str, expected) =>
      val p = fastparse.parse(str, StringValParser.dataTypeString(_))

      p.get.value shouldEqual expected
    }
  }

  "parse l10n string" should "parse nonempty l10n strings correctly" in {
    val strings: List[(String, StringVal.LANG_STRING)] =
      List(
        ("\"hello\"@en", StringVal.LANG_STRING("hello", "en")),
        ("\"hello goodbye\"@en", StringVal.LANG_STRING("hello goodbye", "en"))
      )

    strings foreach { case (str, expected) =>
      val p = fastparse.parse(str, StringValParser.langString(_))

      p.get.value shouldEqual expected
    }
  }

  it should "parse l10n strings with whitespace correctly" in {
    val strings: List[(String, StringVal.LANG_STRING)] =
      List(
        ("\" hello\"@en", StringVal.LANG_STRING(" hello", "en")),
        (
          "\" hello goodbye \"@en",
          StringVal.LANG_STRING(" hello goodbye ", "en")
        )
      )

    strings foreach { case (str, expected) =>
      val p = fastparse.parse(str, StringValParser.langString(_))

      p.get.value shouldEqual expected
    }
  }

  "parse blank node" should "work correctly" in {
    val strings = List(
      (
        "_:Ncdddad69172c41bf841e9974021ec93c",
        StringVal.BLANK("Ncdddad69172c41bf841e9974021ec93c")
      ),
      (
        "_:Nd399f2f35123439ab23ffa6f842467a7",
        StringVal.BLANK("Nd399f2f35123439ab23ffa6f842467a7")
      ),
      (
        "_:Nf40a89247d7d46a99bdc6302aac23dd4",
        StringVal.BLANK("Nf40a89247d7d46a99bdc6302aac23dd4")
      ),
      (
        "_:N91f8124526c2476c9d06f5854727c7fc",
        StringVal.BLANK("N91f8124526c2476c9d06f5854727c7fc")
      ),
      (
        "_:Nb757b546a3454984a69a66bcd3a4b6c1",
        StringVal.BLANK("Nb757b546a3454984a69a66bcd3a4b6c1")
      )
    )

    strings foreach { case (str, expected) =>
      val p = fastparse.parse(str, StringValParser.blankNode(_))

      p.get.value shouldEqual expected
    }

  }
}
