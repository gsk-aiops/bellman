package com.gsk.kg.sparqlparser

import fastparse.CharIn
import fastparse.P
import org.scalatest.wordspec.AnyWordSpec

class PropertyExpressionParserSpec extends AnyWordSpec {

  "Property Expression parser" when {

    "simple property paths expressions" should {

      "return Uri type" in {

        val p = fastparse.parse(
          """<http://example.org/Alice>""",
          PropertyPathParser.uri(_)
        )

        p.get.value match {
          case Uri("<http://example.org/Alice>") => succeed
          case _                                 => fail
        }
      }

      "return Alt type" in {

        val p = fastparse.parse(
          """(alt <http://purl.org/dc/elements/1.1/title> <http://www.w3.org/2000/01/rdf-schema#label>)""",
          PropertyPathParser.alternativeParen(_)
        )

        p.get.value match {
          case Alternative(
                Uri("<http://purl.org/dc/elements/1.1/title>"),
                Uri("<http://www.w3.org/2000/01/rdf-schema#label>")
              ) =>
            succeed
          case _ => fail
        }
      }

      "return Reverse type" in {

        val p = fastparse.parse(
          """(rev <http://www.w3.org/2000/01/rdf-schema#label>)""",
          PropertyPathParser.reverseParen(_)
        )

        p.get.value match {
          case Reverse(Uri("<http://www.w3.org/2000/01/rdf-schema#label>")) =>
            succeed
          case _ => fail
        }
      }

      "return SeqExpression type" in {

        val p = fastparse.parse(
          """(seq (seq <http://xmlns.org/foaf/0.1/knows> <http://xmlns.org/foaf/0.1/knows>) <http://xmlns.org/foaf/0.1/name>)""",
          PropertyPathParser.sequenceParen(_)
        )

        p.get.value match {
          case SeqExpression(
                SeqExpression(
                  Uri("<http://xmlns.org/foaf/0.1/knows>"),
                  Uri("<http://xmlns.org/foaf/0.1/knows>")
                ),
                Uri("<http://xmlns.org/foaf/0.1/name>")
              ) =>
            succeed
          case _ => fail
        }
      }

      "return OneOrMore type" in {

        val p = fastparse.parse(
          """(path+ <http://purl.org/dc/elements/1.1/title>)""",
          PropertyPathParser.oneOrMoreParen(_)
        )

        p.get.value match {
          case OneOrMore(Uri("<http://purl.org/dc/elements/1.1/title>")) =>
            succeed
          case _ => fail
        }
      }

      "return ZeroOrMore type" in {

        val p = fastparse.parse(
          """(path* <http://purl.org/dc/elements/1.1/title>)""",
          PropertyPathParser.zeroOrMoreParen(_)
        )

        p.get.value match {
          case ZeroOrMore(Uri("<http://purl.org/dc/elements/1.1/title>")) =>
            succeed
          case _ => fail
        }
      }

      "return ZeroOrOne type" in {

        val p = fastparse.parse(
          """(path? <http://purl.org/dc/elements/1.1/title>)""",
          PropertyPathParser.zeroOrOneParen(_)
        )

        p.get.value match {
          case ZeroOrOne(Uri("<http://purl.org/dc/elements/1.1/title>")) =>
            succeed
          case _ => fail
        }
      }

      "return NotOneOf type" in {

        val p = fastparse.parse(
          """(notoneof <http://purl.org/dc/elements/1.1/title> <http://www.w3.org/2000/01/rdf-schema#label>)""",
          PropertyPathParser.notOneOfParen(_)
        )

        p.get.value match {
          case NotOneOf(
                List(
                  Uri("<http://purl.org/dc/elements/1.1/title>"),
                  Uri("<http://www.w3.org/2000/01/rdf-schema#label>")
                )
              ) =>
            succeed
          case _ => fail
        }
      }

      "return BetweenNAndM type" in {

        val p = fastparse.parse(
          """(mod 1 2 <http://xmlns.org/foaf/0.1/knows>)""",
          PropertyPathParser.betweenNAndMParen(_),
          verboseFailures = true
        )

        p.get.value match {
          case BetweenNAndM(
                1,
                2,
                Uri("<http://xmlns.org/foaf/0.1/knows>")
              ) =>
            succeed
          case _ => fail
        }
      }

      "return ExactlyN type" in {

        val p = fastparse.parse(
          """(pathN 2 <http://xmlns.org/foaf/0.1/knows>)""",
          PropertyPathParser.exactlyNParen(_),
          verboseFailures = true
        )

        p.get.value match {
          case ExactlyN(
                2,
                Uri("<http://xmlns.org/foaf/0.1/knows>")
              ) =>
            succeed
          case _ => fail
        }
      }

      "return NOrMore type" in {

        val p = fastparse.parse(
          """(mod 1 _ <http://xmlns.org/foaf/0.1/knows>)""",
          PropertyPathParser.nOrMoreParen(_),
          verboseFailures = true
        )

        p.get.value match {
          case NOrMore(
                1,
                Uri("<http://xmlns.org/foaf/0.1/knows>")
              ) =>
            succeed
          case _ => fail
        }
      }

      "return BetweenZeroAndN type" in {

        val p = fastparse.parse(
          """(mod _ 2 <http://xmlns.org/foaf/0.1/knows>)""",
          PropertyPathParser.betweenNZeroAndN(_),
          verboseFailures = true
        )

        p.get.value match {
          case BetweenZeroAndN(
                2,
                Uri("<http://xmlns.org/foaf/0.1/knows>")
              ) =>
            succeed
          case _ => fail
        }
      }

//      "test" in {
//        import fastparse._
//        import fastparse.MultiLineWhitespace._
//
//        val s1 = "55"
//        val s2 = "15 2 10 38"
//
//        def number[_: P]: P[Int] = {
//          import fastparse.NoWhitespace._
//          P(CharIn("0-9").rep(1).!.map(_.toInt))
//        }
//
//        def numbers[_: P]: P[Seq[Int]] =
//          P(number.rep(4, " "))
//
//        val a = fastparse.parse(s1, number(_))
//        a
//        val p = fastparse.parse(s2, numbers(_), verboseFailures = true)
//        p
//      }
    }

    "complex property paths expressions" should {

      "complex query 1" in {}
    }
  }
}
