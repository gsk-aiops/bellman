package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.BuiltInFunc._
import com.gsk.kg.sparqlparser.ConditionOrder.ASC
import com.gsk.kg.sparqlparser.ConditionOrder.DESC
import com.gsk.kg.sparqlparser.Conditional._
import com.gsk.kg.sparqlparser.Expr._
import com.gsk.kg.sparqlparser.PropertyExpression._
import com.gsk.kg.sparqlparser.StringVal._
import org.scalatest.wordspec.AnyWordSpec

class ExprParserSpec extends AnyWordSpec with TestUtils {

  "Basic Graph Pattern" should {

    "parse correct number of Triples" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/q0-simple-basic-graph-pattern.sparql"),
        ExprParser.parser(_)
      )
      p.get.value match {
        case BGP(triples) => assert(triples.length == 2)
        case _            => fail
      }
    }
  }

  "Single optional" should {

    "result in single leftjoin" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/q1-single-leftjoin.sparql"),
        ExprParser.parser(_)
      )
      p.get.value match {
        case LeftJoin(l: BGP, r: BGP) => succeed
        case _                        => fail
      }
    }
  }

  "Double optional" should {

    "result in nested leftjoin" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/q2-nested-leftjoins.sparql"),
        ExprParser.parser(_)
      )
      p.get.value match {
        case LeftJoin(l: LeftJoin, r: BGP) => succeed
        case _                             => fail
      }
    }
  }

  "Single Union" should {

    "result in a single nested union" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/q3-union.sparql"),
        ExprParser.parser(_)
      )
      p.get.value match {
        case Union(BGP(triplesL: Seq[Quad]), BGP(triplesR: Seq[Quad])) =>
          succeed
        case _ => fail
      }
    }
  }
  "Single Bind" should {

    "result in a successful extend instruction" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/q4-simple-bind.sparql"),
        ExprParser.parser(_)
      )
      p.get.value match {
        case Extend(l: StringVal, r: StringVal, BGP(triples: Seq[Quad])) =>
          succeed
        case _ => fail
      }
    }
  }

  "Single union plus bind" should {

    "result in a successful extend and union instruction" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/q5-union-plus-bind.sparql"),
        ExprParser.parser(_)
      )
      p.get.value match {
        case Union(
              Extend(l: StringVal, r: StringVal, BGP(triples1: Seq[Quad])),
              BGP(triples2: Seq[Quad])
            ) =>
          succeed
        case _ => fail
      }
    }
  }

  "Nested leftjoin, nested union, multiple binds" should {

    "result in successful nestings" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/q6-nested-leftjoin-union-bind.sparql"),
        ExprParser.parser(_)
      )

      p.get.value match {
        case Union(
              Union(
                Extend(
                  s1: StringVal,
                  s2: StringVal,
                  LeftJoin(
                    LeftJoin(BGP(l1: Seq[Quad]), BGP(l2: Seq[Quad])),
                    BGP(l3: Seq[Quad])
                  )
                ),
                BGP(l4: Seq[Quad])
              ),
              Extend(
                s3: StringVal,
                s4: StringVal,
                LeftJoin(BGP(l5: Seq[Quad]), BGP(l6: Seq[Quad]))
              )
            ) =>
          succeed
        case _ => fail
      }
    }
  }

  "Nested bind" should {

    "Result in correct nesting of bind" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/q7-nested-bind.sparql"),
        ExprParser.parser(_)
      )
      p.get.value match {
        case Extend(
              s1: StringVal,
              s2: StringVal,
              Extend(s3: StringVal, s4: StringVal, BGP(l1: Seq[Quad]))
            ) =>
          succeed
        case _ => fail
      }
    }
  }

  "Filter over simple BGP" should {

    "Result in correct nesting of filter and BGP" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/q8-filter-simple-basic-graph.sparql"),
        ExprParser.parser(_)
      )
      p.get.value match {
        case Filter(s1: Seq[Expression], b: BGP) => succeed
        case _                                   => fail
      }
    }
  }

  "Multiple filters over simple BGP" should {

    "Result in correct nesting of filters and BGP" in {
      val p = fastparse.parse(
        sparql2Algebra(
          "/queries/q9-double-filter-simple-basic-graph.sparql"
        ),
        ExprParser.parser(_)
      )
      p.get.value match {
        case Filter(
              Seq(
                EQUALS(sl1: StringLike, sl2: StringLike),
                REGEX(sl3: StringLike, sl4: StringLike, sl15: StringLike)
              ),
              b: BGP
            ) =>
          succeed
        case _ => fail
      }
    }
  }

  "Complex filters" should {

    "Result in the correct nesting" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/q10-complex-filter.sparql"),
        ExprParser.parser(_)
      )
      p.get.value match {
        case Filter(
              seq1: Seq[Expression],
              Union(
                Union(
                  Filter(
                    seq2: Seq[Expression],
                    Extend(
                      s1: StringVal,
                      s2: StringVal,
                      LeftJoin(
                        LeftJoin(BGP(seq3: Seq[Quad]), BGP(seq4: Seq[Quad])),
                        BGP(seq5: Seq[Quad])
                      )
                    )
                  ),
                  BGP(seq6: Seq[Quad])
                ),
                Extend(
                  s3: StringVal,
                  s4: StringVal,
                  LeftJoin(BGP(seq7: Seq[Quad]), BGP(seq8: Seq[Quad]))
                )
              )
            ) =>
          succeed
        case _ => fail
      }
    }
  }

  "Simple named graph query" should {

    "Return correct named graph algebra" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/q11-simple-named-graph.sparql"),
        ExprParser.parser(_)
      )
      p.get.value match {
        case Join(Graph(ng1: URIVAL, BGP(s1: Seq[Quad])), BGP(s2: Seq[Quad])) =>
          succeed
        case _ => fail
      }
    }
  }

  "Double named graph query" should {

    "Return correct named graph algebra" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/q12-double-named-graph.sparql"),
        ExprParser.parser(_)
      )
      p.get.value match {
        case Join(
              Graph(ng1: URIVAL, BGP(s1: Seq[Quad])),
              Graph(ng2: URIVAL, BGP(s2: Seq[Quad]))
            ) =>
          succeed
        case _ => fail
      }
    }
  }

  "Complex named graph query" should {

    "Return correct named graph algebra" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/q13-complex-named-graph.sparql"),
        ExprParser.parser(_)
      )
      p.get.value match {
        case Filter(
              seq1: Seq[Expression],
              Union(
                Union(
                  Graph(
                    g1: URIVAL,
                    Filter(
                      seq2: Seq[Expression],
                      Extend(
                        s1: StringVal,
                        s2: StringVal,
                        LeftJoin(
                          LeftJoin(BGP(seq3: Seq[Quad]), BGP(seq4: Seq[Quad])),
                          BGP(seq5: Seq[Quad])
                        )
                      )
                    )
                  ),
                  BGP(seq6: Seq[Quad])
                ),
                Extend(
                  s3: StringVal,
                  s4: StringVal,
                  LeftJoin(
                    Join(
                      Graph(g2: URIVAL, BGP(seq7: Seq[Quad])),
                      BGP(seq8: Seq[Quad])
                    ),
                    BGP(seq9: Seq[Quad])
                  )
                )
              )
            ) =>
          succeed
        case _ => fail
      }
    }
  }

  "Simple nested string function query" should {

    "return proper nested type" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/q15-string-functions-nested.sparql"),
        ExprParser.parser(_)
      )
      p.get.value match {
        case Extend(
              VARIABLE(s1: String),
              URI(
                STRAFTER(
                  CONCAT(STR(VARIABLE(s2: String)), _),
                  STRING("#")
                )
              ),
              BGP(l1: Seq[Quad])
            ) =>
          succeed
        case _ => fail
      }
    }
  }

  "Nested filter function" should {

    "return proper nested types" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/q17-filter-nested.sparql"),
        ExprParser.parser(_)
      )
      p.get.value match {
        case Filter(
              Seq(
                STRSTARTS(STR(VARIABLE("?src")), STRING("ner:")),
                GT(VARIABLE("?year"), STRING("2015"))
              ),
              BGP(
                Seq(
                  Quad(
                    VARIABLE("?d"),
                    URIVAL("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"),
                    URIVAL("<http://gsk-kg.rdip.gsk.com/dm/1.0/Document>"),
                    _
                  ),
                  Quad(
                    VARIABLE("?d"),
                    URIVAL("<http://gsk-kg.rdip.gsk.com/dm/1.0/docSource>"),
                    VARIABLE("?src"),
                    _
                  )
                )
              )
            ) =>
          succeed
        case _ => fail
      }
    }
  }

  "Simple > FILTER query" should {

    "return the proper type" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/q18-filter-gt.sparql"),
        ExprParser.parser(_)
      )
      p.get.value match {
        case Filter(
              List(GT(VARIABLE("?year"), STRING("2015"))),
              BGP(l1: Seq[Quad])
            ) =>
          succeed
        case _ => fail
      }
    }
  }

  "Simple < FILTER query" should {

    "return the proper type" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/q19-filter-lt.sparql"),
        ExprParser.parser(_)
      )
      p.get.value match {
        case Filter(
              List(LT(VARIABLE("?year"), STRING("2015"))),
              BGP(l1: Seq[Quad])
            ) =>
          succeed
        case _ => fail
      }
    }
  }

  "GroupBy query" should {

    "return the proper type" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/q38-groupby.sparql"),
        ExprParser.parser(_)
      )
      p.get.value match {
        case Project(
              Seq(VARIABLE("?p1")),
              Group(Seq(VARIABLE("?p1")), seq, BGP(_))
            ) if seq.isEmpty =>
          succeed
        case _ => fail
      }
    }

    "capture all variables" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/q39-groupby-two-vars.sparql"),
        ExprParser.parser(_)
      )
      p.get.value match {
        case Project(
              Seq(VARIABLE("?p1"), VARIABLE("?p2")),
              Group(Seq(VARIABLE("?p1"), VARIABLE("?p2")), seq, BGP(_))
            ) if seq.isEmpty =>
          succeed
        case _ => fail
      }
    }

    "capture aggregate functions correctly" in {
      val p = fastparse.parse(
        sparql2Algebra(
          "/queries/q40-groupby-aggregate-functions.sparql"
        ),
        ExprParser.parser(_)
      )

      p.get.value match {
        case Project(
              Seq(VARIABLE("?1")),
              Extend(
                VARIABLE("?1"),
                VARIABLE("?0"),
                Group(
                  Seq(VARIABLE("?p1")),
                  Seq((VARIABLE("?0"), Aggregate.COUNT(VARIABLE("?p1")))),
                  BGP(
                    Seq(
                      Quad(
                        VARIABLE("?p1"),
                        URIVAL("<http://www.w3.org/2006/vcard/ns#hasAddress>"),
                        VARIABLE("?ad"),
                        List(GRAPH_VARIABLE)
                      ),
                      Quad(
                        VARIABLE("?p2"),
                        URIVAL("<http://www.w3.org/2006/vcard/ns#hasAddress>"),
                        VARIABLE("?ad"),
                        List(GRAPH_VARIABLE)
                      )
                    )
                  )
                )
              )
            ) =>
          succeed
        case _ => fail
      }
    }

    "parse correctly when there's no explicit GROUP BY expression" in {
      val p = fastparse.parse(
        sparql2Algebra(
          "/queries/q41-groupby-implicit.sparql"
        ),
        ExprParser.parser(_)
      )

      p.get.value match {
        case Project(
              Seq(VARIABLE("?count")),
              Extend(
                VARIABLE("?count"),
                VARIABLE("?0"),
                Group(
                  Seq(),
                  Seq((VARIABLE("?0"), Aggregate.COUNT(VARIABLE("?s")))),
                  BGP(
                    Seq(
                      Quad(
                        VARIABLE("?s"),
                        URIVAL(
                          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
                        ),
                        URIVAL(
                          "<http://gsk-kg.rdip.gsk.com/dm/1.0/LinkedEntity>"
                        ),
                        List(GRAPH_VARIABLE)
                      )
                    )
                  )
                )
              )
            ) =>
          succeed
        case _ => fail
      }
    }

    "work correctly when there's more than one aggregate function" in {
      val query = """(group
      ()
      ((?.0 (count ?pred)) (?.1 (min ?year)))
      (bgp
        (triple ?pred
                <http://example.com/year>
                ?year)))
    """

      val p = fastparse
        .parse(
          query,
          x => ExprParser.groupParen(x)
        )

      p.get.value match {
        case Group(
              Seq(),
              Seq(
                (VARIABLE("?0"), Aggregate.COUNT(VARIABLE("?pred"))),
                (VARIABLE("?1"), Aggregate.MIN(VARIABLE("?year")))
              ),
              BGP(
                Seq(
                  Quad(
                    VARIABLE("?pred"),
                    URIVAL("<http://example.com/year>"),
                    VARIABLE("?year"),
                    List(GRAPH_VARIABLE)
                  )
                )
              )
            ) =>
          succeed
        case _ =>
          fail("this query should parse to Group")
      }
    }
  }

  "Minus" should {

    "parse correctly" in {
      val query = """(minus
          (bgp (triple ?s ?p ?o))
          (bgp (triple ?x ?y ?z)))
        """

      val p = fastparse
        .parse(
          query,
          x => ExprParser.minusParen(x)
        )

      p.get.value match {
        case Minus(
              BGP(
                Seq(
                  Quad(
                    VARIABLE("?s"),
                    VARIABLE("?p"),
                    VARIABLE("?o"),
                    List(GRAPH_VARIABLE)
                  )
                )
              ),
              BGP(
                Seq(
                  Quad(
                    VARIABLE("?x"),
                    VARIABLE("?y"),
                    VARIABLE("?z"),
                    List(GRAPH_VARIABLE)
                  )
                )
              )
            ) =>
          succeed
        case _ =>
          fail("this query should parse to Minus")
      }
    }
  }

  "Order By" should {

    "return proper type when simple variable" in {
      val p = fastparse.parse(
        sparql2Algebra(
          "/queries/q41-orderby-simple-variable.sparql"
        ),
        ExprParser.parser(_)
      )

      p.get.value match {
        case Project(
              Seq(VARIABLE("?name")),
              Order(
                Seq(ASC(VARIABLE("?name"))),
                BGP(
                  Seq(
                    Quad(
                      VARIABLE("?x"),
                      URIVAL("<http://xmlns.com/foaf/0.1/name>"),
                      VARIABLE("?name"),
                      List(GRAPH_VARIABLE)
                    )
                  )
                )
              )
            ) =>
          succeed
        case _ => fail
      }
    }

    "return proper type when simple condition" in {
      val p = fastparse.parse(
        sparql2Algebra(
          "/queries/q42-orderby-simple-condition.sparql"
        ),
        ExprParser.parser(_)
      )

      p.get.value match {
        case Project(
              Seq(VARIABLE("?name")),
              Order(
                Seq(DESC(ISBLANK(VARIABLE("?x")))),
                BGP(
                  Seq(
                    Quad(
                      VARIABLE("?x"),
                      URIVAL("<http://xmlns.com/foaf/0.1/name>"),
                      VARIABLE("?name"),
                      List(GRAPH_VARIABLE)
                    ),
                    Quad(
                      VARIABLE("?x"),
                      URIVAL("<http://xmlns.com/foaf/0.1/age>"),
                      VARIABLE("?age"),
                      List(GRAPH_VARIABLE)
                    )
                  )
                )
              )
            ) =>
          succeed
        case _ => fail
      }
    }

    "return proper type when mixing multiple variables and multiple conditions" in {
      val p = fastparse.parse(
        sparql2Algebra(
          "/queries/q43-orderby-multi-mixing.sparql"
        ),
        ExprParser.parser(_)
      )

      p.get.value match {
        case Project(
              Seq(VARIABLE("?name")),
              Order(
                Seq(
                  DESC(VARIABLE("?name")),
                  ASC(VARIABLE("?age")),
                  DESC(VARIABLE("?age")),
                  ASC(VARIABLE("?name")),
                  DESC(
                    OR(
                      ISBLANK(VARIABLE("?x")),
                      ISBLANK(VARIABLE("?age"))
                    )
                  )
                ),
                BGP(
                  Seq(
                    Quad(
                      VARIABLE("?x"),
                      URIVAL("<http://xmlns.com/foaf/0.1/name>"),
                      VARIABLE("?name"),
                      List(GRAPH_VARIABLE)
                    ),
                    Quad(
                      VARIABLE("?x"),
                      URIVAL("<http://xmlns.com/foaf/0.1/age>"),
                      VARIABLE("?age"),
                      List(GRAPH_VARIABLE)
                    )
                  )
                )
              )
            ) =>
          succeed
        case _ => fail
      }
    }
  }

  "Values" should {

    "return proper type when simple variable" in {

      val p = fastparse.parse(
        sparql2Algebra(
          "/queries/q44-values-simple-variable.sparql"
        ),
        ExprParser.parser(_)
      )

      p.get.value match {
        case Project(
              Seq(VARIABLE("?book"), VARIABLE("?title"), VARIABLE("?price")),
              Join(
                Table(
                  Seq(VARIABLE("?book")),
                  Seq(
                    Row(
                      Seq(
                        (
                          VARIABLE("?book"),
                          URIVAL("<http://example.org/book/book1>")
                        )
                      )
                    ),
                    Row(
                      Seq(
                        (
                          VARIABLE("?book"),
                          URIVAL("<http://example.org/book/book3>")
                        )
                      )
                    )
                  )
                ),
                BGP(
                  Seq(
                    Quad(
                      VARIABLE("?book"),
                      URIVAL("<http://purl.org/dc/elements/1.1/title>"),
                      VARIABLE("?title"),
                      List(GRAPH_VARIABLE)
                    ),
                    Quad(
                      VARIABLE("?book"),
                      URIVAL("<http://example.org/ns#price>"),
                      VARIABLE("?price"),
                      List(GRAPH_VARIABLE)
                    )
                  )
                )
              )
            ) =>
          succeed
        case _ => fail
      }
    }

    "return proper type when multiple variables" in {

      val p = fastparse.parse(
        sparql2Algebra(
          "/queries/q45-values-multiple-variables.sparql"
        ),
        ExprParser.parser(_)
      )

      p.get.value match {
        case Project(
              Seq(VARIABLE("?book"), VARIABLE("?title"), VARIABLE("?price")),
              Join(
                BGP(
                  Seq(
                    Quad(
                      VARIABLE("?book"),
                      URIVAL("<http://purl.org/dc/elements/1.1/title>"),
                      VARIABLE("?title"),
                      List(GRAPH_VARIABLE)
                    ),
                    Quad(
                      VARIABLE("?book"),
                      URIVAL("<http://example.org/ns#price>"),
                      VARIABLE("?price"),
                      List(GRAPH_VARIABLE)
                    )
                  )
                ),
                Table(
                  Seq(VARIABLE("?book"), VARIABLE("?title")),
                  Seq(
                    Row(Seq((VARIABLE("?title"), STRING("SPARQL Tutorial")))),
                    Row(
                      Seq(
                        (
                          VARIABLE("?book"),
                          URIVAL("<http://example.org/book/book2>")
                        )
                      )
                    )
                  )
                )
              )
            ) =>
          succeed
        case _ => fail
      }
    }

    "return proper type when multiple values on rows" in {

      val p = fastparse.parse(
        sparql2Algebra(
          "/queries/q46-values-multiple-values-rows.sparql"
        ),
        ExprParser.parser(_)
      )

      p.get.value match {
        case Project(
              Seq(VARIABLE("?book"), VARIABLE("?title"), VARIABLE("?price")),
              Join(
                BGP(
                  Seq(
                    Quad(
                      VARIABLE("?book"),
                      URIVAL("<http://purl.org/dc/elements/1.1/title>"),
                      VARIABLE("?title"),
                      List(GRAPH_VARIABLE)
                    ),
                    Quad(
                      VARIABLE("?book"),
                      URIVAL("<http://example.org/ns#price>"),
                      VARIABLE("?price"),
                      List(GRAPH_VARIABLE)
                    )
                  )
                ),
                Table(
                  Seq(VARIABLE("?book"), VARIABLE("?title")),
                  Seq(
                    Row(
                      Seq(
                        (VARIABLE("?title"), STRING("SPARQL Tutorial")),
                        (
                          VARIABLE("?book"),
                          URIVAL("<http://example.org/book/book1>")
                        )
                      )
                    ),
                    Row(
                      Seq(
                        (VARIABLE("?title"), STRING("The Semantic Web")),
                        (
                          VARIABLE("?book"),
                          URIVAL("<http://example.org/book/book2>")
                        )
                      )
                    )
                  )
                )
              )
            ) =>
          succeed
        case _ => fail
      }
    }
  }

  "Exists" should {

    "return proper type when simple query" in {
      val p = fastparse.parse(
        sparql2Algebra(
          "/queries/q47-exists-simple-query.sparql"
        ),
        ExprParser.parser(_)
      )

      p.get.value match {
        case Project(
              Seq(VARIABLE("?name"), VARIABLE("?age")),
              Exists(
                false,
                BGP(
                  Seq(
                    Quad(
                      VARIABLE("?s"),
                      URIVAL("<http://xmlns.com/foaf/0.1/mail>"),
                      VARIABLE("?mail"),
                      List(GRAPH_VARIABLE)
                    )
                  )
                ),
                BGP(
                  Seq(
                    Quad(
                      VARIABLE("?s"),
                      URIVAL("<http://xmlns.com/foaf/0.1/name>"),
                      VARIABLE("?name"),
                      List(GRAPH_VARIABLE)
                    ),
                    Quad(
                      VARIABLE("?s"),
                      URIVAL("<http://xmlns.com/foaf/0.1/age>"),
                      VARIABLE("?age"),
                      List(GRAPH_VARIABLE)
                    )
                  )
                )
              )
            ) =>
          succeed
        case _ => fail
      }
    }
  }

  "Not exists" should {

    "return proper type when simple query" in {
      val p = fastparse.parse(
        sparql2Algebra(
          "/queries/q48-not-exists-simple-query.sparql"
        ),
        ExprParser.parser(_)
      )

      p.get.value match {
        case Project(
              Seq(VARIABLE("?name"), VARIABLE("?age")),
              Exists(
                true,
                BGP(
                  Seq(
                    Quad(
                      VARIABLE("?s"),
                      URIVAL("<http://xmlns.com/foaf/0.1/mail>"),
                      VARIABLE("?mail"),
                      List(GRAPH_VARIABLE)
                    )
                  )
                ),
                BGP(
                  Seq(
                    Quad(
                      VARIABLE("?s"),
                      URIVAL("<http://xmlns.com/foaf/0.1/name>"),
                      VARIABLE("?name"),
                      List(GRAPH_VARIABLE)
                    ),
                    Quad(
                      VARIABLE("?s"),
                      URIVAL("<http://xmlns.com/foaf/0.1/age>"),
                      VARIABLE("?age"),
                      List(GRAPH_VARIABLE)
                    )
                  )
                )
              )
            ) =>
          succeed
        case _ => fail
      }
    }
  }

  "Reduced" should {

    "return proper type when simple query" in {
      val p = fastparse.parse(
        sparql2Algebra(
          "/queries/q49-reduced-simple-query.sparql"
        ),
        ExprParser.parser(_)
      )

      p.get.value match {
        case Reduced(
              Project(
                Seq(VARIABLE("?name")),
                BGP(
                  Seq(
                    Quad(
                      VARIABLE("?x"),
                      URIVAL("<http://xmlns.com/foaf/0.1/name>"),
                      VARIABLE("?name"),
                      List(GRAPH_VARIABLE)
                    )
                  )
                )
              )
            ) =>
          succeed
        case _ => fail
      }
    }
  }

  /*Below are where assertions are beginning to get complex. The assumption is that previous tests appropriately exercise the parser
  combinator functions. Reading expected results from file instead of explicitly defining inline.
   */
  "Complex nested string function query" should {

    "return proper nested type" in {
      val p = fastparse.parse(
        sparql2Algebra(
          "/queries/q16-string-functions-nested-complex.sparql"
        ),
        ExprParser.parser(_)
      )
      val output = readOutputFile("/queries/output/q16-output.txt")
      assert(output.trim == p.get.value.toString.trim)
    }
  }

  "Full query1" should {

    "return proper type" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/lit-search-1.sparql"),
        ExprParser.parser(_)
      )
      val output =
        readOutputFile("/queries/output/lit-search-1-output.txt")
      assert(output.trim == p.get.value.toString.trim)
    }
  }
  "Full query2" should {

    "return proper type" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/lit-search-2.sparql"),
        ExprParser.parser(_)
      )
      val output =
        readOutputFile("/queries/output/lit-search-2-output.txt")
      assert(output.trim == p.get.value.toString.trim)
    }
  }

  "Full query3" should {

    "return proper type" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/lit-search-3.sparql"),
        ExprParser.parser(_)
      )
      val output =
        readOutputFile("/queries/output/lit-search-3-output.txt")
      assert(output.trim == p.get.value.toString.trim)
    }
  }

  "Full query4" should {

    "return proper type" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/lit-search-4.sparql"),
        ExprParser.parser(_)
      )
      val output =
        readOutputFile("/queries/output/lit-search-4-output.txt")
      assert(output.trim == p.get.value.toString.trim)
    }
  }

  "Full query5" should {

    "return proper type" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/lit-search-5.sparql"),
        ExprParser.parser(_)
      )
      val output =
        readOutputFile("/queries/output/lit-search-5-output.txt")
      assert(output.trim == p.get.value.toString.trim)
    }
  }
  "Full query6" should {

    "return proper type" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/lit-search-6.sparql"),
        ExprParser.parser(_)
      )
      val output =
        readOutputFile("/queries/output/lit-search-6-output.txt")
      assert(output.trim == p.get.value.toString.trim)
    }
  }

  "Extra large query" should {

    "return proper type" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/lit-search-xlarge.sparql"),
        ExprParser.parser(_)
      )
      val output =
        readOutputFile("/queries/output/lit-search-xlarge-output.txt")
      assert(output.trim == p.get.value.toString.trim)
    }
  }

  "Document query" should {

    "return the proper type" in {
      val p = fastparse.parse(
        sparql2Algebra("/queries/q20-document.sparql"),
        ExprParser.parser(_)
      )
      val output =
        readOutputFile("/queries/output/q20-document-output.txt")
      assert(output.trim == p.get.value.toString.trim)
    }
  }

  "Property paths queries" when {

    "simple queries" should {

      "parse Alternative property path" in {

        val p = fastparse.parse(
          sparql2Algebra("/queries/q50-property-simple-alt.sparql"),
          ExprParser.parser(_)
        )

        p.get.value match {
          case Project(
                Seq(VARIABLE("?book"), VARIABLE("?displayString")),
                Path(
                  VARIABLE("?book"),
                  Alternative(
                    Uri("<http://purl.org/dc/elements/1.1/title>"),
                    Uri("<http://www.w3.org/2000/01/rdf-schema#label>")
                  ),
                  VARIABLE("?displayString"),
                  List(GRAPH_VARIABLE)
                )
              ) =>
            succeed
          case _ => fail
        }
      }

      "parse Reverse property path" in {

        val p = fastparse.parse(
          sparql2Algebra("/queries/q51-property-simple-rev.sparql"),
          ExprParser.parser(_)
        )

        p.get.value match {
          case Project(
                Seq(VARIABLE("?o"), VARIABLE("?s")),
                Path(
                  VARIABLE("?o"),
                  Reverse(Uri("<http://xmlns.org/foaf/0.1/mbox>")),
                  VARIABLE("?s"),
                  List(GRAPH_VARIABLE)
                )
              ) =>
            succeed
          case _ => fail
        }
      }

      "parse Seq property path" in {

        val p = fastparse.parse(
          sparql2Algebra("/queries/q52-property-simple-seq.sparql"),
          ExprParser.parser(_)
        )

        p.get.value match {
          case Project(
                Seq(VARIABLE("?o")),
                Path(
                  VARIABLE("?s"),
                  SeqExpression(
                    SeqExpression(
                      Uri("<http://xmlns.org/foaf/0.1/knows>"),
                      Uri("<http://xmlns.org/foaf/0.1/knows>")
                    ),
                    Uri("<http://xmlns.org/foaf/0.1/name>")
                  ),
                  VARIABLE("?o"),
                  List(GRAPH_VARIABLE)
                )
              ) =>
            succeed
          case _ => fail
        }
      }

      "parse OneOrMore property path" in {

        val p = fastparse.parse(
          sparql2Algebra("/queries/q53-property-simple-plus.sparql"),
          ExprParser.parser(_)
        )

        p.get.value match {
          case Project(
                Seq(VARIABLE("?s"), VARIABLE("?o")),
                Path(
                  VARIABLE("?s"),
                  OneOrMore(Uri("<http://xmlns.org/foaf/0.1/knows>")),
                  VARIABLE("?o"),
                  List(GRAPH_VARIABLE)
                )
              ) =>
            succeed
          case _ => fail
        }
      }

      "parse ZeroOrMore property path" in {

        val p = fastparse.parse(
          sparql2Algebra("/queries/q54-property-simple-star.sparql"),
          ExprParser.parser(_)
        )

        p.get.value match {
          case Project(
                Seq(VARIABLE("?s"), VARIABLE("?o")),
                Path(
                  VARIABLE("?s"),
                  ZeroOrMore(Uri("<http://xmlns.org/foaf/0.1/knows>")),
                  VARIABLE("?o"),
                  List(GRAPH_VARIABLE)
                )
              ) =>
            succeed
          case _ => fail
        }
      }

      "parse ZeroOrOne property path" in {

        val p = fastparse.parse(
          sparql2Algebra("/queries/q55-property-simple-question.sparql"),
          ExprParser.parser(_)
        )

        p.get.value match {
          case Project(
                Seq(VARIABLE("?s"), VARIABLE("?o")),
                Path(
                  VARIABLE("?s"),
                  ZeroOrOne(Uri("<http://xmlns.org/foaf/0.1/knows>")),
                  VARIABLE("?o"),
                  List(GRAPH_VARIABLE)
                )
              ) =>
            succeed
          case _ => fail
        }
      }

      "parse NotOneOf property path" in {

        val p = fastparse.parse(
          sparql2Algebra("/queries/q56-property-simple-negation.sparql"),
          ExprParser.parser(_)
        )

        p.get.value match {
          case Project(
                Seq(VARIABLE("?s"), VARIABLE("?o")),
                Path(
                  VARIABLE("?s"),
                  NotOneOf(List(Uri("<http://xmlns.org/foaf/0.1/name>"))),
                  VARIABLE("?o"),
                  List(GRAPH_VARIABLE)
                )
              ) =>
            succeed
          case _ => fail
        }
      }

      "parse BetweenNAndM property path" in {

        val p = fastparse.parse(
          sparql2Algebra("/queries/q57-property-simple-nm.sparql"),
          ExprParser.parser(_)
        )

        p.get.value match {
          case Project(
                Seq(VARIABLE("?s"), VARIABLE("?o")),
                Path(
                  VARIABLE("?s"),
                  BetweenNAndM(1, 2, Uri("<http://xmlns.org/foaf/0.1/knows>")),
                  VARIABLE("?o"),
                  List(GRAPH_VARIABLE)
                )
              ) =>
            succeed
          case _ => fail
        }
      }

      "parse ExactlyN property path" in {

        val p = fastparse.parse(
          sparql2Algebra("/queries/q58-property-simple-n.sparql"),
          ExprParser.parser(_)
        )

        p.get.value match {
          case Project(
                Seq(VARIABLE("?s"), VARIABLE("?o")),
                Path(
                  VARIABLE("?s"),
                  ExactlyN(1, Uri("<http://xmlns.org/foaf/0.1/knows>")),
                  VARIABLE("?o"),
                  List(GRAPH_VARIABLE)
                )
              ) =>
            succeed
          case _ => fail
        }
      }

      "parse NOrMore property path" in {

        val p = fastparse.parse(
          sparql2Algebra("/queries/q59-property-simple-ncomma.sparql"),
          ExprParser.parser(_)
        )

        p.get.value match {
          case Project(
                Seq(VARIABLE("?s"), VARIABLE("?o")),
                Path(
                  VARIABLE("?s"),
                  NOrMore(1, Uri("<http://xmlns.org/foaf/0.1/knows>")),
                  VARIABLE("?o"),
                  List(GRAPH_VARIABLE)
                )
              ) =>
            succeed
          case _ => fail
        }
      }

      "parse BetweenZeroAndN property path" in {

        val p = fastparse.parse(
          sparql2Algebra("/queries/q60-property-simple-comman.sparql"),
          ExprParser.parser(_)
        )

        p.get.value match {
          case Project(
                Seq(VARIABLE("?s"), VARIABLE("?o")),
                Path(
                  VARIABLE("?s"),
                  BetweenZeroAndN(1, Uri("<http://xmlns.org/foaf/0.1/knows>")),
                  VARIABLE("?o"),
                  List(GRAPH_VARIABLE)
                )
              ) =>
            succeed
          case _ => fail
        }
      }
    }

    "complex queries" should {

      "parse complex property path query 1" in {

        val p = fastparse.parse(
          sparql2Algebra("/queries/q61-property-complex-1.sparql"),
          ExprParser.parser(_)
        )

        p.get.value match {
          case Project(
                Seq(VARIABLE("?s"), VARIABLE("?o")),
                Path(
                  VARIABLE("?s"),
                  Alternative(
                    SeqExpression(
                      Reverse(
                        ZeroOrMore(
                          NotOneOf(List(Reverse(Uri("<http://example.org/a>"))))
                        )
                      ),
                      ZeroOrOne(Uri("<http://example.org/b>"))
                    ),
                    OneOrMore(Uri("<http://example.org/c>"))
                  ),
                  VARIABLE("?o"),
                  List(GRAPH_VARIABLE)
                )
              ) =>
            succeed
          case _ => fail
        }
      }

      "parse complex property path query 2" in {

        val p = fastparse.parse(
          sparql2Algebra("/queries/q62-property-complex-2.sparql"),
          ExprParser.parser(_)
        )

        p.get.value match {
          case Project(
                Seq(VARIABLE("?s"), VARIABLE("?o")),
                Sequence(
                  List(
                    Path(
                      VARIABLE("?s"),
                      Alternative(
                        Alternative(
                          ExactlyN(1, Uri("<http://example.org/a>")),
                          BetweenNAndM(1, 3, Uri("<http://example.org/b>"))
                        ),
                        NOrMore(2, Uri("<http://example.org/c>"))
                      ),
                      VARIABLE("?o"),
                      List(GRAPH_VARIABLE)
                    ),
                    Path(
                      VARIABLE("?s"),
                      Alternative(
                        SeqExpression(
                          Reverse(
                            ZeroOrMore(
                              NotOneOf(
                                List(Reverse(Uri("<http://example.org/a>")))
                              )
                            )
                          ),
                          ZeroOrOne(Uri("<http://example.org/b>"))
                        ),
                        OneOrMore(Uri("<http://example.org/c>"))
                      ),
                      VARIABLE("?o"),
                      List(GRAPH_VARIABLE)
                    ),
                    BGP(
                      Seq(
                        Quad(
                          VARIABLE("?s"),
                          URIVAL("<http://example.org/a>"),
                          VARIABLE("?o"),
                          List(GRAPH_VARIABLE)
                        )
                      )
                    )
                  )
                )
              ) =>
            succeed
          case _ => fail
        }
      }
    }
  }
}
