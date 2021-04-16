package com.gsk.kg.engine.optimize

import higherkindness.droste.data.Fix

import com.gsk.kg.engine.DAG
import com.gsk.kg.engine.DAG._
import com.gsk.kg.engine.data.ChunkedList
import com.gsk.kg.sparqlparser.Expr
import com.gsk.kg.sparqlparser.StringVal.GRAPH_VARIABLE
import com.gsk.kg.sparqlparser.StringVal.URIVAL
import com.gsk.kg.sparqlparser.TestConfig
import com.gsk.kg.sparqlparser.TestUtils

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class GraphsPushdownSpec
    extends AnyWordSpec
    with Matchers
    with TestUtils
    with TestConfig {

  type T = Fix[DAG]

  val assertForAllQuads: ChunkedList[Expr.Quad] => (
      Expr.Quad => Assertion
  ) => Unit = {
    chunkedList: ChunkedList[Expr.Quad] => assert: (Expr.Quad => Assertion) =>
      chunkedList.mapChunks(_.map(assert))
  }

  "GraphsPushdown" should {

    "set the graph column of quads when inside a GRAPH statement" when {

      "has BGP immediately after Scan" in {

        val q =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX dc: <http://purl.org/dc/elements/1.1/>
            |PREFIX ex: <http://example.org/>
            |    
            |SELECT ?mbox ?name
            |FROM <http://example.org/dft.ttl>
            |FROM NAMED <http://example.org/alice>
            |FROM NAMED <http://example.org/bob>
            |WHERE
            |{
            | GRAPH ex:alice {
            |   ?x foaf:mbox ?mbox .
            |   ?x foaf:name ?name .
            | }
            |}
            |""".stripMargin
        val (query, graphs) = parse(q, config)

        val dag: T = DAG.fromQuery.apply(query)
        Fix.un(dag) match {
          case Project(_, Project(_, Scan(_, BGP(quads)))) =>
            assertForAllQuads(quads)(_.g shouldEqual GRAPH_VARIABLE :: Nil)
          case _ => fail
        }

        val renamed = GraphsPushdown[T].apply(dag, graphs)
        Fix.un(renamed) match {
          case Project(_, Project(_, BGP(quads))) =>
            assertForAllQuads(quads)(
              _.g shouldEqual URIVAL("http://example.org/alice") :: Nil
            )
          case _ => fail
        }
      }

      "has BGP before and immediately after Scan" in {

        val q =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX dc: <http://purl.org/dc/elements/1.1/>
            |PREFIX ex: <http://example.org/>
            |    
            |SELECT ?mbox ?name
            |FROM <http://example.org/dft.ttl>
            |FROM NAMED <http://example.org/alice>
            |FROM NAMED <http://example.org/bob>
            |WHERE
            |{
            | ?x foaf:name ?name .
            | GRAPH ex:alice {
            |   ?x foaf:mbox ?mbox
            | }
            |}
            |""".stripMargin
        val (query, graphs) = parse(q, config)

        val dag: T = DAG.fromQuery.apply(query)
        Fix.un(dag) match {
          case Project(
                _,
                Project(
                  _,
                  Join(BGP(externalQuads), Scan(_, BGP(internalQuads)))
                )
              ) =>
            assertForAllQuads(externalQuads)(
              _.g shouldEqual GRAPH_VARIABLE :: Nil
            )
            assertForAllQuads(internalQuads.concat(externalQuads))(
              _.g shouldEqual GRAPH_VARIABLE :: Nil
            )
          case _ => fail
        }

        val renamed = GraphsPushdown[T].apply(dag, graphs)
        Fix.un(renamed) match {
          case Project(
                _,
                Project(
                  _,
                  Join(BGP(externalQuads), BGP(internalQuads))
                )
              ) =>
            assertForAllQuads(internalQuads)(
              _.g shouldEqual URIVAL("http://example.org/alice") :: Nil
            )
            assertForAllQuads(externalQuads)(
              _.g shouldEqual graphs.default
            )
          case _ => fail
        }
      }

      "has BGP before and Union after Scan" in {

        val q =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX dc: <http://purl.org/dc/elements/1.1/>
            |PREFIX ex: <http://example.org/>
            |    
            |SELECT ?mbox ?name
            |FROM <http://example.org/dft.ttl>
            |FROM NAMED <http://example.org/alice>
            |FROM NAMED <http://example.org/bob>
            |WHERE
            |{
            | ?x foaf:name ?name .
            | GRAPH ex:alice {
            |   { ?x foaf:mbox ?mbox }
            |   UNION
            |   { ?x foaf:name ?name }
            | }
            |}
            |""".stripMargin
        val (query, graphs) = parse(q, config)

        val dag: T = DAG.fromQuery.apply(query)
        Fix.un(dag) match {
          case Project(
                _,
                Project(
                  _,
                  Join(
                    BGP(externalQuads),
                    Scan(
                      graph,
                      Union(BGP(leftInsideQuads), BGP(rightInsideQuads))
                    )
                  )
                )
              ) =>
            assertForAllQuads(
              externalQuads.concat(leftInsideQuads).concat(rightInsideQuads)
            )(_.g shouldEqual GRAPH_VARIABLE :: Nil)
          case _ => fail
        }

        val renamed = GraphsPushdown[T].apply(dag, graphs)
        Fix.un(renamed) match {
          case Project(
                _,
                Project(
                  _,
                  Join(
                    BGP(externalQuads),
                    Union(BGP(leftInsideQuads), BGP(rightInsideQuads))
                  )
                )
              ) =>
            assertForAllQuads(externalQuads)(
              _.g shouldEqual graphs.default
            )
            assertForAllQuads(leftInsideQuads.concat(rightInsideQuads))(
              _.g shouldEqual URIVAL("http://example.org/alice") :: Nil
            )
          case _ => fail
        }
      }

      "has BGP before and LeftJoin after Scan" in {

        val q =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX dc: <http://purl.org/dc/elements/1.1/>
            |PREFIX ex: <http://example.org/>
            |    
            |SELECT ?mbox ?name
            |FROM <http://example.org/dft.ttl>
            |FROM NAMED <http://example.org/alice>
            |FROM NAMED <http://example.org/bob>
            |WHERE
            |{
            | ?x foaf:name ?name .
            | GRAPH ex:alice {
            |   ?x foaf:mbox ?mbox
            |   OPTIONAL { ?x foaf:name ?name }
            | }
            |}
            |""".stripMargin
        val (query, graphs) = parse(q, config)

        val dag: T = DAG.fromQuery.apply(query)
        Fix.un(dag) match {
          case Project(
                _,
                Project(
                  _,
                  Join(
                    BGP(externalQuads),
                    Scan(
                      graph,
                      LeftJoin(BGP(leftInsideQuads), BGP(rightInsideQuads), _)
                    )
                  )
                )
              ) =>
            assertForAllQuads(
              externalQuads.concat(leftInsideQuads).concat(rightInsideQuads)
            )(_.g shouldEqual GRAPH_VARIABLE :: Nil)
          case _ => fail
        }

        val renamed = GraphsPushdown[T].apply(dag, graphs)
        Fix.un(renamed) match {
          case Project(
                _,
                Project(
                  _,
                  Join(
                    BGP(externalQuads),
                    LeftJoin(BGP(leftInsideQuads), BGP(rightInsideQuads), _)
                  )
                )
              ) =>
            assertForAllQuads(externalQuads)(
              _.g shouldEqual graphs.default
            )
            assertForAllQuads(leftInsideQuads.concat(rightInsideQuads))(
              _.g shouldEqual URIVAL("http://example.org/alice") :: Nil
            )
          case _ => fail
        }
      }

      "has BGP before and a Join with Scan after first Scan" in {

        val q =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX dc: <http://purl.org/dc/elements/1.1/>
            |PREFIX ex: <http://example.org/>
            |    
            |SELECT ?mbox ?name
            |FROM <http://example.org/dft.ttl>
            |FROM NAMED <http://example.org/alice>
            |FROM NAMED <http://example.org/bob>
            |WHERE
            |{
            | ?x foaf:name ?name .
            | GRAPH ex:alice {
            |   ?x foaf:mbox ?mbox .
            |   GRAPH ex:bob {
            |     ?x foaf:name ?name
            |   }
            | }
            |}
            |""".stripMargin
        val (query, graphs) = parse(q, config)

        val dag: T = DAG.fromQuery.apply(query)
        Fix.un(dag) match {
          case Project(
                _,
                Project(
                  _,
                  Join(
                    BGP(externalQuads),
                    Scan(
                      graph1,
                      Join(BGP(graph1Quads), Scan(graph2, BGP(graph2Quads)))
                    )
                  )
                )
              ) =>
            assertForAllQuads(
              externalQuads.concat(graph1Quads).concat(graph2Quads)
            )(_.g shouldEqual GRAPH_VARIABLE :: Nil)
          case _ => fail
        }

        val renamed = GraphsPushdown[T].apply(dag, graphs)
        Fix.un(renamed) match {
          case Project(
                _,
                Project(
                  _,
                  Join(
                    BGP(externalQuads),
                    Join(BGP(graph1Quads), BGP(graph2Quads))
                  )
                )
              ) =>
            assertForAllQuads(externalQuads)(
              _.g shouldEqual graphs.default
            )
            assertForAllQuads(graph1Quads)(
              _.g shouldEqual URIVAL("http://example.org/alice") :: Nil
            )
            assertForAllQuads(graph2Quads)(
              _.g shouldEqual URIVAL("http://example.org/bob") :: Nil
            )
          case _ => fail
        }
      }
    }

    "set the graph column of quads with the default graph" when {

      "we have multiple default graphs" in {

        val q =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX dc: <http://purl.org/dc/elements/1.1/>
            |PREFIX ex: <http://example.org/>
            |    
            |SELECT ?mbox ?name
            |FROM <http://example.org/alice>
            |FROM <http://example.org/bob>
            |WHERE
            |{
            | ?x foaf:mbox ?mbox .
            | ?x foaf:name ?name .
            |}
            |""".stripMargin
        val (query, graphs) = parse(q, config)

        val dag: T = DAG.fromQuery.apply(query)
        Fix.un(dag) match {
          case Project(_, Project(_, BGP(quads))) =>
            assertForAllQuads(quads)(_.g shouldEqual GRAPH_VARIABLE :: Nil)
          case _ => fail
        }

        val pushedDown = GraphsPushdown[T].apply(dag, graphs)
        Fix.un(pushedDown) match {
          case Project(_, Project(_, BGP(quads))) =>
            assertForAllQuads(quads)(_.g shouldEqual graphs.default)
          case _ => fail
        }
      }
    }

    "set the graph column of quads when variable in a GRAPH statement" when {

      "default and named graphs" in {

        val q =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX dc: <http://purl.org/dc/elements/1.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?who ?g ?mbox
            |FROM <http://example.org/dft.ttl>
            |FROM NAMED <http://example.org/alice>
            |FROM NAMED <http://example.org/bob>
            |FROM NAMED <http://example.org/charles>
            |WHERE
            |{
            |   ?g dc:publisher ?who .
            |   GRAPH ?g { ?x foaf:mbox ?mbox }
            |}
            |""".stripMargin
        val (query, graphs) = parse(q, config)

        val dag: T = DAG.fromQuery.apply(query)
        Fix.un(dag) match {
          case Project(
                _,
                Project(
                  _,
                  Join(BGP(externalQuads), Scan(variable, BGP(internalQuads)))
                )
              ) =>
            assertForAllQuads(externalQuads.concat(internalQuads))(
              _.g shouldEqual GRAPH_VARIABLE :: Nil
            )
          case _ => fail
        }

        val pushedDown = GraphsPushdown[T].apply(dag, graphs)
        Fix.un(pushedDown) match {
          case Project(
                _,
                Project(
                  _,
                  Join(BGP(externalQuads), Scan(variable, BGP(internalQuads)))
                )
              ) =>
            assertForAllQuads(externalQuads)(_.g shouldEqual graphs.default)
            assertForAllQuads(internalQuads)(_.g shouldEqual graphs.named)
          case _ => fail
        }
      }
    }
  }
}
