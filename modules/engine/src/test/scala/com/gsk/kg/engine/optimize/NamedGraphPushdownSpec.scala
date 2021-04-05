package com.gsk.kg.engine
package optimizer

import higherkindness.droste.data.Fix

import com.gsk.kg.engine.DAG
import com.gsk.kg.engine.DAG._
import com.gsk.kg.engine.data.ChunkedList
import com.gsk.kg.sparql.syntax.all.SparqlQueryInterpolator
import com.gsk.kg.sparqlparser.Expr
import com.gsk.kg.sparqlparser.StringVal.GRAPH_VARIABLE
import com.gsk.kg.sparqlparser.StringVal.URIVAL

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class NamedGraphPushdownSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  type T = Fix[DAG]

  val assertForAllQuads: ChunkedList[Expr.Quad] => (
      Expr.Quad => Assertion
  ) => Unit = {
    chunkedList: ChunkedList[Expr.Quad] => assert: (Expr.Quad => Assertion) =>
      chunkedList.mapChunks(_.map(assert))
  }

  "NamedGraphPushdown" should {

    "rename the graph column of quads when inside a GRAPH statement" when {

      "has BGP immediately after Scan" in {

        val (query, _) =
          sparql"""
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX dc: <http://purl.org/dc/elements/1.1/>
        PREFIX ex: <http://example.org/>

        SELECT ?mbox ?name
        FROM <http://example.org/dft.ttl>
        FROM NAMED <http://example.org/alice>
        FROM NAMED <http://example.org/bob>
        WHERE
        {
           GRAPH ex:alice {
             ?x foaf:mbox ?mbox .
             ?x foaf:name ?name .
           }
        }
      """

        val dag: T = DAG.fromQuery.apply(query)
        Fix.un(dag) match {
          case Project(_, Project(_, Scan(_, BGP(quads)))) =>
            assertForAllQuads(quads)(_.g shouldEqual GRAPH_VARIABLE :: Nil)
          case _ => fail
        }

        val renamed = NamedGraphPushdown[T].apply(dag)
        Fix.un(renamed) match {
          case Project(_, Project(_, BGP(quads))) =>
            assertForAllQuads(quads)(
              _.g shouldEqual URIVAL("http://example.org/alice") :: Nil
            )
          case _ => fail
        }
      }

      "has BGP before and immediately after Scan" in {

        val (query, _) =
          sparql"""
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX dc: <http://purl.org/dc/elements/1.1/>
        PREFIX ex: <http://example.org/>

        SELECT ?mbox ?name
        FROM <http://example.org/dft.ttl>
        FROM NAMED <http://example.org/alice>
        FROM NAMED <http://example.org/bob>
        WHERE
        {
           ?x foaf:name ?name .
           GRAPH ex:alice {
             ?x foaf:mbox ?mbox
           }
        }
      """

        val dag: T = DAG.fromQuery.apply(query)
        Fix.un(dag) match {
          case Project(
                _,
                Project(
                  _,
                  Join(BGP(externalQuads), Scan(_, BGP(internalQuads)))
                )
              ) =>
            assertForAllQuads(internalQuads.concat(externalQuads))(
              _.g shouldEqual GRAPH_VARIABLE :: Nil
            )
          case _ => fail
        }

        val renamed = NamedGraphPushdown[T].apply(dag)
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
              _.g shouldEqual GRAPH_VARIABLE :: Nil
            )
          case _ => fail
        }
      }

      "has BGP before and Union after Scan" in {

        val (query, _) =
          sparql"""
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX dc: <http://purl.org/dc/elements/1.1/>
        PREFIX ex: <http://example.org/>

        SELECT ?mbox ?name
        FROM <http://example.org/dft.ttl>
        FROM NAMED <http://example.org/alice>
        FROM NAMED <http://example.org/bob>
        WHERE
        {
           ?x foaf:name ?name .
           GRAPH ex:alice {
             { ?x foaf:mbox ?mbox }
             UNION
             { ?x foaf:name ?name }
           }
        }
      """

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

        val renamed = NamedGraphPushdown[T].apply(dag)
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
              _.g shouldEqual GRAPH_VARIABLE :: Nil
            )
            assertForAllQuads(leftInsideQuads.concat(rightInsideQuads))(
              _.g shouldEqual URIVAL("http://example.org/alice") :: Nil
            )
          case _ => fail
        }
      }

      "has BGP before and LeftJoin after Scan" in {

        val (query, _) =
          sparql"""
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX dc: <http://purl.org/dc/elements/1.1/>
        PREFIX ex: <http://example.org/>

        SELECT ?mbox ?name
        FROM <http://example.org/dft.ttl>
        FROM NAMED <http://example.org/alice>
        FROM NAMED <http://example.org/bob>
        WHERE
        {
           ?x foaf:name ?name .
           GRAPH ex:alice {
             ?x foaf:mbox ?mbox
             OPTIONAL { ?x foaf:name ?name }
           }
        }
      """

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

        val renamed = NamedGraphPushdown[T].apply(dag)
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
              _.g shouldEqual GRAPH_VARIABLE :: Nil
            )
            assertForAllQuads(leftInsideQuads.concat(rightInsideQuads))(
              _.g shouldEqual URIVAL("http://example.org/alice") :: Nil
            )
          case _ => fail
        }
      }

      "has BGP before and a Join with Scan after first Scan" in {

        val (query, _) =
          sparql"""
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX dc: <http://purl.org/dc/elements/1.1/>
        PREFIX ex: <http://example.org/>

        SELECT ?mbox ?name
        FROM <http://example.org/dft.ttl>
        FROM NAMED <http://example.org/alice>
        FROM NAMED <http://example.org/bob>
        WHERE
        {
           ?x foaf:name ?name .
           GRAPH ex:alice {
             ?x foaf:mbox ?mbox .
             GRAPH ex:bob {
               ?x foaf:name ?name
             }
           }
        }
      """

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

        val renamed = NamedGraphPushdown[T].apply(dag)
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
              _.g shouldEqual GRAPH_VARIABLE :: Nil
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
  }
}
