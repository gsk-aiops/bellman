package com.gsk.kg.engine
package optimizer

import cats.implicits._

import higherkindness.droste.Basis

import com.gsk.kg.engine.DAG.BGP
import com.gsk.kg.engine.DAG.Project
import com.gsk.kg.engine.data.ChunkedList
import com.gsk.kg.engine.optics._
import com.gsk.kg.engine.scalacheck.ChunkedListArbitraries
import com.gsk.kg.engine.scalacheck.DAGArbitraries
import com.gsk.kg.engine.scalacheck.DrosteImplicits
import com.gsk.kg.sparql.syntax.all._
import com.gsk.kg.sparqlparser.StringVal
import com.gsk.kg.sparqlparser.Expr

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class ReorderBgpsSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with DrosteImplicits
    with DAGArbitraries
    with ChunkedListArbitraries {

  "ReorderBgps" should "keep queries with a single triple as they're" in {

    val query = sparql"""
      prefix : <http://example.org/>

      select * where {
        ?qwer :knows ?asdf .
      }
      """

    val newDag = ReorderBgps.reorderBgps(DAG.fromQuery.apply(query))

    getQuads(newDag).toList shouldEqual List(
      Expr.Quad(
        StringVal.VARIABLE("?qwer"),
        StringVal.URIVAL("<http://example.org/knows>"),
        StringVal.VARIABLE("?asdf"),
        List(StringVal.GRAPH_VARIABLE)
      )
    )
  }

  it should "keep queries with two triples as they're" in {

    val query = sparql"""
      prefix : <http://example.org/>

      select * where {
        ?qwer :knows ?asdf .
        ?a :knows ?b .
      }
      """

    val newDag = ReorderBgps.reorderBgps(DAG.fromQuery.apply(query))

    getQuads(newDag).toList shouldEqual List(
      Expr.Quad(
        StringVal.VARIABLE("?qwer"),
        StringVal.URIVAL("<http://example.org/knows>"),
        StringVal.VARIABLE("?asdf"),
        List(StringVal.GRAPH_VARIABLE)
      ),
      Expr.Quad(
        StringVal.VARIABLE("?a"),
        StringVal.URIVAL("<http://example.org/knows>"),
        StringVal.VARIABLE("?b"),
        List(StringVal.GRAPH_VARIABLE)
      )
    )

  }

  it should "reorder complex queries" in {

    val query = sparql"""
      prefix : <http://example.org/>

      select * where {
        ?qwer :knows ?asdf .

        ?a :knows :alice .
        ?b :knows :alice .
        ?a :knows ?b .

        ?qwer :knows ?zxcv .
        ?asdf :knows ?zxcv .

        ?b :knows :other .
        ?a :knows ?c .

        ?a :knows ?zxcv
      }
      """

    val newDag = ReorderBgps.reorderBgps(DAG.fromQuery.apply(query))

    getQuads(newDag).toList shouldEqual List(
      Expr.Quad(
        StringVal.VARIABLE("?qwer"),
        StringVal.URIVAL("<http://example.org/knows>"),
        StringVal.VARIABLE("?asdf"),
        List(StringVal.GRAPH_VARIABLE)
      ),
      Expr.Quad(
        StringVal.VARIABLE("?qwer"),
        StringVal.URIVAL("<http://example.org/knows>"),
        StringVal.VARIABLE("?zxcv"),
        List(StringVal.GRAPH_VARIABLE)
      ),
      Expr.Quad(
        StringVal.VARIABLE("?asdf"),
        StringVal.URIVAL("<http://example.org/knows>"),
        StringVal.VARIABLE("?zxcv"),
        List(StringVal.GRAPH_VARIABLE)
      ),
      Expr.Quad(
        StringVal.VARIABLE("?a"),
        StringVal.URIVAL("<http://example.org/knows>"),
        StringVal.VARIABLE("?zxcv"),
        List(StringVal.GRAPH_VARIABLE)
      ),
      Expr.Quad(
        StringVal.VARIABLE("?a"),
        StringVal.URIVAL("<http://example.org/knows>"),
        StringVal.VARIABLE("?b"),
        List(StringVal.GRAPH_VARIABLE)
      ),
      Expr.Quad(
        StringVal.VARIABLE("?a"),
        StringVal.URIVAL("<http://example.org/knows>"),
        StringVal.VARIABLE("?c"),
        List(StringVal.GRAPH_VARIABLE)
      ),
      Expr.Quad(
        StringVal.VARIABLE("?a"),
        StringVal.URIVAL("<http://example.org/knows>"),
        StringVal.URIVAL("<http://example.org/alice>"),
        List(StringVal.GRAPH_VARIABLE)
      ),
      Expr.Quad(
        StringVal.VARIABLE("?b"),
        StringVal.URIVAL("<http://example.org/knows>"),
        StringVal.URIVAL("<http://example.org/alice>"),
        List(StringVal.GRAPH_VARIABLE)
      ),
      Expr.Quad(
        StringVal.VARIABLE("?b"),
        StringVal.URIVAL("<http://example.org/knows>"),
        StringVal.URIVAL("<http://example.org/other>"),
        List(StringVal.GRAPH_VARIABLE)
      )
    )

  }

  def getQuads[T: Basis[DAG, *]](dag: T): ChunkedList[Expr.Quad] =
    _projectR
      .composeLens(Project.r)
      .composePrism(_bgpR)
      .composeLens(BGP.quads)
      .getOption(dag)
      .getOrElse(ChunkedList.empty)
}
