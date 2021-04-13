package com.gsk.kg.engine
package optimizer

import cats.Traverse

import higherkindness.droste.data.Fix

import com.gsk.kg.engine.DAG.BGP
import com.gsk.kg.engine.DAG.Project
import com.gsk.kg.engine.data.ChunkedList
import com.gsk.kg.engine.optics._
import com.gsk.kg.sparqlparser.Expr.Quad
import com.gsk.kg.sparqlparser.TestConfig
import com.gsk.kg.sparqlparser.TestUtils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class CompactBGPsSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with TestUtils
    with TestConfig {

  type T = Fix[DAG]

  "CompactBGPs" should "compact BGPs based on subject" in {

    val q =
      """
        |PREFIX dm: <http://gsk-kg.rdip.gsk.com/dm/1.0/>
        |
        |SELECT ?d
        |WHERE {
        | ?d a dm:Document .
        | ?d dm:source "potato"
        |}
        |""".stripMargin
    val (query, _) = parse(q)

    val dag: T = DAG.fromQuery.apply(query)
    countChunksInBGP(dag) shouldEqual 2

    val optimized = CompactBGPs[T].apply(dag)
    countChunksInBGP(optimized) shouldEqual 1
  }

  it should "not change the order when compacting" in {

    val q =
      """
        |PREFIX dm: <http://gsk-kg.rdip.gsk.com/dm/1.0/>
        |
        |SELECT ?d
        |WHERE {
        | ?d a dm:Document .
        | ?other dm:source "qwer" .
        | ?d dm:source "potato" .
        |}
        |""".stripMargin
    val (query, _) = parse(q)

    val dag: T       = DAG.fromQuery.apply(query)
    val optimized: T = CompactBGPs[T].apply(dag)

    Traverse[ChunkedList]
      .toList(getQuads(dag)) shouldEqual Traverse[ChunkedList].toList(
      getQuads(optimized)
    )
  }

  def getQuads(dag: T): ChunkedList[Quad] =
    _projectR
      .composeLens(Project.r)
      .composePrism(_projectR)
      .composeLens(Project.r)
      .composePrism(_bgpR)
      .composeLens(BGP.quads)
      .getOption(dag)
      .getOrElse(ChunkedList.empty)

  def countChunksInBGP(dag: T): Int = getQuads(dag)
    .foldLeftChunks(0)((acc, _) => acc + 1)
}
