package com.gsk.kg.engine
package optimizer

import cats.implicits._
import com.gsk.kg.engine.data.ToTree._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import com.gsk.kg.sparql.syntax.all._
import higherkindness.droste.data.Fix
import higherkindness.droste.Basis
import com.gsk.kg.engine.DAG.BGP
import com.gsk.kg.engine.DAG.Project

class CompactBGPsSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  type T = Fix[DAG]

  "CompactBGPs" should "compact BGPs based on subject" in {

    val query = sparql"""
        PREFIX dm: <http://gsk-kg.rdip.gsk.com/dm/1.0/>

        SELECT ?d
        WHERE {
          ?d a dm:Document .
          ?d dm:source "potato"
        }
      """

    val dag: T = DAG.fromQuery.apply(query)
    countChunksInBGP(dag) shouldEqual 2

    val optimized = CompactBGPs[T].apply(dag)
    countChunksInBGP(optimized) shouldEqual 1
  }

  def countChunksInBGP(dag: T): Int =
    Fix.un(dag) match {
      case Project(vars, Fix(Project(vars2, Fix(BGP(triples))))) =>
        val countChunks = triples.foldLeftChunks(0)((acc, _) => acc + 1)
        countChunks
    }

}
