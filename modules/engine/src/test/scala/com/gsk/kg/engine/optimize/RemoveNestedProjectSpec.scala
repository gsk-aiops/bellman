package com.gsk.kg.engine
package optimizer

import higherkindness.droste.data.Fix

import com.gsk.kg.engine.DAG.Project
import com.gsk.kg.sparqlparser.TestUtils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class RemoveNestedProjectSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with TestUtils {

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

    Fix.un(dag) match {
      case Project(v1, Fix(Project(v2, r))) =>
        v1 shouldEqual v2
      case _ => fail()
    }

    val optimized = RemoveNestedProject[T].apply(dag)
    Fix.un(optimized) match {
      case Project(v1, Fix(Project(v2, r))) =>
        fail("RemoveNestedProject should have deduplicated Project nodes")
      case _ => succeed
    }

  }

}
