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
import com.gsk.kg.engine.DAG.Describe
import com.gsk.kg.engine.DAG.Ask
import com.gsk.kg.engine.DAG.Construct
import com.gsk.kg.engine.DAG.Scan
import com.gsk.kg.engine.DAG.Bind
import com.gsk.kg.engine.DAG.LeftJoin
import com.gsk.kg.engine.DAG.Union
import com.gsk.kg.engine.DAG.Filter
import com.gsk.kg.engine.DAG.Join
import com.gsk.kg.engine.DAG.Offset
import com.gsk.kg.engine.DAG.Limit
import com.gsk.kg.engine.DAG.Distinct
import com.gsk.kg.engine.DAG.Noop

class RemoveNestedProjectSpec
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
