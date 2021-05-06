package com.gsk.kg.engine.optimizer

import cats.syntax.either._

import higherkindness.droste.data.Fix

import com.gsk.kg.engine.DAG
import com.gsk.kg.engine.DAG.Construct
import com.gsk.kg.engine.DAG.Join
import com.gsk.kg.engine.DAG.Project
import com.gsk.kg.engine.DAG.Scan
import com.gsk.kg.engine.utils.CustomMatchers
import com.gsk.kg.sparqlparser.StringVal.GRAPH_VARIABLE
import com.gsk.kg.sparqlparser.StringVal.VARIABLE
import com.gsk.kg.sparqlparser.TestConfig
import com.gsk.kg.sparqlparser.TestUtils

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SubqueryPushdownSpec
    extends AnyWordSpec
    with Matchers
    with CustomMatchers
    with TestUtils
    with TestConfig {

  type T = Fix[DAG]

  "SubqueryPushdown" should {

    "push-down graph variable to inner subqueries" when {

      "outer SELECT and inner SELECT as graph pattern" in {

        val q =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?y ?name
            |WHERE {
            |  ?y foaf:knows ?x .
            |  {
            |    SELECT ?x ?name
            |    WHERE {
            |      ?x foaf:name ?name .
            |    }
            |  }
            |}
            |""".stripMargin

        parse(q, config)
          .map { case (query, _) =>
            val dag: T = DAG.fromQuery.apply(query)
            Fix.un(dag) match {
              case Project(
                    outerVars,
                    Project(innerVars1, Join(_, Project(innerVars2, _)))
                  ) =>
                (outerVars ++ innerVars1 ++ innerVars2) should not contain VARIABLE(
                  GRAPH_VARIABLE.s
                )
              case _ => fail
            }

            val pushed = SubqueryPushdown[T].apply(dag)
            Fix.un(pushed) match {
              case Project(
                    outerVars,
                    Project(innerVars1, Join(_, Project(innerVars2, _)))
                  ) =>
                outerVars should not contain VARIABLE(GRAPH_VARIABLE.s)
                innerVars1 should containExactlyOnce(VARIABLE(GRAPH_VARIABLE.s))
                innerVars2 should containExactlyOnce(VARIABLE(GRAPH_VARIABLE.s))
              case _ => fail
            }
          }
          .getOrElse(fail)
      }

      // TODO: Un-ignore when implemented ASK
      "outer SELECT and inner ASK as graph pattern" ignore {}

      "outer CONSTRUCT and inner SELECT as graph pattern" in {

        val q =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |
            |CONSTRUCT {
            |  ?y foaf:knows ?name .
            |} WHERE {
            |  ?y foaf:knows ?x .
            |  {
            |    SELECT ?x ?name
            |    WHERE {
            |      ?x foaf:name ?name .
            |    }
            |  }
            |}
            |""".stripMargin

        parse(q, config)
          .map { case (query, _) =>
            val dag: T = DAG.fromQuery.apply(query)
            Fix.un(dag) match {
              case Construct(_, Join(_, Project(innerVars, _))) =>
                innerVars should not contain VARIABLE(GRAPH_VARIABLE.s)
              case _ => fail
            }

            val pushed = SubqueryPushdown[T].apply(dag)
            Fix.un(pushed) match {
              case Construct(_, Join(_, Project(innerVars, _))) =>
                innerVars should containExactlyOnce(VARIABLE(GRAPH_VARIABLE.s))
              case _ => fail
            }
          }
          .getOrElse(fail)
      }

      // TODO: Un-ignore when implemented ASK
      "outer CONSTRUCT and inner ASK as graph pattern" ignore {}

      // TODO: Un-ignore when implemented ASK
      "outer ASK and inner SELECT as graph pattern" ignore {}

      "multiple inner sub-queries" in {

        val q =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?x ?y ?name ?group
            |WHERE {
            |  ?y foaf:member ?group .
            |  {
            |    SELECT ?y ?x ?name
            |    WHERE {
            |      ?y foaf:knows ?x .
            |      {
            |         SELECT ?x ?name
            |         WHERE {
            |           ?x foaf:name ?name .
            |         }
            |      }
            |    }
            |  }
            |}
            |""".stripMargin

        parse(q, config)
          .map { case (query, _) =>
            val dag: T = DAG.fromQuery.apply(query)
            Fix.un(dag) match {
              case Project(
                    outerVars,
                    Project(
                      innerVars1,
                      Join(
                        _,
                        Project(innerVars2, Join(_, Project(innerVars3, _)))
                      )
                    )
                  ) =>
                (outerVars ++ innerVars1 ++ innerVars2 ++ innerVars3) should not contain VARIABLE(
                  GRAPH_VARIABLE.s
                )
              case _ => fail
            }

            val pushed = SubqueryPushdown[T].apply(dag)
            Fix.un(pushed) match {
              case Project(
                    outerVars,
                    Project(
                      innerVars1,
                      Join(
                        _,
                        Project(innerVars2, Join(_, Project(innerVars3, _)))
                      )
                    )
                  ) =>
                outerVars should not contain VARIABLE(GRAPH_VARIABLE.s)
                innerVars1 should containExactlyOnce(VARIABLE(GRAPH_VARIABLE.s))
                innerVars2 should containExactlyOnce(VARIABLE(GRAPH_VARIABLE.s))
                innerVars3 should containExactlyOnce(VARIABLE(GRAPH_VARIABLE.s))
              case _ => fail
            }
          }
          .getOrElse(fail)
      }

      "mixing graphs" in {

        val q =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?x ?name
            |FROM NAMED <http://some-other.ttl>
            |WHERE {
            |  GRAPH <http://some-other.ttl> {
            |    {
            |      SELECT ?x ?name
            |      WHERE {
            |        ?x foaf:name ?name .
            |      }
            |    }
            |  }
            |}
            |""".stripMargin

        parse(q, config)
          .map { case (query, _) =>
            val dag: T = DAG.fromQuery.apply(query)
            Fix.un(dag) match {
              case Project(
                    outerVars,
                    Project(innerVars1, Scan(_, Project(innerVars2, _)))
                  ) =>
                (outerVars ++ innerVars1 ++ innerVars2) should not contain VARIABLE(
                  GRAPH_VARIABLE.s
                )
              case _ => fail
            }

            val pushed = SubqueryPushdown[T].apply(dag)
            Fix.un(pushed) match {
              case Project(
                    outerVars,
                    Project(innerVars1, Scan(_, Project(innerVars2, _)))
                  ) =>
                outerVars should not contain VARIABLE(GRAPH_VARIABLE.s)
                innerVars1 should containExactlyOnce(VARIABLE(GRAPH_VARIABLE.s))
                innerVars2 should containExactlyOnce(VARIABLE(GRAPH_VARIABLE.s))
              case _ => fail
            }
          }
          .getOrElse(fail)
      }
    }
  }
}
