package com.gsk.kg.site

import cats.implicits._
import cats.syntax.either._

import higherkindness.droste.data._

import com.gsk.kg.Graphs
import com.gsk.kg.config.Config
import com.gsk.kg.engine.DAG._
import com.gsk.kg.engine._
import com.gsk.kg.engine.data._
import com.gsk.kg.engine.optimizer._
import com.gsk.kg.engine.optimizer._
import com.gsk.kg.site.contrib.reftree.prelude._
import com.gsk.kg.sparqlparser._

import java.nio.file.Paths

import scala.concurrent.duration._

import reftree.contrib.SimplifiedInstances._
import reftree.diagram.Animation
import reftree.diagram.Diagram
import reftree.render.AnimationOptions
import reftree.render.Renderer
import reftree.render.RenderingOptions

/** This object generates all the diagrams and animations we have in our documentation.
  */
object Animations extends App {

  val ImagePath = "modules/site/src/main/resources/site/images"

  val renderer = Renderer(
    renderingOptions = RenderingOptions(density = 75),
    directory = Paths.get(ImagePath, "overview")
  )
  import renderer._

  def tweakAnimation(opts: AnimationOptions): AnimationOptions =
    opts.copy(
      keyFrameDuration = 1.5.seconds,
      interpolationDuration = 0.5.seconds
    )

  def createBasicAnimation(): Result[Unit] = {
    QueryConstruct
      .parse(
        """
        SELECT ?d
        WHERE {
          ?d a <http://example.com/Doc> .
          GRAPH <http://example.com/sourcesGraph> {
            ?d <http://example.com/source> <http://example.com/mySource> .
          }
        }
        """,
        Config.default
      )
      .map { case (query, _) =>
        val dag = DAG.fromQuery.apply(query)

        val optimizations: Map[Int, Fix[DAG] => Fix[DAG]] = Map(
          1 -> RemoveNestedProject[Fix[DAG]].apply,
          2 -> { dag => GraphsPushdown[Fix[DAG]].apply(dag, Graphs.empty) },
          3 -> JoinBGPs[Fix[DAG]].apply,
          4 -> CompactBGPs[Fix[DAG]].apply
        )

        (
          Animation
            .startWith(dag)
            .iterateWithIndex(4) { (dag, i) =>
              optimizations(i).apply(dag)
            }
            .build(Diagram(_).withCaption("DAG").withColor(2))
            .render(
              "animation-simple",
              tweakAnimation = tweakAnimation
            )
        )
      }
  }

  def createChunkedListAnimation(): Result[Unit] = {
    val list: ChunkedList[String] =
      ChunkedList.fromList(List("a", "as", "asterisk", "b", "bee", "burrito"))

    Right(
      Animation
        .startWith(list)
        .iterateWithIndex(1) { (list, i) =>
          list.compact(_.head)
        }
        .build(Diagram(_).withCaption("ChunkedList compaction").withColor(2))
        .render(
          "chunkedlist",
          tweakAnimation = tweakAnimation
        )
    )
  }

  def createCompactBGPAnimation(): Result[Unit] = {
    QueryConstruct
      .parse(
        """
        SELECT ?d
        WHERE {
          ?d a <http://example.com/Doc> .
          ?d <http://example.com/source> <http://example.com/mysource> .
        }
        """,
        Config.default
      )
      .map { case (query, _) =>
        val dag = DAG.fromQuery.apply(query)

        (
          Animation
            .startWith(dag)
            .iterateWithIndex(1) { (dag, i) =>
              CompactBGPs[Fix[DAG]].apply(dag)
            }
            .build(Diagram(_).withCaption("DAG").withColor(2))
            .render(
              "bgp-compaction",
              tweakAnimation = tweakAnimation
            )
        )
      }
  }

  def createJoinBGPsAnimation(): Result[Unit] = {
    QueryConstruct
      .parse(
        """
        CONSTRUCT {
          ?d <http://example.com/hasSource> <http://example.com/mysource>
        } WHERE {
          { ?d a <http://example.com/Doc> }
          GRAPH <http://example.com/named-graph>
          { ?d <http://example.com/source> <http://example.com/mysource> }
        }
        """,
        Config.default
      )
      .map { case (query, _) =>
        val optimizations: Map[Int, Fix[DAG] => Fix[DAG]] = Map(
          1 -> { dag => GraphsPushdown[Fix[DAG]].apply(dag, Graphs.empty) },
          2 -> JoinBGPs[Fix[DAG]].apply
        )

        val dag = DAG.fromQuery.apply(query)

        (
          Animation
            .startWith(dag)
            .iterateWithIndex(2) { (dag, i) =>
              optimizations(i).apply(dag)
            }
            .build(Diagram(_).withCaption("DAG").withColor(2))
            .render(
              "join-bgps",
              tweakAnimation = tweakAnimation
            )
        )
      }
  }

  def createGraphPushdownAnimation(): Result[Unit] = {
    QueryConstruct
      .parse(
        """
        SELECT *
        FROM NAMED <http://example.com/named-graph>
        {
          GRAPH <http://example.com/named-graph> {
            ?s <http://example.com/predicate> ?o
          }
        }""",
        Config.default
      )
      .map { case (query, _) =>
        val dag = DAG.fromQuery.apply(query)

        (
          Animation
            .startWith(dag)
            .iterateWithIndex(1) { (dag, i) =>
              GraphsPushdown[Fix[DAG]].apply(dag, Graphs.empty)
            }
            .build(Diagram(_).withCaption("DAG").withColor(2))
            .render(
              "graph-pushdown",
              tweakAnimation = tweakAnimation
            )
        )
      }
  }

  def createSubqueryPushdownAnimation(): Result[Unit] = {
    QueryConstruct
      .parse(
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
          |""".stripMargin,
        Config.default
      )
      .map { case (query, _) =>
        val dag = DAG.fromQuery.apply(query)

        (
          Animation
            .startWith(dag)
            .iterateWithIndex(1) { (dag, i) =>
              SubqueryPushdown[Fix[DAG]].apply(dag)
            }
            .build(Diagram(_).withCaption("DAG").withColor(2))
            .render(
              "subquery-pushdown",
              tweakAnimation = tweakAnimation
            )
        )
      }
  }

  (for {
    _ <- createBasicAnimation()
    _ <- createJoinBGPsAnimation()
    _ <- createCompactBGPAnimation()
    _ <- createChunkedListAnimation()
    _ <- createGraphPushdownAnimation()
    _ <- createSubqueryPushdownAnimation()
  } yield ()).right.get
}
