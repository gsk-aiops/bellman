package com.gsk.kg.site

import com.gsk.kg.site.contrib.reftree.prelude._
import com.gsk.kg.engine.optimize._
import reftree.diagram.Animation
import reftree.diagram.Diagram
import reftree.render.Renderer
import reftree.render.RenderingOptions
import reftree.contrib.SimplifiedInstances._
import java.nio.file.Paths
import cats.implicits._

import higherkindness.droste.data._

import com.gsk.kg.sparqlparser._
import com.gsk.kg.engine.data._
import com.gsk.kg.engine._
import com.gsk.kg.engine.DAG._
import com.gsk.kg.engine.optimizer._
import com.gsk.kg.Graphs
import com.gsk.kg.config.Config

/** This object generates all the diagrams and animations we have in our documentation.
  */
object Animations extends App {

  val ImagePath = "modules/site/src/main/resources/site/images"

  val renderer = Renderer(
    renderingOptions = RenderingOptions(density = 75),
    directory = Paths.get(ImagePath, "overview")
  )
  import renderer._

  def createBasicAnimation(): Unit = {
    val query = QueryConstruct
      .parse(
        """
        SELECT ?d
        WHERE {
          ?d a <http://example.com/Doc> .
          ?d <http://example.com/source> <http://example.com/mySource> .
        }
        """,
        Config.default
      )
      ._1

    val dag = DAG.fromQuery.apply(query)

    val optimizations: Map[Int, Fix[DAG] => Fix[DAG]] = Map(
      1 -> RemoveNestedProject[Fix[DAG]].apply,
      2 -> { dag => GraphsPushdown[Fix[DAG]].apply(dag, Graphs.empty) },
      3 -> CompactBGPs[Fix[DAG]].apply
    )

    (Animation
      .startWith(dag)
      .iterateWithIndex(3) { (dag, i) =>
        optimizations(i).apply(dag)
      }
      .build(Diagram(_).withCaption("DAG").withColor(2))
      .render("animation-simple"))
  }

  def createChunkedListAnimation(): Unit = {
    val list: ChunkedList[String] =
      ChunkedList.fromList(List("a", "as", "asterisk", "b", "bee", "burrito"))

    (Animation
      .startWith(list)
      .iterateWithIndex(1) { (list, i) =>
        list.compact(_.head)
      }
      .build(Diagram(_).withCaption("ChunkedList compaction").withColor(2))
      .render("chunkedlist"))
  }

  def createCompactBGPAnimation(): Unit = {
    val query = QueryConstruct
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
      ._1

    val dag = DAG.fromQuery.apply(query)

    (Animation
      .startWith(dag)
      .iterateWithIndex(1) { (dag, i) =>
        CompactBGPs[Fix[DAG]].apply(dag)
      }
      .build(Diagram(_).withCaption("DAG").withColor(2))
      .render("bgp-compaction"))
  }

  def createGraphPushdownAnimation(): Unit = {
    val query = QueryConstruct
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
      ._1

    val dag = DAG.fromQuery.apply(query)

    (Animation
      .startWith(dag)
      .iterateWithIndex(1) { (dag, i) =>
        GraphsPushdown[Fix[DAG]].apply(dag, Graphs.empty)
      }
      .build(Diagram(_).withCaption("DAG").withColor(2))
      .render("graph-pushdown"))

  }

  createBasicAnimation()
  createCompactBGPAnimation()
  createChunkedListAnimation()
  createGraphPushdownAnimation()
}
