package com.gsk.kg.site

import cats.data.State
import com.gsk.kg.site.contrib.reftree.prelude._
import com.gsk.kg.engine.optimize._
import reftree.core._
import reftree.diagram.Animation
import reftree.diagram.Diagram
import reftree.render.Renderer
import reftree.render.RenderingOptions
import reftree.contrib.SimplifiedInstances._
import java.nio.file.Paths
import com.gsk.kg.engine.data.ChunkedList._
import cats._
import cats.implicits._

import higherkindness.droste._
import higherkindness.droste.data._
import higherkindness.droste.data.prelude._
import higherkindness.droste.syntax.all._

import com.gsk.kg.sparql.syntax.all._
import com.gsk.kg.sparqlparser._
import com.gsk.kg.engine.data._
import com.gsk.kg.engine.data.ToTree._
import com.gsk.kg.engine._
import com.gsk.kg.engine.DAG._
import com.gsk.kg.engine.optimizer._

object Animations extends App {

  val ImagePath = "/Users/pepe/projects/github.com/gsk-aiops/bellman"

  val renderer = Renderer(
    renderingOptions = RenderingOptions(density = 75),
    directory = Paths.get(ImagePath, "overview")
  )
  import renderer._

  val query = QueryConstruct
    .parse(
      """
  SELECT ?d
  WHERE {
    ?d a <http://example.com/Doc> .
    ?d <http://example.com/source> <http://source.com/source> .
  }
  """,
      false
    )
    ._1

  val dag = DAG.fromQuery.apply(query)

  Diagram.sourceCodeCaption[Fix[DAG]](dag).render("dag")

  val optimizations: Map[Int, Fix[DAG] => Fix[DAG]] = Map(
    1 -> RemoveNestedProject[Fix[DAG]].apply,
    2 -> { dag => GraphsPushdown[Fix[DAG]].apply(dag, List.empty) },
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
