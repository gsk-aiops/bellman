package graph

import cats.data.NonEmptyChain
import com.gsk.kg.engine.DAG
import com.gsk.kg.engine.graph.RenameQuadsInsideGraph
import com.gsk.kg.sparql.syntax.all.SparqlQueryInterpolator
import higherkindness.droste.data.Fix
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import cats.instances.string._
import com.gsk.kg.engine.DAG.BGP
import com.gsk.kg.engine.DAG.Project
import com.gsk.kg.engine.DAG.Scan
import com.gsk.kg.engine.data.ToTree
import com.gsk.kg.sparqlparser.StringVal.GRAPH_VARIABLE
import com.gsk.kg.sparqlparser.StringVal.URIVAL

class RenameQuadsInsideGraphSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  type T = Fix[DAG]

  "RenameQuadsInsideGraph" should "rename the graph column of quads when inside a GRAPH statement" in {

    import ToTree._

    val query =
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
      case Project(_, Fix(Project(_, Fix(Scan(_, Fix(BGP(quads))))))) =>
        quads.mapChunks(_.map(_.g shouldEqual GRAPH_VARIABLE))
      case _ => fail
    }

    val renamed = RenameQuadsInsideGraph[T].apply(dag)
    Fix.un(renamed) match {
      case Project(_, Fix(Project(_, Fix(Scan(graph, Fix(BGP(quads))))))) =>
        quads.mapChunks(_.map(_.g shouldEqual URIVAL(graph)))
    }
  }
}
