package com.gsk.kg.engine.optimize

import cats.implicits._

import higherkindness.droste.Algebra
import higherkindness.droste.Basis
import higherkindness.droste.scheme

import com.gsk.kg.Graphs
import com.gsk.kg.engine.DAG
import com.gsk.kg.sparqlparser.StringVal
import com.gsk.kg.sparqlparser.StringVal.URIVAL

/** Rename the graph column of quads with a list of graphs, the list can contain:
  * - A list with default graphs if Quads are not inside a GRAPH statement.
  * - A list with the named graph if Quads are inside a GRAPH statement.
  * Also it performs an optimization by remove Scan expression on the DAG.
  * Lets see an example of the pushdown. Eg:
  *
  * Initial DAG without renaming:
  * Project
  * |
  * +- List(VARIABLE(?mbox), VARIABLE(?name))
  * |
  * `- Project
  *   |
  *   +- List(VARIABLE(?mbox), VARIABLE(?name))
  *   |
  *   `- Join
  *      |
  *      +- BGP
  *      |  |
  *      |  `- ChunkedList.Node
  *      |     |
  *      |     `- NonEmptyChain
  *      |        |
  *      |        `- Quad
  *      |           |
  *      |           +- ?x
  *      |           |
  *      |           +- http://xmlns.com/foaf/0.1/name
  *      |           |
  *      |           +- ?name
  *      |           |
  *      |           `- List(GRAPH_VARIABLE)
  *      |
  *      `- Scan
  *         |
  *         +- http://example.org/alice
  *         |
  *         `- BGP
  *            |
  *            `- ChunkedList.Node
  *               |
  *               `- NonEmptyChain
  *                  |
  *                  `- Quad
  *                     |
  *                     +- ?x
  *                     |
  *                     +- http://xmlns.com/foaf/0.1/mbox
  *                     |
  *                     +- ?mbox
  *                     |
  *                     `- List(GRAPH_VARIABLE)
  *
  * DAG when renamed quads inside graph statement:
  * Project
  * |
  * +- List(VARIABLE(?mbox), VARIABLE(?name))
  * |
  * `- Project
  *   |
  *   +- List(VARIABLE(?mbox), VARIABLE(?name))
  *   |
  *   `- Join
  *      |
  *      +- BGP
  *      |  |
  *      |  `- ChunkedList.Node
  *      |     |
  *      |     `- NonEmptyChain
  *      |        |
  *      |        `- Quad
  *      |           |
  *      |           +- ?x
  *      |           |
  *      |           +- http://xmlns.com/foaf/0.1/name
  *      |           |
  *      |           +- ?name
  *      |           |
  *      |           `- List(URIVAL(http://example.org/dft.ttl), URIVAL())
  *      |
  *      `- BGP
  *         |
  *         `- ChunkedList.Node
  *            |
  *            `- NonEmptyChain
  *               |
  *               `- Quad
  *                  |
  *                  +- ?x
  *                  |
  *                  +- http://xmlns.com/foaf/0.1/mbox
  *                  |
  *                  +- ?mbox
  *                  |
  *                  `- List(URIVAL(http://example.org/alice))
  *
  * The trick we're doing here in order to pass information from
  * parent nodes to child nodes in the [[DAG]] is to have a carrier
  * function as the result value in the
  * [[higherkindness.droste.Algebra]].  That way, we can make parents,
  * such as the case of the [[Scan]] in this case, pass information to
  * children as part of the parameter of the carrier function.
  */
object GraphsPushdown {

  type GraphsOrList = Either[List[StringVal], Graphs]

  def apply[T](implicit T: Basis[DAG, T]): (T, Graphs) => T = {
    case (t, graphs) =>
      val alg: Algebra[DAG, GraphsOrList => T] =
        Algebra[DAG, GraphsOrList => T] {
          case DAG.Describe(vars, r) =>
            graphsOrList => DAG.describeR(vars, r(graphsOrList))
          case DAG.Ask(r) => graphsOrList => DAG.askR(r(graphsOrList))
          case DAG.Construct(bgp, r) =>
            graphsOrList => DAG.constructR(bgp, r(graphsOrList))
          case DAG.Scan(graph, expr) =>
            _ =>
              if (graph.startsWith("?")) {
                DAG.scanR(graph, expr(graphs.named.asLeft))
              } else {
                expr((URIVAL(graph) :: Nil).asLeft)
              }
          case DAG.Project(variables, r) =>
            graphsOrList => DAG.projectR(variables, r(graphsOrList))
          case DAG.Bind(variable, expression, r) =>
            graphsOrList => DAG.bindR(variable, expression, r(graphsOrList))
          case DAG.BGP(quads) =>
            graphsOrList =>
              graphsOrList.fold(
                list => DAG.bgpR(quads.flatMapChunks(_.map(_.copy(g = list)))),
                graphs =>
                  DAG.bgpR(
                    quads.flatMapChunks(_.map(_.copy(g = graphs.default)))
                  )
              )
          case DAG.LeftJoin(l, r, filters) =>
            graphsOrList =>
              DAG.leftJoinR(l(graphsOrList), r(graphsOrList), filters)
          case DAG.Union(l, r) =>
            graphsOrList => DAG.unionR(l(graphsOrList), r(graphsOrList))
          case DAG.Filter(funcs, expr) =>
            graphsOrList => DAG.filterR(funcs, expr(graphsOrList))
          case DAG.Join(l, r) =>
            graphsOrList => DAG.joinR(l(graphsOrList), r(graphsOrList))
          case DAG.Offset(o, r) =>
            graphsOrList => DAG.offsetR(o, r(graphsOrList))
          case DAG.Limit(l, r) => graphsOrList => DAG.limitR(l, r(graphsOrList))
          case DAG.Distinct(r) => graphsOrList => DAG.distinctR(r(graphsOrList))
          case DAG.Group(vars, func, r) =>
            graphsOrList => DAG.groupR(vars, func, r(graphsOrList))
          case DAG.Noop(graphsOrList) => _ => DAG.noopR(graphsOrList)
        }

      val eval = scheme.cata(alg)
      eval(t)(Right(graphs))
  }
}
