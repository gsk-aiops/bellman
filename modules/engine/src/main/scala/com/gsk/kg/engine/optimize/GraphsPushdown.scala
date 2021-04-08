package com.gsk.kg.engine.optimize

import higherkindness.droste.Algebra
import higherkindness.droste.Basis
import higherkindness.droste.scheme

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

  def apply[T](implicit T: Basis[DAG, T]): (T, List[StringVal]) => T = {
    case (t, defaultGraphs) =>
      val alg: Algebra[DAG, List[StringVal] => T] =
        Algebra[DAG, List[StringVal] => T] {
          case DAG.Describe(vars, r) => graphs => DAG.describeR(vars, r(graphs))
          case DAG.Ask(r)            => graphs => DAG.askR(r(graphs))
          case DAG.Construct(bgp, r) => graphs => DAG.constructR(bgp, r(graphs))
          case DAG.Scan(graph, expr) =>
            graphs => expr(URIVAL(graph) :: Nil)
          case DAG.Project(variables, r) =>
            graphs => DAG.projectR(variables, r(graphs))
          case DAG.Bind(variable, expression, r) =>
            graphs => DAG.bindR(variable, expression, r(graphs))
          case DAG.BGP(quads) =>
            graphs => DAG.bgpR(quads.flatMapChunks(_.map(_.copy(g = graphs))))
          case DAG.LeftJoin(l, r, filters) =>
            graphs => DAG.leftJoinR(l(graphs), r(graphs), filters)
          case DAG.Union(l, r) => graphs => DAG.unionR(l(graphs), r(graphs))
          case DAG.Filter(funcs, expr) =>
            graphs => DAG.filterR(funcs, expr(graphs))
          case DAG.Join(l, r)   => graphs => DAG.joinR(l(graphs), r(graphs))
          case DAG.Offset(o, r) => graphs => DAG.offsetR(o, r(graphs))
          case DAG.Limit(l, r)  => graphs => DAG.limitR(l, r(graphs))
          case DAG.Distinct(r)  => graphs => DAG.distinctR(r(graphs))
          case DAG.Group(vars, func, r) =>
            graphs => DAG.groupR(vars, func, r(graphs))
          case DAG.Noop(graphs) => _ => DAG.noopR(graphs)
        }

      val eval = scheme.cata(alg)

      eval(t)(defaultGraphs)
  }
}
