package com.gsk.kg.engine.optimizer

import higherkindness.droste.Algebra
import higherkindness.droste.Basis
import higherkindness.droste.scheme

import com.gsk.kg.engine.DAG
import com.gsk.kg.sparqlparser.StringVal.GRAPH_VARIABLE
import com.gsk.kg.sparqlparser.StringVal.VARIABLE

/** Adds the graph column as a variable to all subqueries. This is done when
  * an Ask, Construct or Project node is detected as the topmost node then
  * the subsequent Project nodes add the Graph column as a variable
  * Lets see an example of the pushdown. Eg:
  *
  * Initial DAG without adding graph column to subqueries:
  * Construct
  * |
  * +- BGP
  * |  |
  * |  `- ChunkedList.Node
  * |     |
  * |     `- NonEmptyChain
  * |        |
  * |        `- Quad
  * |           |
  * |           +- ?y
  * |           |
  * |           +- http://xmlns.com/foaf/0.1/knows
  * |           |
  * |           +- ?name
  * |           |
  * |           `- List(URIVAL(urn:x-arq:DefaultGraphNode))
  * |
  * `- Join
  *   |
  *   +- BGP
  *   |  |
  *   |  `- ChunkedList.Node
  *   |     |
  *   |     `- NonEmptyChain
  *   |        |
  *   |        `- Quad
  *   |           |
  *   |           +- ?y
  *   |           |
  *   |           +- http://xmlns.com/foaf/0.1/knows
  *   |           |
  *   |           +- ?x
  *   |           |
  *   |           `- List(GRAPH_VARIABLE)
  *   |
  *   `- Project
  *      |
  *      +- List
  *      |  |
  *      |  +- VARIABLE(?x)
  *      |  |
  *      |  `- VARIABLE(?name)
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
  *                  +- http://xmlns.com/foaf/0.1/name
  *                  |
  *                  +- ?name
  *                  |
  *                  `- List(GRAPH_VARIABLE)
  *
  * DAG when added graph column variable to subqueries:
  * Construct
  * |
  * +- BGP
  * |  |
  * |  `- ChunkedList.Node
  * |     |
  * |     `- NonEmptyChain
  * |        |
  * |        `- Quad
  * |           |
  * |           +- ?y
  * |           |
  * |           +- http://xmlns.com/foaf/0.1/knows
  * |           |
  * |           +- ?name
  * |           |
  * |           `- List(URIVAL(urn:x-arq:DefaultGraphNode))
  * |
  * `- Join
  *   |
  *   +- BGP
  *   |  |
  *   |  `- ChunkedList.Node
  *   |     |
  *   |     `- NonEmptyChain
  *   |        |
  *   |        `- Quad
  *   |           |
  *   |           +- ?y
  *   |           |
  *   |           +- http://xmlns.com/foaf/0.1/knows
  *   |           |
  *   |           +- ?x
  *   |           |
  *   |           `- List(GRAPH_VARIABLE)
  *   |
  *   `- Project
  *      |
  *      +- List
  *      |  |
  *      |  +- VARIABLE(?x)
  *      |  |
  *      |  +- VARIABLE(?name)
  *      |  |
  *      |  `- VARIABLE(*g)
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
  *                  +- http://xmlns.com/foaf/0.1/name
  *                  |
  *                  +- ?name
  *                  |
  *                  `- List(GRAPH_VARIABLE)
  *
  * The trick we're doing here in order to pass information from
  * parent nodes to child nodes in the [[DAG]] is to have a carrier
  * function as the result value in the
  * [[higherkindness.droste.Algebra]].  That way, we can make parents,
  * pass information to children as part of the parameter of the carrier function.
  */

object SubqueryPushdown {

  private def toSubquery[T](dag: DAG[Boolean => T], isFromSubquery: Boolean)(
      implicit T: Basis[DAG, T]
  ): T = dag match {
    case DAG.Ask(r) if !isFromSubquery =>
      DAG.askR(r(true))
    case DAG.Ask(r) =>
      DAG.askR(r(isFromSubquery))
    case DAG.Construct(bgp, r) if !isFromSubquery =>
      DAG.constructR(bgp, r(true))
    case DAG.Construct(bgp, r) =>
      DAG.constructR(bgp, r(isFromSubquery))
    case DAG.Project(variables, r) if !isFromSubquery =>
      DAG.projectR(variables, r(true))
    case DAG.Project(variables, r) =>
      DAG.projectR(variables :+ VARIABLE(GRAPH_VARIABLE.s), r(isFromSubquery))
  }

  def apply[T](implicit T: Basis[DAG, T]): T => T = { t =>
    val alg: Algebra[DAG, Boolean => T] =
      Algebra[DAG, Boolean => T] {
        case dag @ DAG.Ask(_) =>
          isFromSubquery => toSubquery(dag, isFromSubquery)
        case dag @ DAG.Construct(_, _) =>
          isFromSubquery => toSubquery(dag, isFromSubquery)
        case dag @ DAG.Project(_, _) =>
          isFromSubquery => toSubquery(dag, isFromSubquery)
        case DAG.Describe(vars, r) =>
          isFromSubquery => DAG.describeR(vars, r(isFromSubquery))
        case DAG.Scan(graph, expr) =>
          isFromSubquery => DAG.scanR(graph, expr(isFromSubquery))
        case DAG.Bind(variable, expression, r) =>
          isFromSubquery => DAG.bindR(variable, expression, r(isFromSubquery))
        case DAG.BGP(quads) => _ => DAG.bgpR(quads)
        case DAG.LeftJoin(l, r, filters) =>
          isFromSubquery =>
            DAG.leftJoinR(l(isFromSubquery), r(isFromSubquery), filters)
        case DAG.Union(l, r) =>
          isFromSubquery => DAG.unionR(l(isFromSubquery), r(isFromSubquery))
        case DAG.Filter(funcs, expr) =>
          isFromSubquery => DAG.filterR(funcs, expr(isFromSubquery))
        case DAG.Join(l, r) =>
          isFromSubquery => DAG.joinR(l(isFromSubquery), r(isFromSubquery))
        case DAG.Offset(o, r) =>
          isFromSubquery => DAG.offsetR(o, r(isFromSubquery))
        case DAG.Limit(l, r) =>
          isFromSubquery => DAG.limitR(l, r(isFromSubquery))
        case DAG.Distinct(r) =>
          isFromSubquery => DAG.distinctR(r(isFromSubquery))
        case DAG.Group(vars, func, r) =>
          isFromSubquery => DAG.groupR(vars, func, r(isFromSubquery))
        case DAG.Noop(s) => _ => DAG.noopR(s)
      }

    val eval = scheme.cata(alg)
    eval(t)(false)
  }
}
