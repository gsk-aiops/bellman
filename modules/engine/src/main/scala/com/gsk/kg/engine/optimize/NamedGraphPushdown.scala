package com.gsk.kg.engine
package optimizer

import higherkindness.droste.Algebra
import higherkindness.droste.Basis
import higherkindness.droste.scheme

import com.gsk.kg.engine.DAG
import com.gsk.kg.sparqlparser.StringVal
import com.gsk.kg.sparqlparser.StringVal.GRAPH_VARIABLE
import com.gsk.kg.sparqlparser.StringVal.URIVAL

/** Rename the graph column of quads inside a graph statement, so the
  * graph column will contain the graph that is being queried on the
  * subtree DAG. Eg:
  *
  * Initial DAG without renaming:
  * Project
  *  |
  *  +- List(VARIABLE(?mbox), VARIABLE(?name))
  *  |
  *  `- Project
  *     |
  *     +- List(VARIABLE(?mbox), VARIABLE(?name))
  *     |
  *     `- Scan
  *        |
  *        +- http://example.org/alice
  *        |
  *        `- BGP
  *           |
  *           `- ChunkedList.Node
  *              |
  *              +- NonEmptyChain
  *              |  |
  *              |  `- Quad
  *              |     |
  *              |     +- ?x
  *              |     |
  *              |     +- http://xmlns.com/foaf/0.1/name
  *              |     |
  *              |     +- ?name
  *              |     |
  *              |     `- *g
  *              |
  *              `- NonEmptyChain
  *                 |
  *                 `- Quad
  *                    |
  *                    +- ?x
  *                    |
  *                    +- http://xmlns.com/foaf/0.1/mbox
  *                    |
  *                    +- ?mbox
  *                    |
  *                    `- *g
  *
  * DAG when renamed quads inside graph statement:
  *  Project
  *  |
  *  +- List(VARIABLE(?mbox), VARIABLE(?name))
  *  |
  *  `- Project
  *     |
  *     +- List(VARIABLE(?mbox), VARIABLE(?name))
  *     |
  *     `- BGP
  *        |
  *        `- ChunkedList.Node
  *           |
  *           +- NonEmptyChain
  *           |  |
  *           |  `- Quad
  *           |     |
  *           |     +- ?x
  *           |     |
  *           |     +- http://xmlns.com/foaf/0.1/name
  *           |     |
  *           |     +- ?name
  *           |     |
  *           |     `- http://example.org/alice
  *           |
  *           `- NonEmptyChain
  *              |
  *              `- Quad
  *                 |
  *                 +- ?x
  *                 |
  *                 +- http://xmlns.com/foaf/0.1/mbox
  *                 |
  *                 +- ?mbox
  *                 |
  *                 `- http://example.org/alice
  *
  * The trick we're doing here in order to pass information from
  * parent nodes to child nodes in the [[DAG]] is to have a carrier
  * function as the result value in the
  * [[higherkindness.droste.Algebra]].  That way, we can make parents,
  * such as the case of the [[Scan]] in this case, pass information to
  * children as part of the parameter of the carrier function.
  */
object NamedGraphPushdown {

  def apply[T](implicit T: Basis[DAG, T]): T => T = { t =>
    val alg: Algebra[DAG, StringVal => T] = Algebra[DAG, StringVal => T] {
      case DAG.Describe(vars, r)     => str => DAG.describeR(vars, r(str))
      case DAG.Ask(r)                => str => DAG.askR(r(str))
      case DAG.Construct(bgp, r)     => str => DAG.constructR(bgp, r(str))
      case DAG.Scan(graph, expr)     => str => expr(URIVAL(graph))
      case DAG.Project(variables, r) => str => DAG.projectR(variables, r(str))
      case DAG.Bind(variable, expression, r) =>
        str => DAG.bindR(variable, expression, r(str))
      case DAG.BGP(quads) =>
        str => DAG.bgpR(quads.flatMapChunks(_.map(_.copy(g = str :: Nil))))
      case DAG.LeftJoin(l, r, filters) =>
        str => DAG.leftJoinR(l(str), r(str), filters)
      case DAG.Union(l, r)         => str => DAG.unionR(l(str), r(str))
      case DAG.Filter(funcs, expr) => str => DAG.filterR(funcs, expr(str))
      case DAG.Join(l, r)          => str => DAG.joinR(l(str), r(str))
      case DAG.Offset(o, r)        => str => DAG.offsetR(o, r(str))
      case DAG.Limit(l, r)         => str => DAG.limitR(l, r(str))
      case DAG.Distinct(r)         => str => DAG.distinctR(r(str))
      case DAG.Noop(str)           => _ => DAG.noopR(str)
    }

    val eval = scheme.cata(alg)

    eval(t)(GRAPH_VARIABLE)
  }

}
