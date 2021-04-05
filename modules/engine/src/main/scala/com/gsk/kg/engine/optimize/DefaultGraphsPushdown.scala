package com.gsk.kg.engine.optimize

import higherkindness.droste.Algebra
import higherkindness.droste.Basis
import higherkindness.droste.scheme

import com.gsk.kg.engine.DAG
import com.gsk.kg.sparqlparser.StringVal

object DefaultGraphsPushdown {

  def apply[T](implicit T: Basis[DAG, T]): (T, List[StringVal]) => T = {
    case (t, defaultGraphs) =>
      val alg: Algebra[DAG, List[StringVal] => T] =
        Algebra[DAG, List[StringVal] => T] {
          case DAG.Describe(vars, r) => graphs => DAG.describeR(vars, r(graphs))
          case DAG.Ask(r)            => graphs => DAG.askR(r(graphs))
          case DAG.Construct(bgp, r) => graphs => DAG.constructR(bgp, r(graphs))
          case DAG.Scan(graph, expr) => graphs => DAG.scanR(graph, expr(graphs))
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
          case DAG.Noop(graphs) => _ => DAG.noopR(graphs)
        }

      val eval = scheme.cata(alg)

      eval(t)(defaultGraphs)
  }
}
