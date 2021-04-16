package com.gsk.kg

import com.gsk.kg.sparqlparser.StringVal

final case class Graphs(default: List[StringVal], named: List[StringVal])

object Graphs {
  def empty: Graphs = Graphs(List.empty, List.empty)
}
