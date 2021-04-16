package com.gsk.kg.sparql.syntax

import com.gsk.kg.config.Config
import com.gsk.kg.sparqlparser.Query
import com.gsk.kg.sparqlparser.QueryConstruct

trait Interpolators {

  implicit class SparqlQueryInterpolator(sc: StringContext) {

    def sparql(args: Any*)(config: Config): Query = {
      val strings     = sc.parts.iterator
      val expressions = args.iterator
      val buf         = new StringBuilder(strings.next())
      while (strings.hasNext) {
        buf.append(expressions.next())
        buf.append(strings.next())
      }
      QueryConstruct.parse(buf.toString(), config)._1
    }

  }

}

object Interpolators extends Interpolators
