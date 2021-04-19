package com.gsk.kg.sparql.syntax

import com.gsk.kg.config.Config
import com.gsk.kg.sparqlparser.Query
import com.gsk.kg.sparqlparser.QueryConstruct

trait Interpolators {

  implicit class SparqlQueryInterpolator(sc: StringContext) {

    /** This method uses a default configuration, if a custom configuration wanted to be provided
      * we recommend using the method [[QueryConstruct.parse()]] instead.
      * @param args
      * @return
      */
    def sparql(args: Any*): Query = {
      val strings     = sc.parts.iterator
      val expressions = args.iterator
      val buf         = new StringBuilder(strings.next())
      while (strings.hasNext) {
        buf.append(expressions.next())
        buf.append(strings.next())
      }
      QueryConstruct.parse(buf.toString(), Config.default)._1
    }

  }

}

object Interpolators extends Interpolators
