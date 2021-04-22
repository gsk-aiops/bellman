package com.gsk.kg.engine.optimize

import higherkindness.droste.data.Fix

import com.gsk.kg.engine.DAG
import com.gsk.kg.sparqlparser.TestConfig
import com.gsk.kg.sparqlparser.TestUtils

import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SubqueryPushdownSpec
    extends AnyWordSpec
    with Matchers
    with TestUtils
    with TestConfig {

  type T = Fix[DAG]

  "SubqueryPushdown" should {

    "push-down graph variable to inner subqueries" when {

      "outer SELECT and" when {

        "inner SELECT" in {}

        "inner CONSTRUCT" in {}

        // TODO: Un-ignore when implemented ASK
        "inner ASK" ignore {}

        // TODO: Un-ignore when implemented DESCRIBE
        "inner DESCRIBE" ignore {}
      }

      "outer CONSTRUCT and" when {

        "inner SELECT" in {}

        "inner CONSTRUCT" in {}

        // TODO: Un-ignore when implemented ASK
        "inner ASK" ignore {}

        // TODO: Un-ignore when implemented DESCRIBE
        "inner DESCRIBE" ignore {}
      }

      // TODO: Un-ignore when implemented ASK
      "outer ASK and" ignore {

        "inner SELECT" in {}

        "inner CONSTRUCT" in {}

        "inner ASK" in {}

        "inner DESCRIBE" in {}
      }

      // TODO: Un-ignore when implemented DESCRIBE
      "outer DESCRIBE and" when {

        "inner SELECT" in {}

        "inner CONSTRUCT" in {}

        "inner ASK" in {}

        "inner DESCRIBE" in {}
      }

      "multiple inner sub-queries" when {}

      "mixing graphs" when {}
    }
  }
}
