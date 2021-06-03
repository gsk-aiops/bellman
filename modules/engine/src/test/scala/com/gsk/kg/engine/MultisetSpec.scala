package com.gsk.kg.engine

import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.engine.utils.MultisetMatchers
import com.gsk.kg.sparqlparser.EngineError
import com.gsk.kg.sparqlparser.StringVal.GRAPH_VARIABLE
import com.gsk.kg.sparqlparser.StringVal.VARIABLE

import org.scalatest.wordspec.AnyWordSpec

class MultisetSpec extends AnyWordSpec with SparkSpec with MultisetMatchers {

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  "Multiset.join" should {

    "join two empty multisets together" in {
      val ms1 = Multiset.empty
      val ms2 = Multiset.empty

      ms1.join(ms2) should equalsMultiset(Multiset.empty)
    }

    "join other nonempty multiset on the right" in {
      import sqlContext.implicits._
      val empty = Multiset.empty
      val nonEmpty = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        Seq(("test1", "graph1"), ("test2", "graph2"))
          .toDF("d", GRAPH_VARIABLE.s)
      )

      empty.join(nonEmpty) should equalsMultiset(nonEmpty)
    }

    "join other nonempty multiset on the left" in {
      import sqlContext.implicits._
      val empty = Multiset.empty
      val nonEmpty = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        Seq(("test1", "graph1"), ("test2", "graph2"))
          .toDF("d", GRAPH_VARIABLE.s)
      )

      nonEmpty.join(empty) should equalsMultiset(nonEmpty)
    }

    "join other multiset when they have both the same single binding" in {
      import sqlContext.implicits._
      val variables = Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s))
      val df1 = List(
        ("test1", "graph1"),
        ("test2", "graph1"),
        ("test3", "graph1")
      ).toDF("d", GRAPH_VARIABLE.s)
      val df2 = List(
        ("test2", "graph1"),
        ("test3", "graph2"),
        ("test4", "graph2")
      ).toDF("d", GRAPH_VARIABLE.s)

      val ms1 = Multiset(variables, df1)
      val ms2 = Multiset(variables, df2)

      ms1.join(ms2) should equalsMultiset(
        Multiset(
          variables,
          List(("test2", "graph1"), ("test3", ""))
            .toDF("d", GRAPH_VARIABLE.s)
        )
      )
    }

    "join other multiset when they share one binding" in {
      import sqlContext.implicits._
      val d = VARIABLE("d")
      val e = VARIABLE("e")
      val f = VARIABLE("f")

      val ms1 = Multiset(
        Set(d, e, VARIABLE(GRAPH_VARIABLE.s)),
        List(
          ("test1", "123", "graph1"),
          ("test2", "456", "graph1"),
          ("test3", "789", "graph1")
        )
          .toDF(d.s, e.s, GRAPH_VARIABLE.s)
      )
      val ms2 = Multiset(
        Set(d, f, VARIABLE(GRAPH_VARIABLE.s)),
        List(
          ("test2", "hello", "graph1"),
          ("test3", "goodbye", "graph2"),
          ("test4", "hola", "graph2")
        )
          .toDF(d.s, f.s, GRAPH_VARIABLE.s)
      )

      ms1.join(ms2) should equalsMultiset(
        Multiset(
          Set(d, e, f, VARIABLE(GRAPH_VARIABLE.s)),
          List(
            ("test2", "456", "hello", "graph1"),
            ("test3", "789", "goodbye", "")
          )
            .toDF("d", "e", "f", GRAPH_VARIABLE.s)
        )
      )
    }

    "join other multiset when they share more than one binding" in {
      import sqlContext.implicits._
      val d = VARIABLE("d")
      val e = VARIABLE("e")
      val f = VARIABLE("f")
      val g = VARIABLE("g")
      val h = VARIABLE("h")

      val ms1 = Multiset(
        Set(d, e, g, h, VARIABLE(GRAPH_VARIABLE.s)),
        List(
          ("test1", "123", "g1", "h1", "graph1"),
          ("test2", "456", "g2", "h2", "graph1"),
          ("test3", "789", "g3", "h3", "graph1")
        ).toDF(d.s, e.s, g.s, h.s, GRAPH_VARIABLE.s)
      )
      val ms2 = Multiset(
        Set(d, e, f, VARIABLE(GRAPH_VARIABLE.s)),
        List(
          ("test2", "456", "hello", "graph1"),
          ("test3", "789", "goodbye", "graph2"),
          ("test4", "012", "hola", "graph2")
        ).toDF(d.s, e.s, f.s, GRAPH_VARIABLE.s)
      )

      val result = ms1.join(ms2)
      val expectedResult = Multiset(
        Set(d, e, f, g, h, VARIABLE(GRAPH_VARIABLE.s)),
        List(
          ("test2", "456", "g2", "h2", "hello", "graph1"),
          ("test3", "789", "g3", "h3", "goodbye", "")
        ).toDF("d", "e", "g", "h", "f", GRAPH_VARIABLE.s)
      )

      result should equalsMultiset(expectedResult)
    }

    "perform a cartesian product when there's no shared bindings between multisets" in {
      import sqlContext.implicits._
      val d = VARIABLE("d")
      val e = VARIABLE("e")
      val f = VARIABLE("f")
      val g = VARIABLE("g")
      val h = VARIABLE("h")
      val i = VARIABLE("i")

      val ms1 = Multiset(
        Set(d, e, f, VARIABLE(GRAPH_VARIABLE.s)),
        List(
          ("test1", "123", "g1", "graph1"),
          ("test2", "456", "g2", "graph1"),
          ("test3", "789", "g3", "graph1")
        ).toDF(d.s, e.s, f.s, GRAPH_VARIABLE.s)
      )
      val ms2 = Multiset(
        Set(g, h, i, VARIABLE(GRAPH_VARIABLE.s)),
        List(
          ("test2", "456", "hello", "graph1"),
          ("test3", "789", "goodbye", "graph2"),
          ("test4", "012", "hola", "graph2")
        ).toDF(g.s, h.s, i.s, GRAPH_VARIABLE.s)
      )

      val result = ms1.join(ms2)
      result should equalsMultiset(
        Multiset(
          Set(d, e, f, g, h, i, VARIABLE(GRAPH_VARIABLE.s)),
          List(
            ("test1", "123", "g1", "graph1", "test2", "456", "hello", "graph1"),
            (
              "test1",
              "123",
              "g1",
              "graph1",
              "test3",
              "789",
              "goodbye",
              "graph2"
            ),
            ("test1", "123", "g1", "graph1", "test4", "012", "hola", "graph2"),
            ("test2", "456", "g2", "graph1", "test2", "456", "hello", "graph1"),
            (
              "test2",
              "456",
              "g2",
              "graph1",
              "test3",
              "789",
              "goodbye",
              "graph2"
            ),
            ("test2", "456", "g2", "graph1", "test4", "012", "hola", "graph2"),
            ("test3", "789", "g3", "graph1", "test2", "456", "hello", "graph1"),
            (
              "test3",
              "789",
              "g3",
              "graph1",
              "test3",
              "789",
              "goodbye",
              "graph2"
            ),
            ("test3", "789", "g3", "graph1", "test4", "012", "hola", "graph2")
          ).toDF(
            "d",
            "e",
            "g",
            GRAPH_VARIABLE.s,
            "h",
            "f",
            "i",
            GRAPH_VARIABLE.s
          )
        )
      )
    }
  }

  "Multiset.leftJoin" should {

    "return empty multiset when left joined two empty multisets" in {
      val ms1 = Multiset.empty
      val ms2 = Multiset.empty

      val result = ms1.leftJoin(ms2)

      result shouldBe a[Right[_, _]]
      result.right.get should equalsMultiset(Multiset.empty)
    }

    "return empty multiset when empty multiset on left and non empty multiset on right" in {
      import sqlContext.implicits._
      val leftEmpty = Multiset.empty
      val rightNonEmpty = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        Seq(("test1", "graph1"), ("test2", "graph1"))
          .toDF("d", GRAPH_VARIABLE.s)
      )

      val result = leftEmpty.leftJoin(rightNonEmpty)

      result shouldBe a[Right[_, _]]
      result.right.get should equalsMultiset(leftEmpty)
    }

    "return right multiset when non empty multiset on left and empty multiset on right" in {
      import sqlContext.implicits._
      val rightEmpty = Multiset.empty
      val leftNonEmpty = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        Seq(("test1", "graph1"), ("test2", "graph1"))
          .toDF("d", GRAPH_VARIABLE.s)
      )

      val result = leftNonEmpty.leftJoin(rightEmpty)

      result shouldBe a[Right[_, _]]
      result.right.get should equalsMultiset(leftNonEmpty)
    }

    "return left multiset joined with right multiset when they have both the same single binding" in {
      import sqlContext.implicits._
      val variables = Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s))
      val leftDf = List(
        ("test1", "graph1"),
        ("test2", "graph1")
      ).toDF("d", GRAPH_VARIABLE.s)
      val rightDf = List(
        ("test1", "graph2"),
        ("test3", "graph2")
      ).toDF("d", GRAPH_VARIABLE.s)

      val left  = Multiset(variables, leftDf)
      val right = Multiset(variables, rightDf)

      val result = left.leftJoin(right)

      result shouldBe a[Right[_, _]]
      result.right.get should equalsMultiset(
        Multiset(
          variables,
          List(("test1", "graph1"), ("test2", "graph1"))
            .toDF("d", GRAPH_VARIABLE.s)
        )
      )
    }

    "return left multiset joined with right multiset when they share one binding" in {
      import sqlContext.implicits._
      val d     = VARIABLE("d")
      val e     = VARIABLE("e")
      val f     = VARIABLE("f")
      val graph = VARIABLE(GRAPH_VARIABLE.s)

      val left = Multiset(
        Set(d, e, graph),
        List(("test1", "234", "graph1"), ("test2", "123", "graph1"))
          .toDF(d.s, e.s, graph.s)
      )
      val right = Multiset(
        Set(d, f, graph),
        List(("test1", "hello", "graph2"), ("test3", "goodbye", "graph2"))
          .toDF(d.s, f.s, graph.s)
      )

      val result = left.leftJoin(right)

      result shouldBe a[Right[_, _]]
      result.right.get should equalsMultiset(
        Multiset(
          Set(d, e, f, graph),
          List(
            ("test1", "234", "hello", "graph1"),
            ("test2", "123", null, "graph1")
          ).toDF("d", "e", "f", graph.s)
        )
      )
    }

    "return left when there's no shared bindings between multisets" in {
      import sqlContext.implicits._
      val d     = VARIABLE("d")
      val e     = VARIABLE("e")
      val f     = VARIABLE("f")
      val g     = VARIABLE("g")
      val h     = VARIABLE("h")
      val graph = VARIABLE(GRAPH_VARIABLE.s)

      val left = Multiset(
        Set(d, e, g, h, graph),
        List(
          ("test1", "234", "g1", "h1", "graph1"),
          ("test2", "123", "g2", "h2", "graph2")
        )
          .toDF(d.s, e.s, g.s, h.s, graph.s)
      )
      val right = Multiset(
        Set(d, e, f, graph),
        List(
          ("test1", "234", "hello", "graph3"),
          ("test3", "e2", "goodbye", "graph4")
        )
          .toDF(d.s, e.s, f.s, graph.s)
      )

      val result = left.leftJoin(right)
      val expectedResult = Multiset(
        Set(d, e, f, g, h, graph),
        List(
          ("test1", "234", "g1", "h1", "hello", "graph1"),
          ("test2", "123", "g2", "h2", null, "graph2")
        ).toDF("d", "e", "g", "h", "f", graph.s)
      )

      result shouldBe a[Right[_, _]]
      result.right.get should equalsMultiset(expectedResult)
    }
  }

  "Multiset.union" should {

    "union two empty multisets together" in {
      val left  = Multiset.empty
      val right = Multiset.empty

      left.union(right) should equalsMultiset(Multiset.empty)
    }

    "union other non empty multiset on right" in {
      import sqlContext.implicits._

      val left = Multiset.empty
      val right = Multiset(
        Set(VARIABLE("a"), VARIABLE(GRAPH_VARIABLE.s)),
        List(("A", "graph1"), ("B", "graph2"), ("C", "graph3"))
          .toDF("a", GRAPH_VARIABLE.s)
      )

      left.union(right) should equalsMultiset(right)
    }

    "union other non empty multiset on left" in {
      import sqlContext.implicits._

      val left = Multiset(
        Set(VARIABLE("a"), VARIABLE(GRAPH_VARIABLE.s)),
        List(("A", "graph1"), ("B", "graph2"), ("C", "graph3"))
          .toDF("a", GRAPH_VARIABLE.s)
      )
      val right = Multiset.empty

      left.union(right) should equalsMultiset(left)
    }

    "union two multisets when they have both the single bindings" in {
      import sqlContext.implicits._
      val variables = Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s))

      val left = Multiset(
        variables,
        List(("test1", "graph1"), ("test2", "graph1"))
          .toDF("d", GRAPH_VARIABLE.s)
      )
      val right = Multiset(
        variables,
        List(("test2", "graph2"), ("test3", "graph2"))
          .toDF("d", GRAPH_VARIABLE.s)
      )

      val result = left.union(right)
      val expectedResult = Multiset(
        variables,
        List(
          ("test1", "graph1"),
          ("test2", "graph1"),
          ("test2", "graph2"),
          ("test3", "graph2")
        ).toDF("d", GRAPH_VARIABLE.s)
      )

      result should equalsMultiset(expectedResult)
    }

    "union two multisets when they share one binding" in {
      import sqlContext.implicits._
      val d     = VARIABLE("d")
      val e     = VARIABLE("e")
      val f     = VARIABLE("f")
      val graph = VARIABLE(GRAPH_VARIABLE.s)

      val left = Multiset(
        Set(d, e, graph),
        List(("test1", "234", "graph1"), ("test2", "123", "graph1"))
          .toDF(d.s, e.s, graph.s)
      )
      val right = Multiset(
        Set(d, f),
        List(("test1", "hello", "graph2"), ("test3", "goodbye", "graph2"))
          .toDF(d.s, f.s, graph.s)
      )

      left.union(right) should equalsMultiset(
        Multiset(
          Set(d, e, f, graph),
          List(
            ("test1", "234", null, "graph1"),
            ("test2", "123", null, "graph1"),
            ("test1", null, "hello", "graph2"),
            ("test3", null, "goodbye", "graph2")
          ).toDF("d", "e", "f", graph.s)
        )
      )
    }

    "union other multiset when they share more than one binding" in {
      import sqlContext.implicits._
      val d     = VARIABLE("d")
      val e     = VARIABLE("e")
      val f     = VARIABLE("f")
      val g     = VARIABLE("g")
      val h     = VARIABLE("h")
      val graph = VARIABLE(GRAPH_VARIABLE.s)

      val left = Multiset(
        Set(d, e, g, h, graph),
        List(
          ("test1", "234", "g1", "h1", "graph1"),
          ("test2", "123", "g2", "h2", "graph1")
        )
          .toDF(d.s, e.s, g.s, h.s, graph.s)
      )
      val right = Multiset(
        Set(d, e, f, graph),
        List(
          ("test1", "234", "hello", "graph2"),
          ("test3", "e2", "goodbye", "graph2")
        )
          .toDF(d.s, e.s, f.s, graph.s)
      )

      val result = left.union(right)
      val expectedResult = Multiset(
        Set(d, e, f, g, h, graph),
        List(
          ("test1", "234", "g1", "h1", null, "graph1"),
          ("test2", "123", "g2", "h2", null, "graph1"),
          ("test1", "234", null, null, "hello", "graph2"),
          ("test3", "e2", null, null, "goodbye", "graph2")
        ).toDF("d", "e", "g", "h", "f", graph.s)
      )

      result should equalsMultiset(expectedResult)
    }
  }

  "Multiset.limit" should {

    "return solutions limited by l when l > 0" in {
      import sqlContext.implicits._
      val m = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        List(("a", ""), ("b", ""), ("c", "")).toDF("d", GRAPH_VARIABLE.s)
      )

      val result = m.limit(2)

      result shouldBe a[Right[_, _]]
      result.right.get should equalsMultiset(
        Multiset(
          Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
          List(("a", ""), ("b", "")).toDF("d", GRAPH_VARIABLE.s)
        )
      )
    }

    "return no solutions when l = 0" in {
      import sqlContext.implicits._
      val m = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        List(("a", ""), ("b", ""), ("c", "")).toDF("d", GRAPH_VARIABLE.s)
      )

      val result = m.limit(0)

      result shouldBe a[Right[_, _]]
      result.right.get should equalsMultiset(
        Multiset(
          Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
          spark.emptyDataFrame
        )
      )
    }

    "return error when l > MAX_INTEGER" in {
      import sqlContext.implicits._
      val m = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        List(("a", ""), ("b", ""), ("c", "")).toDF("d", GRAPH_VARIABLE.s)
      )

      val result = m.limit(Integer.MAX_VALUE.toLong + 1)

      result shouldBe a[Left[_, _]]
      result.left.get shouldBe a[EngineError.NumericTypesDoNotMatch]
    }

    "return error when l < 0" in {
      import sqlContext.implicits._
      val m = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        List(("a", ""), ("b", ""), ("c", "")).toDF("d", GRAPH_VARIABLE.s)
      )

      val result = m.limit(-1)

      result shouldBe a[Left[_, _]]
      result.left.get shouldBe a[EngineError.UnexpectedNegative]
    }
  }

  "Multiset.offset" should {

    "return solutions form offset" in {
      import sqlContext.implicits._
      val m = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        List(("a", ""), ("b", ""), ("c", "")).toDF("d", GRAPH_VARIABLE.s)
      )

      val result = m.offset(1)

      result shouldBe a[Right[_, _]]
      result.right.get should equalsMultiset(
        Multiset(
          Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
          List(("b", ""), ("c", "")).toDF("d", GRAPH_VARIABLE.s)
        )
      )
    }

    "return no solutions when offset is greater than size of initial dataframe" in {
      import sqlContext.implicits._
      val m = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        List(("a", ""), ("b", ""), ("c", "")).toDF("d", GRAPH_VARIABLE.s)
      )

      val result = m.offset(5)

      result shouldBe a[Right[_, _]]
      result.right.get should equalsMultiset(
        Multiset(
          Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
          spark.emptyDataFrame
        )
      )
    }

    "return all solutions when offset is 0" in {
      import sqlContext.implicits._
      val m = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        List(("a", ""), ("b", ""), ("c", "")).toDF("d", GRAPH_VARIABLE.s)
      )

      val result = m.offset(0)

      result shouldBe a[Right[_, _]]
      result.right.get should equalsMultiset(
        Multiset(
          Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
          List(("a", ""), ("b", ""), ("c", "")).toDF("d", GRAPH_VARIABLE.s)
        )
      )
    }

    "return 0 when offset is negative" in {
      import sqlContext.implicits._
      val m = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        List(("a", ""), ("b", ""), ("c", "")).toDF("d", GRAPH_VARIABLE.s)
      )

      val result = m.offset(-1)

      result shouldBe a[Left[_, _]]
      result.left.get shouldBe a[EngineError.UnexpectedNegative]
    }
  }

  // TODO: Add unit tests for filter
  "Multiset.filter" should {}

  // TODO: Add unit test for distinct
  "Multiset.distinct" should {}

  // TODO: Add unit test for withColumn
  "Multiset.withColumn" should {}

  // TODO: Add unit test for select
  "Multiset.select" should {}

  // TODO: Add unit test for isEmpty
  "Multiset.empty" should {}

}
