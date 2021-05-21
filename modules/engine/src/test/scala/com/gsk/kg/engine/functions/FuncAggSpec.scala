package com.gsk.kg.engine.functions

import org.apache.spark.sql.Row

import com.gsk.kg.engine.scalacheck.CommonGenerators

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class FuncAggSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with ScalaCheckDrivenPropertyChecks
    with CommonGenerators {

  import sqlContext.implicits._

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  "FuncAgg.sample" should {

    "return an arbitrary value from the column" in {

      val elems = List(1, 2, 3, 4, 5)
      val df    = elems.toDF("a")

      elems.toSet should contain(
        df.select(FuncAgg.sample(df("a"))).collect().head.get(0)
      )
    }
  }

  "FuncAgg.count" should {

    "operate correctly on multiple types" in {
      val df = List(
        "1",
        "1.5",
        "\"2\"^^xsd:int",
        "\"3\"^^xsd:decimal",
        "\"2.0\"^^xsd:float",
        "\"3.5\"^^xsd:float",
        "\"4.2\"^^xsd:double",
        "\"4.5\"^^xsd:numeric",
        "non numeric type",
        "\"non numeric type\"^^xsd:string"
      ).toDF("v")

      val result = df.select(FuncAgg.countAgg(df("v"))).collect()

      result.toSet shouldEqual Set(Row(10))
    }
  }

  "FuncAgg.avgAgg" should {

    "operate correctly on multiple numeric types" in {

      val df = List(
        "1",
        "1.5",
        "\"2\"^^xsd:int",
        "\"3\"^^xsd:decimal",
        "\"2.0\"^^xsd:float",
        "\"3.5\"^^xsd:float",
        "\"4.2\"^^xsd:double",
        "\"4.5\"^^xsd:numeric",
        "non numeric type",
        "\"non numeric type\"^^xsd:string"
      ).toDF("v")

      val result = df.select(FuncAgg.avgAgg(df("v"))).collect()

      result.toSet shouldEqual Set(Row(2.7125))
    }
  }

  "FuncAgg.sumAgg" should {

    "operate correctly on multiple types" in {

      val df = List(
        "1",
        "1.5",
        "\"2\"^^xsd:int",
        "\"3\"^^xsd:decimal",
        "\"2.0\"^^xsd:float",
        "\"3.5\"^^xsd:float",
        "\"4.2\"^^xsd:double",
        "\"4.5\"^^xsd:numeric",
        "non numeric type",
        "\"non numeric type\"^^xsd:string"
      ).toDF("v")

      val result = df.select(FuncAgg.sumAgg(df("v"))).collect()

      result.toSet shouldEqual Set(Row(21.7))
    }
  }

  "FuncAgg.minAgg" should {

    "operate correctly on numeric types" in {

      val df = List(
        "1",
        "1.5",
        "\"2\"^^xsd:int",
        "\"3\"^^xsd:decimal",
        "\"2.0\"^^xsd:float",
        "\"3.5\"^^xsd:float",
        "\"4.2\"^^xsd:double",
        "\"4.5\"^^xsd:numeric"
      ).toDF("v")

      val result = df.select(FuncAgg.minAgg(df("v"))).collect()

      result.toSet shouldEqual Set(Row("1"))
    }

    "operate correctly on string types" in {

      val df = List(
        "alice",
        "bob",
        "\"alice\"^^xsd:string",
        "\"bob\"^^xsd:string"
      ).toDF("v")

      val result = df.select(FuncAgg.minAgg(df("v"))).collect()

      result.toSet shouldEqual Set(Row("alice"))
    }

    "operate correctly mixing types" in {

      val df = List(
        "1.0",
        "\"2.2\"^^xsd:float",
        "alice",
        "\"bob\"^^xsd:string"
      ).toDF("v")

      val result = df.select(FuncAgg.minAgg(df("v"))).collect()

      result.toSet shouldEqual Set(Row("1.0"))
    }
  }

  "FuncAgg.maxAgg" should {

    "operate correctly on numeric types" in {

      val df = List(
        "1",
        "1.5",
        "\"2\"^^xsd:int",
        "\"3\"^^xsd:decimal",
        "\"2.0\"^^xsd:float",
        "\"3.5\"^^xsd:float",
        "\"4.2\"^^xsd:double",
        "\"4.5\"^^xsd:numeric"
      ).toDF("v")

      val result = df.select(FuncAgg.maxAgg(df("v"))).collect()

      result.toSet shouldEqual Set(Row("4.5"))
    }

    "operate correctly on string types" in {

      val df = List(
        "alice",
        "bob",
        "\"alice\"^^xsd:string",
        "\"bob\"^^xsd:string"
      ).toDF("v")

      val result = df.select(FuncAgg.maxAgg(df("v"))).collect()

      result.toSet shouldEqual Set(Row("bob"))
    }

    "operate correctly mixing types" in {

      val df = List(
        "1.0",
        "\"2.2\"^^xsd:float",
        "alice",
        "\"bob\"^^xsd:string"
      ).toDF("v")

      val result = df.select(FuncAgg.maxAgg(df("v"))).collect()

      result.toSet shouldEqual Set(Row("bob"))
    }
  }

}
