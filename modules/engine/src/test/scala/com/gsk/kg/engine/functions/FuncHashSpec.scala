package com.gsk.kg.engine.functions

import org.apache.spark.sql.Row

import com.gsk.kg.engine.scalacheck.CommonGenerators

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class FuncHashSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with ScalaCheckDrivenPropertyChecks
    with CommonGenerators {

  import sqlContext.implicits._

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  "Func on Hash" when {

    "FuncHash.md5" should {

      "correctly return hash" in {
        val initial = List(
          ("abc", "900150983cd24fb0d6963f7d28e17f72"),
          ("\"abc\"^^xsd:string", "900150983cd24fb0d6963f7d28e17f72")
        ).toDF("input", "expected")

        val df = initial.withColumn("result", FuncHash.md5(initial("input")))

        df.collect.foreach { case Row(_, expected, result) =>
          expected shouldEqual result
        }
      }
    }

    "FuncHash.sha1" should {

      "correctly return hash" in {
        val initial = List(
          ("abc", "a9993e364706816aba3e25717850c26c9cd0d89d"),
          ("\"abc\"^^xsd:string", "a9993e364706816aba3e25717850c26c9cd0d89d")
        ).toDF("input", "expected")

        val df = initial.withColumn("result", FuncHash.sha1(initial("input")))

        df.collect.foreach { case Row(_, expected, result) =>
          expected shouldEqual result
        }
      }
    }

    "FuncHash.sha256" should {

      "correctly return hash" in {
        val initial = List(
          (
            "abc",
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
          ),
          (
            "\"abc\"^^xsd:string",
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
          )
        ).toDF("input", "expected")

        val df =
          initial.withColumn("result", FuncHash.sha256(initial("input")))

        df.collect.foreach { case Row(_, expected, result) =>
          expected shouldEqual result
        }
      }
    }

    "FuncHash.sha384" should {

      "correctly return hash" in {
        val initial = List(
          (
            "abc",
            "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7"
          ),
          (
            "\"abc\"^^xsd:string",
            "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7"
          )
        ).toDF("input", "expected")

        val df =
          initial.withColumn("result", FuncHash.sha384(initial("input")))

        df.collect.foreach { case Row(_, expected, result) =>
          expected shouldEqual result
        }
      }
    }

    "FuncHash.sha512" should {

      "correctly return hash" in {
        val initial = List(
          (
            "abc",
            "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
          ),
          (
            "\"abc\"^^xsd:string",
            "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
          )
        ).toDF("input", "expected")

        val df =
          initial.withColumn("result", FuncHash.sha512(initial("input")))

        df.collect.foreach { case Row(_, expected, result) =>
          expected shouldEqual result
        }
      }
    }
  }
}
