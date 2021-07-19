package com.gsk.kg.engine.properties

import cats.syntax.either._

import org.apache.spark.sql.Row

import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.engine.scalacheck.CommonGenerators

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class FuncPropertySpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with ScalaCheckDrivenPropertyChecks
    with CommonGenerators {

  import sqlContext.implicits._

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  "Funcs on Properties" when {

    "Uri function" should {

      "return expected column" in {

        val df = List(
          (
            "<http://example.org/Alice>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Bob>"
          ),
          (
            "<http://example.org/Bob>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Charles>"
          ),
          (
            "<http://example.org/Charles>",
            "<http://xmlns.org/foaf/0.1/name>",
            "\"Charles\""
          )
        ).toDF("s", "p", "o")

        val uriFunc = FuncProperty.uri("<http://xmlns.org/foaf/0.1/knows>")
        uriFunc shouldBe a[Left[_, _]]

        val result = df.select(uriFunc.left.get)
        result.collect().toSet shouldEqual Set(
          Row("<http://xmlns.org/foaf/0.1/knows>"),
          Row("<http://xmlns.org/foaf/0.1/knows>")
        )
      }
    }

    "Alternative function" should {

      "return expected column" in {

        val df = List(
          (
            "<http://example.org/book1>",
            "<http://purl.org/dc/elements/1.1/title>",
            "SPARQL Tutorial"
          ),
          (
            "<http://example.org/book2>",
            "<http://www.w3.org/2000/01/rdf-schema#label>",
            "From Earth To The Moon"
          ),
          (
            "<http://example.org/book3>",
            "<http://www.w3.org/2000/01/rdf-schema#label2>",
            "Another title"
          )
        ).toDF("s", "p", "o")

        lazy val titleUriFunc =
          FuncProperty.uri("<http://purl.org/dc/elements/1.1/title>")
        lazy val labelUriFunc =
          FuncProperty.uri("<http://www.w3.org/2000/01/rdf-schema#label>")
        val alternativeFunc =
          FuncProperty.alternative(df, titleUriFunc, labelUriFunc)

        alternativeFunc.right.get shouldBe a[Left[_, _]]

        val result = df.select(alternativeFunc.right.get.left.get).collect()
        result.toSet shouldEqual Set(Row(true), Row(true), Row(false))
      }
    }

    "Seq function" should {

      "return expected dataframe" in {

        val df = List(
          (
            "<http://example.org/Alice>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Bob>"
          ),
          (
            "<http://example.org/Bob>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Charles>"
          ),
          (
            "<http://example.org/Charles>",
            "<http://xmlns.org/foaf/0.1/name>",
            "\"Charles\""
          )
        ).toDF("s", "p", "o")

        lazy val knowsUriFunc =
          FuncProperty.uri("<http://xmlns.org/foaf/0.1/knows>")
        lazy val nameUriFunc =
          FuncProperty.uri("<http://xmlns.org/foaf/0.1/name>")

        val result = for {
          // (seq <http://xmlns.org/foaf/0.1/knows> <http://xmlns.org/foaf/0.1/knows>)
          innerSeq <- FuncProperty.seq(
            df,
            knowsUriFunc,
            knowsUriFunc
          )
          // (seq (seq <http://xmlns.org/foaf/0.1/knows> <http://xmlns.org/foaf/0.1/knows>) <http://xmlns.org/foaf/0.1/name>)
          outerSeq <- FuncProperty.seq(
            df,
            innerSeq,
            nameUriFunc
          )
        } yield outerSeq

        result.right.get.right.get.collect().toSet shouldEqual Set(
          Row("<http://example.org/Alice>", "\"Charles\"")
        )
      }
    }

    "OneOrMore function" should {

      "return expected values" in {

        val df = List(
          (
            "<http://example.org/Alice>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Bob>"
          ),
          (
            "<http://example.org/Bob>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Charles>"
          ),
          (
            "<http://example.org/Charles>",
            "<http://xmlns.org/foaf/0.1/name>",
            "\"Charles\""
          )
        ).toDF("s", "p", "o")

        // ?s foaf:knows+ ?o
        lazy val knowsUriFunc =
          FuncProperty.uri("<http://xmlns.org/foaf/0.1/knows>")

        val result = FuncProperty.oneOrMore(df, knowsUriFunc)

        result.right.get.right.get.collect().toSet shouldEqual Set(
          Row(
            "<http://example.org/Alice>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Bob>"
          ),
          Row(
            "<http://example.org/Bob>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Charles>"
          ),
          Row(
            "<http://example.org/Alice>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Charles>"
          )
        )
      }
    }
  }

}
