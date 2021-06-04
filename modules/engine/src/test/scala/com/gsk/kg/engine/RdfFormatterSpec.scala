package com.gsk.kg.engine

import com.gsk.kg.config.Config
import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.engine.scalacheck.CommonGenerators

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class RdfFormatterSpec
    extends AnyFlatSpec
    with ScalaCheckDrivenPropertyChecks
    with Matchers
    with CommonGenerators
    with SparkSpec {

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  "RdfFormatter" should "format all fields from a dataframe" in {
    import sqlContext.implicits._

    val df = List(
      ("\"string\"@en", "_:blanknode", "http://uri.com"),
      ("\"string\"@en", "_:blanknode", "<http://uri.com>"),
      ("\"string\"@en", "_:blanknode", "<http://uri.com>"),
      ("false", "true", "another string"),
      ("un-quoted string", "\"quoted string\"", ""),
      ("\"false\"^^xsd:boolean", "\"true\"^^xsd:boolean", "1")
    ).toDF("s", "p", "o")

    val expected = List(
      ("\"string\"@en", "_:blanknode", "<http://uri.com>"),
      ("\"string\"@en", "_:blanknode", "<http://uri.com>"),
      ("\"string\"@en", "_:blanknode", "<http://uri.com>"),
      ("false", "true", "\"another string\""),
      ("\"un-quoted string\"", "\"quoted string\"", "\"\""),
      ("\"false\"^^xsd:boolean", "\"true\"^^xsd:boolean", "1")
    ).toDF("s", "p", "o")

    val result = RdfFormatter.formatDataFrame(df, Config(true, true, true))

    result.collect() shouldEqual expected.collect()
  }

}
