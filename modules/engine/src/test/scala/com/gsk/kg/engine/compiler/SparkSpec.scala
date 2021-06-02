package com.gsk.kg.engine.compiler

import org.apache.spark.SparkConf

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.Suite

trait SparkSpec extends DataFrameSuiteBase { self: Suite =>

  override def appID: String = (this.getClass.getName
    + math.floor(math.random * 10e4).toLong.toString)

  override def conf: SparkConf = {
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("test")
      .set("spark.ui.enabled", "false")
      .set("spark.app.id", appID)
      .set("spark.driver.host", "localhost")
      .set("spark.sql.codegen.wholeStage", "false")
  }

}
