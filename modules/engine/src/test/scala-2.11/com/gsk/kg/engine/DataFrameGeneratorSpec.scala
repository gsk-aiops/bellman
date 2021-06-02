package com.gsk.kg.engine

import org.apache.spark.sql.DataFrame

import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.engine.scalacheck.DataFrameArbitraries

import java.io.File

import scala.reflect.io.Directory

import net.sansa_stack.rdf.spark.io._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class DataFrameArbitrariesSpec
    extends AnyFlatSpec
    with Matchers
    with SparkSpec
    with ScalaCheckDrivenPropertyChecks
    with DataFrameArbitraries {

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  /*
   * TODO: un-ignore test
   *
   * currently we're not able to make this work because sansa-rdf
   * expects URIs to be wrapped by angle bracket for writing files,
   * but unwraps them when reading...
   *
   * Once we do #103 we'll be able to test this out without problem!
   */
  "DataFrameArbitrariesSpec" should "work" ignore {
    forAll { df: DataFrame =>
      val path = "/tmp/data_frame_generated_by_sansa"
      try {
        df.write.ntriples(path)

        val reloaded: DataFrame = spark.read.ntriples(path)

        df.collect shouldEqual reloaded.collect
      } finally {
        val directory = new Directory(new File(path))
        directory.deleteRecursively()
      }
    }
  }

}
