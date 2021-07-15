package com.gsk.kg.engine.rdf

import org.apache.spark.sql.DataFrame

import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.engine.compiler.readNTtoDF
import com.gsk.kg.engine.rdf.RdfValue._

import frameless._
import frameless.syntax._
import org.scalatest.wordspec.AnyWordSpec
import com.gsk.kg.engine.functions.FuncStrings

class RdfValuesSpec extends AnyWordSpec with SparkSpec {

  "parser" should {

    "convert Rdf values to their String representation" in {
      implicit val sparkSession = spark

      val rdfValues: List[RdfValue] =
        List(
          RdfValue.RdfBoolean(false),
          RdfValue.RdfDecimal(BigDecimal(4.3))
        )

      TypedDataset.create(rdfValues).show().run()
    }

    "example" in {
      val dataFrame: DataFrame =
        readNTtoDF("fixtures/reference-q1-input.nt")

      val typed: TypedDataset[RdfTriple] = dataFrame.unsafeTyped[RdfTriple]

      spark.time(
        typed.deserialized
          .map(triple => Func.strAfter(triple.s, "#"))
          .collect
          .run()
          .foreach(println)
      )

      spark.time(
        dataFrame
          .select(FuncStrings.strafter(dataFrame("s"), "#"))
          .collect
          .foreach(println)
      )
    }
  }

}

object Func {

  def strAfter(
      rdf: RdfValue,
      sequence: String
  ): RdfValue =
    rdf match {
      case s @ RdfString(value, tag) =>
        val i = value.lastIndexOf(sequence)
        if (i != -1)
          s.copy(value = value.substring(i))
        else
          RdfString("", None)
      case RdfUri(value) =>
        val i = value.lastIndexOf(sequence)
        if (i != -1)
          RdfString(value.substring(i), None)
        else
          RdfString("", None)
      case otherwise => otherwise
    }

}
