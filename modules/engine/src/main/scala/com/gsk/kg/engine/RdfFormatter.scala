package com.gsk.kg.engine

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Row
import java.net.URI
import scala.util.Try
import scala.util.Success

object RdfFormatter {

  /**
    * This function reformats a dataframe as per RDF standards.  In
    * the [[formatField]] helper function we apply some heuristics to
    * identify the kind of RDF node we should format to.
    *
    * @param df
    * @return
    */
  def formatDataFrame(df: DataFrame): DataFrame = {
    implicit val encoder: Encoder[Row] = RowEncoder(df.schema)

    df.map { row =>
      Row.fromSeq(row.toSeq.map(formatField))
    }
  }

  def formatField(field: Any): Any =
    Option(field).map(_.toString match {
	    case RDFUri(uri) => uri
      case RDFBlank(blank) => blank
      case RDFNum(num) => num
      case str => s""""$str""""
    }).getOrElse("null")

  object RDFUri {
    def unapply(str: String): Option[String] =
      if (str.startsWith("<") && str.endsWith(">")) {
        Some(str)
      } else if(Try(new URI(str).isAbsolute) == Success(true)) {
        Some(s"<$str>")
      } else {
        None
      }
  }

  object RDFBlank {
    def unapply(str: String): Option[String] =
      if (str.startsWith("_:"))
        Some(str)
      else
        None
  }

  object RDFNum {
    def unapply(str: String): Option[String] =
      Try(Integer.parseInt(str))
        .recoverWith { case _ => Try(java.lang.Float.parseFloat(str)) }
        .map(_.toString)
        .toOption
  }

}
