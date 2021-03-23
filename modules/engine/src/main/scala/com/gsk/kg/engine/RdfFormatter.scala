package com.gsk.kg.engine

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import java.net.URI

import scala.annotation.tailrec
import scala.util.Success
import scala.util.Try

object RdfFormatter {

  /** This function reformats a dataframe as per RDF standards.  In
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
    Option(field)
      .map(_.toString match {
        case RDFUri(uri)             => uri
        case RDFBlank(blank)         => blank
        case RDFNum(num)             => num
        case RDFBoolean(bool)        => bool
        case RDFDataTypeLiteral(lit) => lit
        case RDFLocalizedString(str) => str
        case RDFString(str)          => str
      })
      .getOrElse(null) // scalastyle:off

  object RDFString {
    def unapply(str: String): Option[String] = {

      @tailrec
      def removeExtraDoubleQuotes(str: String): String = {
        if (str.startsWith("\"") && str.endsWith("\"")) {
          removeExtraDoubleQuotes(str.replace("\"", ""))
        } else {
          str
        }
      }

      Some(s""""${removeExtraDoubleQuotes(str)}"""")
    }
  }

  object RDFLocalizedString {
    def unapply(str: String): Option[String] =
      if (str.startsWith("\"") && str.contains("\"@") && !str.endsWith("\"")) {
        Some(str)
      } else {
        None
      }
  }

  object RDFDataTypeLiteral {
    def unapply(str: String): Option[String] =
      if (str.contains("^^")) {
        Some(str.replace("\".", ""))
      } else {
        None
      }
  }

  object RDFUri {
    def unapply(str: String): Option[String] =
      if (str.startsWith("<") && str.endsWith(">")) {
        Some(str)
      } else if (Try(new URI(str).isAbsolute) == Success(true)) {
        Some(str)
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
    def unapply(str: String): Option[Any] =
      Try(Integer.parseInt(str)).recoverWith { case _ =>
        Try(java.lang.Float.parseFloat(str))
      }.toOption
  }

  object RDFBoolean {
    def unapply(str: String): Option[Boolean] =
      str match {
        case "true"  => Some(true)
        case "false" => Some(false)
        case _       => None
      }
  }

}
