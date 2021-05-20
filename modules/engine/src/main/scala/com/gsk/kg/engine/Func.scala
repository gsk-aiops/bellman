package com.gsk.kg.engine

import cats.data.NonEmptyList

import java.nio.charset.StandardCharsets
import java.util.regex.Pattern
import org.apache.commons.codec.binary.Hex
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{concat => cc, _}
import org.apache.spark.sql.types.StringType

import com.gsk.kg.engine.Func.StringFunctionUtils._

object Func {

  /** Performs logical binary operation '==' over two columns
    * @param l
    * @param r
    * @return
    */
  def equals(l: Column, r: Column): Column =
    applyOperator(l, r)(_ === _)

  /** Peforms logical binary operation '>' over two columns
    * @param l
    * @param r
    * @return
    */
  def gt(l: Column, r: Column): Column =
    applyOperator(l, r)(_ > _)

  /** Performs logical binary operation '<' over two columns
    * @param l
    * @param r
    * @return
    */
  def lt(l: Column, r: Column): Column =
    applyOperator(l, r)(_ < _)

  /** Performs logical binary operation '<=' over two columns
    * @param l
    * @param r
    * @return
    */
  def gte(l: Column, r: Column): Column =
    applyOperator(l, r)(_ >= _)

  /** Performs logical binary operation '>=' over two columns
    * @param l
    * @param r
    * @return
    */
  def lte(l: Column, r: Column): Column =
    applyOperator(l, r)(_ <= _)

  /** Performs logical binary operation 'or' over two columns
    * @param l
    * @param r
    * @return
    */
  def or(l: Column, r: Column): Column =
    l || r

  /** Performs logical binary operation 'and' over two columns
    * @param r
    * @param l
    * @return
    */
  def and(l: Column, r: Column): Column =
    l && r

  /** Negates all rows of a column
    * @param s
    * @return
    */
  def negate(s: Column): Column =
    not(s)

  /** Returns the string representation of a column.  It only modifies the data in the column if
    * it contains an URI wrapped in angle brackets, in which case it removes it.
    * @param col
    * @return
    */
  def str(col: Column): Column =
    when(col.startsWith("<") && col.endsWith(">"), rtrim(ltrim(col, "<"), ">"))
      .otherwise(col)

  /** Returns the string representation of a column.  It only modifies the data in the column if
    * it contains an URI wrapped in angle brackets, in which case it removes it.
    * @param col
    * @return
    */
  def str(value: String): Column =
    str(lit(value))

  /** Implementation of SparQL ENCODE_FOR_URI on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-encode]]
    * @param str
    * @return
    */
  def encodeForURI(str: String): Column =
    lit(encodeUri(extractStringLiteral(str)))

  /** Implementation of SparQL ENCODE_FOR_URI on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-encode]]
    * @param col
    * @return
    */
  def encodeForURI(col: Column): Column = {
    val efu = udf((str: String) => encodeUri(str))
    efu(extractStringLiteral(col))
  }

  private def encodeUri(str: String): String =
    str.map {
      case c if Pattern.matches("[A-Za-z0-9~._-]", c.toString) => c.toString
      case c =>
        val hex =
          Hex.encodeHexString(c.toString.getBytes(StandardCharsets.UTF_8))
        if (hex.length > 2) {
          hex.grouped(hex.length / 2).map("%" + _.toUpperCase).mkString
        } else {
          "%" + hex.toUpperCase
        }
    }.mkString

  /** Returns a column with 'true' or 'false' rows indicating whether a column has blank nodes
    * @param col
    * @return
    */
  def isBlank(col: Column): Column =
    when(regexp_extract(col, "^_:.*$", 0) =!= "", true)
      .otherwise(false)

  /** Implementation of SparQL REGEX on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-regex]]
    * @param col
    * @param pattern
    * @param flags
    * @return
    */
  def regex(col: Column, pattern: String, flags: String): Column =
    col.rlike(s"(?$flags)$pattern")

  /** Implementation of SparQL REPLACE on Spark dataframes.
    *
    * =Examples=
    *
    * | Function call                              | Result                     |
    * |:-------------------------------------------|:---------------------------|
    * | replace("abracadabra", "bra", "*")         | "a*cada*"                  |
    * | replace("abracadabra", "a.*a", "*")        | "*"                        |
    * | replace("abracadabra", "a.*?a", "*")       | "*c*bra"                   |
    * | replace("abracadabra", "a", "")            | "brcdbr"                   |
    * | replace("abracadabra", "a(.)", "a$1$1")    | "abbraccaddabbra"          |
    * | replace("abracadabra", ".*?", "$1")        | error (zero length string) |
    * | replace("AAAA", "A+", "b")                 | "b"                        |
    * | replace("AAAA", "A+?", "b")                | "bbbb"                     |
    * | replace("darted", "^(.*?)d(.*)$", "$1c$2") | "carted"                   |
    *
    * @see https://www.w3.org/TR/sparql11-query/#func-replace
    * @see https://www.w3.org/TR/xpath-functions/#func-replace
    * @param col
    * @param pattern
    * @param by
    * @param flags
    * @return
    */
  def replace(col: Column, pattern: String, by: String, flags: String): Column =
    regexp_replace(col, s"(?$flags)$pattern", by)

  /** Implementation of SparQL STRAFTER on Spark dataframes.
    *
    * =Examples=
    *
    * | Function call                  | Result            |
    * |:-------------------------------|:------------------|
    * | strafter("abc","b")            | "c"               |
    * | strafter("abc"@en,"ab")        | "c"@en            |
    * | strafter("abc"@en,"b"@cy)      | error             |
    * | strafter("abc"^^xsd:string,"") | "abc"^^xsd:string |
    * | strafter("abc","xyz")          | ""                |
    * | strafter("abc"@en, "z"@en)     | ""                |
    * | strafter("abc"@en, "z")        | ""                |
    * | strafter("abc"@en, ""@en)      | "abc"@en          |
    * | strafter("abc"@en, "")         | "abc"@en          |
    *
    * TODO (pepegar): Implement argument compatibility checks
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-strafter]]
    * @param col
    * @param str
    * @return
    */
  def strafter(col: Column, str: String): Column = {

    def getLeftOrEmpty(c: Column, s: String): Column =
      when(substring_index(c, s, -1) === c, lit(""))
        .otherwise(substring_index(c, s, -1))

    if (isEmptyPattern(str)) {
      col
    } else {
      when(
        isLocalizedLocalizedArgs(col, str),
        strFuncArgsLocalizedLocalized(col, str, "\"%s@")(getLeftOrEmpty)
      ).when(
        isLocalizedPlainArgs(col),
        strFuncArgsLocalizedPlain(col, str, "\"%s@")(getLeftOrEmpty)
      ).when(
        isTypedTypedArgs(col, str),
        strFuncArgsTypedTyped(col, str, "\"%s^^")(getLeftOrEmpty)
      ).when(
        isTypedPlainArgs(col),
        strFuncArgsTypedPlain(col, str, "\"%s^^")(getLeftOrEmpty)
      ).otherwise(getLeftOrEmpty(col, str))
    }
  }

  /** Implementation of SparQL STRBEFORE on Spark dataframes.
    *
    * TODO (pepegar): Implement argument compatibility checks
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-strbefore]]
    * @param col
    * @param str
    * @return
    */
  def strbefore(col: Column, str: String): Column = {
    def getLeftOrEmpty(c: Column, s: String): Column =
      when(substring_index(c, s, 1) === c, lit(""))
        .otherwise(substring_index(c, s, 1))

    if (isEmptyPattern(str)) {
      cc(lit("\"\""), substring_index(col, "\"", -1))
    } else {
      when(
        isLocalizedLocalizedArgs(col, str),
        strFuncArgsLocalizedLocalized(col, str, "%s\"@")(getLeftOrEmpty)
      ).when(
        isLocalizedPlainArgs(col),
        strFuncArgsLocalizedPlain(col, str, "%s\"@")(getLeftOrEmpty)
      ).when(
        isTypedTypedArgs(col, str),
        strFuncArgsTypedTyped(col, str, "%s\"^^")(getLeftOrEmpty)
      ).when(
        isTypedPlainArgs(col),
        strFuncArgsTypedPlain(col, str, "%s\"^^")(getLeftOrEmpty)
      ).otherwise(getLeftOrEmpty(col, str))
    }
  }

  /** Implementation of SparQL SUBSTR on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-substr]]
    * @param col
    * @param pos
    * @param len
    * @return
    */
  def substr(col: Column, pos: Int, len: Option[Int]): Column = {
    def ss(col: Column, pos: Int, len: Option[Int]) = {
      len match {
        case Some(l) => col.substr(pos, l)
        case None    => col.substr(lit(pos), length(col) - pos + 1)
      }
    }

    when(
      col.contains("\"@"),
      format_string(
        "%s",
        cc(
          cc(
            cc(
              lit("\""),
              ss(trim(substring_index(col, "\"@", 1), "\""), pos, len)
            ),
            lit("\"")
          ),
          cc(lit("@"), substring_index(col, "\"@", -1))
        )
      )
    ).when(
      col.contains("\"^^"),
      format_string(
        "%s",
        cc(
          cc(
            cc(
              lit("\""),
              ss(trim(substring_index(col, "\"^^", 1), "\""), pos, len)
            ),
            lit("\"")
          ),
          cc(lit("^^"), substring_index(col, "\"^^", -1))
        )
      )
    ).otherwise(ss(trim(col, "\""), pos, len))
  }

  /** Implementation of SparQL STRENDS on Spark dataframes.
    *
    * TODO (pepegar): Implement argument compatibility checks
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-strends]]
    * @param col
    * @param str
    * @return
    */
  def strends(col: Column, str: String): Column =
    extractStringLiteral(col).endsWith(extractStringLiteral(str))

  /** Implementation of SparQL STRSTARTS on Spark dataframes.
    *
    * TODO (pepegar): Implement argument compatibility checks
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-strstarts]]
    * @param col
    * @param str
    * @return
    */
  def strstarts(col: Column, str: String): Column =
    extractStringLiteral(col).startsWith(extractStringLiteral(str))

  private def extractStringLiteral(col: Column): Column =
    when(
      col.startsWith("\"") && col.contains("\"@"),
      trim(substring_index(col, "\"@", 1), "\"")
    )
      .when(
        col.startsWith("\"") && col.contains("\"^^"),
        trim(substring_index(col, "\"^^", 1), "\"")
      )
      .otherwise(trim(col, "\""))

  private def extractStringLiteral(str: String): String =
    str match {
      case s if str.contains("\"@") || str.contains("\"^^") =>
        s.stripPrefix("\"").split("\"").head
      case s => s.stripPrefix("\"").stripSuffix("\"")
    }

  /** Implementation of SparQL STRDT on Spark dataframes.
    * The STRDT function constructs a literal with lexical form and type as specified by the arguments.
    *
    * Examples:
    * STRDT("123", xsd:integer) -> "123"^^<http://www.w3.org/2001/XMLSchema#integer>
    * STRDT("iiii", <http://example/romanNumeral>) -> "iiii"^^<http://example/romanNumeral>
    *
    * @param col
    * @param uri
    * @return
    */
  def strdt(col: Column, uri: String): Column =
    cc(lit("\""), col, lit("\""), lit(s"^^$uri"))

  /** Implementation of SparQL STRLEN on Spark dataframes.
    * Counts string number of characters
    *
    * strlen("chat") -> 4
    * strlen("chat"@en) -> 4
    * strlen("chat"^^xsd:string) -> 4
    *
    * @param col
    * @return
    */
  def strlen(col: Column): Column = {
    when(
      RdfFormatter.isLocalizedString(col), {
        val l = LocalizedString(col)
        length(regexp_replace(l.value, "\"", ""))
      }
    ).when(
      RdfFormatter.isDatatypeLiteral(col), {
        val t = TypedString(col)
        length(regexp_replace(t.value, "\"", ""))
      }
    ).otherwise(length(col))
  }

  /** The IRI function constructs an IRI by resolving the string
    * argument (see RFC 3986 and RFC 3987 or any later RFC that
    * superceeds RFC 3986 or RFC 3987). The IRI is resolved against
    * the base IRI of the query and must result in an absolute IRI.
    *
    * The URI function is a synonym for IRI.
    *
    * If the function is passed an IRI, it returns the IRI unchanged.
    *
    * Passing any RDF term other than a simple literal, xsd:string or
    * an IRI is an error.
    *
    * An implementation MAY normalize the IRI.
    *
    * =Examples=
    *
    * | Function call          | Result            |
    * |:-----------------------|:------------------|
    * | IRI("http://example/") | <http://example/> |
    * | IRI(<http://example/>) | <http://example/> |
    *
    * @param col
    * @return
    */
  def iri(col: Column): Column =
    when(
      col.startsWith("<") && col.endsWith(">"),
      col
    ).otherwise(format_string("<%s>", col))

  /** synonym for [[Func.iri]]
    *
    * @param col
    * @return
    */
  def uri(col: Column): Column = iri(col)

  /** Implementation of SparQL ISNUMERIC on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-isNumeric]]
    * @param col
    * @return
    */
  def isNumeric(col: Column): Column =
    /*
      This is a bit of a trick here...any valid numeric type will return true,
      even though the cast is to int
     */
    when(col.cast("int").isNotNull, lit(true))
      .when(
        extractType(col).isInCollection(
          Set(
            "xsd:int",
            "xsd:integer",
            "xsd:decimal",
            "xsd:float",
            "xsd:double",
            "xsd:nonPositiveInteger",
            "xsd:negativeInteger",
            "xsd:long",
            "xsd:short",
            "xsd:nonNegativeInteger",
            "xsd:unsignedLong",
            "xsd:unsignedInt",
            "xsd:unsignedShort",
            "xsd:positiveInteger",
            "<http://www.w3.org/2001/XMLSchema#int>",
            "<http://www.w3.org/2001/XMLSchema#integer>",
            "<http://www.w3.org/2001/XMLSchema#float>",
            "<http://www.w3.org/2001/XMLSchema#decimal>",
            "<http://www.w3.org/2001/XMLSchema#double>",
            "<http://www.w3.org/2001/XMLSchema#nonPositiveInteger>",
            "<http://www.w3.org/2001/XMLSchema#negativeInteger>",
            "<http://www.w3.org/2001/XMLSchema#long>",
            "<http://www.w3.org/2001/XMLSchema#short>",
            "<http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
            "<http://www.w3.org/2001/XMLSchema#unsignedLong>",
            "<http://www.w3.org/2001/XMLSchema#unsignedInt>",
            "<http://www.w3.org/2001/XMLSchema#unsignedShort>",
            "<http://www.w3.org/2001/XMLSchema#positiveInteger>"
          )
        ),
        lit(true)
      )
      .otherwise(lit(false))

  /** Concatenate two [[Column]] into a new one
    *
    * @param a
    * @param b
    * @return
    */
  def concat(appendTo: Column, append: NonEmptyList[Column]): Column = {
    val (lvalue, ltag) = unfold(appendTo)
    val concatValues = append.toList.foldLeft(lvalue) { case (acc, elem) =>
      val (rvalue, _) = unfold(elem)
      cc(acc, rvalue)
    }

    when(
      areAllArgsSameTypeAndSameTags(appendTo, append.toList),
      when(
        RdfFormatter.isLocalizedString(appendTo),
        format_string("\"%s\"@%s", concatValues, ltag)
      ).otherwise(
        format_string("\"%s\"^^%s", concatValues, ltag)
      )
    ).otherwise(
      concatValues
    )
  }

  /** Sample is a set function which returns an arbitrary value from
    * the multiset passed to it.
    *
    * Implemented using [[org.apache.spark.sql.functions.first]].
    *
    * @param col
    * @return
    */
  def sample(col: Column): Column =
    first(col, true)

  /** Implementation of SparQL LCASE on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-lcase]]
    * @param col
    * @return
    */
  def lcase(col: Column): Column =
    applyRdfFormat(col)(lower)

  /** Implementation of SparQL UCASE on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-ucase]]
    * @param col
    * @return
    */
  def ucase(col: Column): Column =
    applyRdfFormat(col)(upper)

  /** Implementation of SparQL ISLITERAL on Spark dataframes.
    *
    * @see [[https://www.w3.org/TR/sparql11-query/#func-isLiteral]]
    * @param col
    * @return
    */
  def isLiteral(col: Column): Column =
    when(
      col.startsWith("\"") && col.contains("\"@"),
      lit(true)
    ).when(
      col.startsWith("\"") && col.contains("\"^^"),
      lit(true)
    ).when(
      col.startsWith("\"") && col.endsWith("\""),
      lit(true)
    ).otherwise(lit(false))

  private def formatRdfString(col: Column, sep: String)(
      f: Column => Column
  ): Column = {
    format_string(
      "%s",
      cc(
        cc(
          cc(
            lit("\""),
            f(trim(substring_index(col, "\"" + sep, 1), "\""))
          ),
          lit("\"")
        ),
        cc(lit(sep), substring_index(col, "\"" + sep, -1))
      )
    )
  }

  private def applyRdfFormat(col: Column)(f: Column => Column): Column = {
    when(
      col.contains("\"@"),
      formatRdfString(col, "@")(f)
    ).when(
      col.contains("\"^^"),
      formatRdfString(col, "^^")(f)
    ).otherwise(f(trim(col, "\"")))
  }

  def groupConcat(col: Column, separator: String): Column =
    ???

  def isTypedLiteral(col: Column): Column =
    col.startsWith("\"") && col.contains("\"^^")

  // scalastyle:off
  def extractType(col: Column): Column =
    when(
      isTypedLiteral(col), {
        val del = "\"^^"
        when(substring_index(col, del, -1) === del, lit(""))
          .otherwise(substring_index(col, del, -1))
      }
    ).otherwise(lit(null))

  def extractNumber(col: Column): Column =
    extractNumberImpl(col, lit(null))
  // scalastyle:on

  def tryExtractNumber(col: Column): Column =
    extractNumberImpl(col, col)

  private def extractNumberImpl(col: Column, default: Column) =
    when(
      isTypedLiteral(col) && isNumeric(col),
      strbefore(ltrim(col, "\""), "\"")
    ).otherwise(default)

  /** This helper method tries to parse a datetime expressed as a RDF
    * datetime string `"0193-07-03T20:50:09.000+04:00"^^xsd:dateTime`
    * to a column with underlying type datetime.
    *
    * @param col
    * @return
    */
  def parseDateFromRDFDateTime(col: Column): Column =
    when(
      regexp_extract(col, ExtractDateTime, 1) =!= lit(""),
      to_timestamp(regexp_extract(col, ExtractDateTime, 1))
    ).otherwise(lit(null)) // scalastyle:off

  private def applyOperator(l: Column, r: Column)(
      operator: (Column, Column) => Column
  ): Column =
    when(
      regexp_extract(l.cast(StringType), ExtractDateTime, 1) =!= lit("") &&
        regexp_extract(r.cast(StringType), ExtractDateTime, 1) =!= lit(""),
      operator(
        parseDateFromRDFDateTime(l.cast(StringType)),
        parseDateFromRDFDateTime(r.cast(StringType))
      )
    ).otherwise(operator(l, r))

  val ExtractDateTime = """^"(.*)"\^\^(.*)dateTime(.*)$"""

  private def isEmptyPattern(pattern: String): Boolean = {
    if (pattern.isEmpty) {
      true
    } else if (pattern.contains("@")) {
      val left = pattern.split("@").head.replace("\"", "")
      if (left.isEmpty) {
        true
      } else {
        false
      }
    } else if (pattern.contains("^^")) {
      val left = pattern.split("\\^\\^").head.replace("\"", "")
      if (left.isEmpty) {
        true
      } else {
        false
      }
    } else {
      false
    }
  }

  final case class LocalizedString(value: Column, tag: Column)
  object LocalizedString {
    def apply(c: Column): LocalizedString = {
      new LocalizedString(
        substring_index(c, "@", 1),
        substring_index(c, "@", -1)
      )
    }

    def apply(s: String): LocalizedString = {
      val split = s.split("@").toSeq
      new LocalizedString(
        lit(split.head.replace("\"", "")),
        lit(split.last)
      )
    }

    def formatLocalized(l: LocalizedString, s: String, localizedFormat: String)(
        f: (Column, String) => Column
    ): Column =
      when(
        f(l.value, s) === lit(""),
        f(l.value, s)
      ).otherwise(
        cc(
          format_string(localizedFormat, f(l.value, s)),
          l.tag
        )
      )
  }

  final case class TypedString(value: Column, tag: Column)
  object TypedString {
    def apply(c: Column): TypedString = {
      new TypedString(
        substring_index(c, "^^", 1),
        substring_index(c, "^^", -1)
      )
    }

    def apply(s: String): TypedString = {
      val split = s.split("\\^\\^")
      new TypedString(
        lit(split.head.replace("\"", "")),
        lit(split.last)
      )
    }

    def formatTyped(t: TypedString, s: String, typedFormat: String)(
        f: (Column, String) => Column
    ): Column = when(
      f(t.value, s) === lit(""),
      f(t.value, s)
    ).otherwise(
      cc(
        format_string(typedFormat, f(t.value, s)),
        t.tag
      )
    )
  }

  object StringFunctionUtils {

    def unfold(arg: Column): (Column, Column) = {
      val getValue = when(
        RdfFormatter.isLocalizedString(arg), {
          val l = LocalizedString(arg)
          trim(l.value, "\"")
        }
      ).when(
        RdfFormatter.isDatatypeLiteral(arg), {
          val l = TypedString(arg)
          trim(l.value, "\"")
        }
      ).otherwise(
        trim(arg, "\"")
      )

      val getTag = when(
        RdfFormatter.isLocalizedString(arg), {
          val l = LocalizedString(arg)
          l.tag
        }
      ).when(
        RdfFormatter.isDatatypeLiteral(arg), {
          val l = TypedString(arg)
          l.tag
        }
      ).otherwise(
        lit("")
      )

      (getValue, getTag)
    }

    def areAllArgsSameTypeAndSameTags(
        arg1: Column,
        args: List[Column]
    ): Column = {
      when(
        RdfFormatter.isLocalizedString(arg1), {
          val l = LocalizedString(arg1)
          args.foldLeft(lit(true)) { case (acc, elem) =>
            acc && when(
              RdfFormatter.isLocalizedString(elem), {
                val r = LocalizedString(elem)
                l.tag === r.tag
              }
            ).otherwise(lit(false))
          }
        }
      ).when(
        RdfFormatter.isDatatypeLiteral(arg1), {
          val l = TypedString(arg1)
          args.foldLeft(lit(true)) { case (acc, elem) =>
            acc && when(
              RdfFormatter.isDatatypeLiteral(elem), {
                val r = TypedString(elem)
                l.tag === r.tag
              }
            ).otherwise(lit(false))
          }
        }
      ).otherwise(lit(false))
    }

    def isLocalizedLocalizedArgs(arg1: Column, arg2: String): Column =
      RdfFormatter.isLocalizedString(arg1) && RdfFormatter.isLocalizedString(
        lit(arg2)
      )

    def isTypedTypedArgs(arg1: Column, arg2: String): Column =
      RdfFormatter.isDatatypeLiteral(arg1) && RdfFormatter.isDatatypeLiteral(
        lit(arg2)
      )

    def isTypedPlainArgs(arg1: Column): Column =
      RdfFormatter.isDatatypeLiteral(arg1)

    def isLocalizedPlainArgs(arg1: Column): Column =
      RdfFormatter.isLocalizedString(arg1)

    // scalastyle:off
    def strFuncArgsLocalizedLocalized(
        col: Column,
        str: String,
        localizedFormat: String
    )(
        f: (Column, String) => Column
    ): Column = {
      val left  = LocalizedString(col)
      val right = LocalizedString(str)
      when(
        left.tag =!= right.tag,
        lit(null)
      ).otherwise(
        LocalizedString.formatLocalized(left, str, localizedFormat)(f)
      )
    }
    // scalastyle:on

    def strFuncArgsLocalizedPlain(
        col: Column,
        str: String,
        localizedFormat: String
    )(
        f: (Column, String) => Column
    ): Column = {
      val left = LocalizedString(col)
      LocalizedString.formatLocalized(left, str, localizedFormat)(f)
    }

    // scalastyle:off
    def strFuncArgsTypedTyped(col: Column, str: String, typedFormat: String)(
        f: (Column, String) => Column
    ) = {
      val left  = TypedString(col)
      val right = TypedString(str)
      when(
        left.tag =!= right.tag,
        lit(null)
      ).otherwise(
        TypedString.formatTyped(left, str, typedFormat)(f)
      )
    }
    // scalastyle:off

    def strFuncArgsTypedPlain(col: Column, str: String, typedFormat: String)(
        f: (Column, String) => Column
    ): Column = {
      val left = TypedString(col)
      TypedString.formatTyped(left, str, typedFormat)(f)
    }
  }
}
