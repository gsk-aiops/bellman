package com.gsk.kg.engine

import cats.data.NonEmptyList
import cats.implicits._

import higherkindness.droste._
import higherkindness.droste.macros.deriveTraverse

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import com.gsk.kg.config.Config
import com.gsk.kg.engine.functions.FuncArithmetics
import com.gsk.kg.engine.functions.FuncForms
import com.gsk.kg.engine.functions.FuncHash
import com.gsk.kg.engine.functions.FuncNumerics
import com.gsk.kg.engine.functions.FuncStrings
import com.gsk.kg.engine.functions.FuncTerms
import com.gsk.kg.sparqlparser._

/** [[ExpressionF]] is a pattern functor for the recursive
  * [[Expression]].
  *
  * Using Droste's syntax, we get tree traversals for free such as the
  * ones seen in [[getVariable]] or [[getString]]
  */
@deriveTraverse sealed trait ExpressionF[+A]

object ExpressionF {

  final case class ADD[A](l: A, r: A)      extends ExpressionF[A]
  final case class SUBTRACT[A](l: A, r: A) extends ExpressionF[A]
  final case class MULTIPLY[A](l: A, r: A) extends ExpressionF[A]
  final case class DIVIDE[A](l: A, r: A)   extends ExpressionF[A]
  final case class REGEX[A](s: A, pattern: String, flags: String)
      extends ExpressionF[A]
  final case class REPLACE[A](st: A, pattern: String, by: String, flags: String)
      extends ExpressionF[A]
  final case class STRENDS[A](s: A, f: String)   extends ExpressionF[A]
  final case class STRSTARTS[A](s: A, f: String) extends ExpressionF[A]
  final case class STRDT[A](s: A, uri: String)   extends ExpressionF[A]
  final case class STRAFTER[A](s: A, f: String)  extends ExpressionF[A]
  final case class STRBEFORE[A](s: A, f: String) extends ExpressionF[A]
  final case class SUBSTR[A](s: A, pos: Int, len: Option[Int])
      extends ExpressionF[A]
  final case class STRLEN[A](s: A)                      extends ExpressionF[A]
  final case class EQUALS[A](l: A, r: A)                extends ExpressionF[A]
  final case class GT[A](l: A, r: A)                    extends ExpressionF[A]
  final case class LT[A](l: A, r: A)                    extends ExpressionF[A]
  final case class GTE[A](l: A, r: A)                   extends ExpressionF[A]
  final case class LTE[A](l: A, r: A)                   extends ExpressionF[A]
  final case class OR[A](l: A, r: A)                    extends ExpressionF[A]
  final case class AND[A](l: A, r: A)                   extends ExpressionF[A]
  final case class NEGATE[A](s: A)                      extends ExpressionF[A]
  final case class IN[A](e: A, xs: List[A])             extends ExpressionF[A]
  final case class SAMETERM[A](l: A, r: A)              extends ExpressionF[A]
  final case class IF[A](cnd: A, ifTrue: A, ifFalse: A) extends ExpressionF[A]
  final case class BOUND[A](e: A)                       extends ExpressionF[A]
  final case class COALESCE[A](xs: List[A])             extends ExpressionF[A]
  final case class URI[A](s: A)                         extends ExpressionF[A]
  final case class LANG[A](s: A)                        extends ExpressionF[A]
  final case class LANGMATCHES[A](s: A, range: String)  extends ExpressionF[A]
  final case class LCASE[A](s: A)                       extends ExpressionF[A]
  final case class UCASE[A](s: A)                       extends ExpressionF[A]
  final case class ISLITERAL[A](s: A)                   extends ExpressionF[A]
  final case class CONCAT[A](appendTo: A, append: NonEmptyList[A])
      extends ExpressionF[A]
  final case class STR[A](s: A)       extends ExpressionF[A]
  final case class ISBLANK[A](s: A)   extends ExpressionF[A]
  final case class ISNUMERIC[A](s: A) extends ExpressionF[A]
  final case class COUNT[A](e: A)     extends ExpressionF[A]
  final case class SUM[A](e: A)       extends ExpressionF[A]
  final case class MIN[A](e: A)       extends ExpressionF[A]
  final case class MAX[A](e: A)       extends ExpressionF[A]
  final case class AVG[A](e: A)       extends ExpressionF[A]
  final case class SAMPLE[A](e: A)    extends ExpressionF[A]
  final case class GROUP_CONCAT[A](e: A, separator: String)
      extends ExpressionF[A]
  final case class ENCODE_FOR_URI[A](s: A)                extends ExpressionF[A]
  final case class MD5[A](s: A)                           extends ExpressionF[A]
  final case class SHA1[A](s: A)                          extends ExpressionF[A]
  final case class SHA256[A](s: A)                        extends ExpressionF[A]
  final case class SHA384[A](s: A)                        extends ExpressionF[A]
  final case class SHA512[A](s: A)                        extends ExpressionF[A]
  final case class STRING[A](s: String)                   extends ExpressionF[A]
  final case class DT_STRING[A](s: String, tag: String)   extends ExpressionF[A]
  final case class LANG_STRING[A](s: String, tag: String) extends ExpressionF[A]
  final case class NUM[A](s: String)                      extends ExpressionF[A]
  final case class VARIABLE[A](s: String)                 extends ExpressionF[A]
  final case class URIVAL[A](s: String)                   extends ExpressionF[A]
  final case class BLANK[A](s: String)                    extends ExpressionF[A]
  final case class BOOL[A](s: String)                     extends ExpressionF[A]
  final case class ASC[A](e: A)                           extends ExpressionF[A]
  final case class DESC[A](e: A)                          extends ExpressionF[A]
  final case class UUID[A]()                              extends ExpressionF[A]
  final case class CEIL[A](s: A)                          extends ExpressionF[A]
  final case class ROUND[A](s: A)                         extends ExpressionF[A]
  final case class RAND[A]()                              extends ExpressionF[A]
  final case class ABS[A](s: A)                           extends ExpressionF[A]
  final case class FLOOR[A](s: A)                         extends ExpressionF[A]

  val fromExpressionCoalg: Coalgebra[ExpressionF, Expression] =
    Coalgebra {
      case Arithmetic.ADD(l, r)                 => ADD(l, r)
      case Arithmetic.SUBTRACT(l, r)            => SUBTRACT(l, r)
      case Arithmetic.MULTIPLY(l, r)            => MULTIPLY(l, r)
      case Arithmetic.DIVIDE(l, r)              => DIVIDE(l, r)
      case Conditional.EQUALS(l, r)             => EQUALS(l, r)
      case Conditional.GT(l, r)                 => GT(l, r)
      case Conditional.LT(l, r)                 => LT(l, r)
      case Conditional.GTE(l, r)                => GTE(l, r)
      case Conditional.LTE(l, r)                => LTE(l, r)
      case Conditional.OR(l, r)                 => OR(l, r)
      case Conditional.AND(l, r)                => AND(l, r)
      case Conditional.NEGATE(s)                => NEGATE(s)
      case Conditional.IN(e, xs)                => IN(e, xs)
      case Conditional.SAMETERM(l, r)           => SAMETERM(l, r)
      case Conditional.IF(cnd, ifTrue, ifFalse) => IF(cnd, ifTrue, ifFalse)
      case Conditional.BOUND(e)                 => BOUND(e)
      case Conditional.COALESCE(xs)             => COALESCE(xs)
      case BuiltInFunc.URI(s)                   => URI(s)
      case BuiltInFunc.LANG(s)                  => LANG(s)
      case BuiltInFunc.LANGMATCHES(s, StringVal.STRING(range)) =>
        LANGMATCHES(s, range)
      case BuiltInFunc.LCASE(s)     => LCASE(s)
      case BuiltInFunc.UCASE(s)     => UCASE(s)
      case BuiltInFunc.ISLITERAL(s) => ISLITERAL(s)
      case BuiltInFunc.CONCAT(appendTo, append) =>
        CONCAT(appendTo, NonEmptyList.fromListUnsafe(append))
      case BuiltInFunc.STR(s)                           => STR(s)
      case BuiltInFunc.STRAFTER(s, StringVal.STRING(f)) => STRAFTER(s, f)
      case BuiltInFunc.STRAFTER(s, l @ StringVal.LANG_STRING(_, _)) =>
        STRAFTER(s, StringVal.LANG_STRING.toString(l))
      case BuiltInFunc.STRAFTER(s, t @ StringVal.DT_STRING(_, _)) =>
        STRAFTER(s, StringVal.DT_STRING.toString(t))
      case BuiltInFunc.STRBEFORE(s, StringVal.STRING(f)) => STRBEFORE(s, f)
      case BuiltInFunc.STRBEFORE(s, l @ StringVal.LANG_STRING(_, _)) =>
        STRBEFORE(s, StringVal.LANG_STRING.toString(l))
      case BuiltInFunc.STRBEFORE(s, t @ StringVal.DT_STRING(_, _)) =>
        STRBEFORE(s, StringVal.DT_STRING.toString(t))
      case BuiltInFunc.SUBSTR(
            s,
            StringVal.NUM(pos),
            Some(StringVal.NUM(len))
          ) =>
        SUBSTR(s, pos.toInt, Some(len.toInt))
      case BuiltInFunc.SUBSTR(s, StringVal.NUM(pos), None) =>
        SUBSTR(s, pos.toInt, None)
      case BuiltInFunc.STRLEN(s) => STRLEN(s)
      case BuiltInFunc.REPLACE(
            st,
            StringVal.STRING(pattern),
            StringVal.STRING(by),
            StringVal.STRING(flags)
          ) =>
        REPLACE(st, pattern, by, flags)
      case BuiltInFunc.REGEX(
            s,
            StringVal.STRING(pattern),
            StringVal.STRING(flags)
          ) =>
        REGEX(s, pattern, flags)
      case BuiltInFunc.STRENDS(s, StringVal.STRING(f)) => STRENDS(s, f)
      case BuiltInFunc.STRENDS(s, l @ StringVal.LANG_STRING(_, _)) =>
        STRENDS(s, StringVal.LANG_STRING.toString(l))
      case BuiltInFunc.STRENDS(s, t @ StringVal.DT_STRING(_, _)) =>
        STRENDS(s, StringVal.DT_STRING.toString(t))
      case BuiltInFunc.STRSTARTS(s, StringVal.STRING(f)) => STRSTARTS(s, f)
      case BuiltInFunc.STRSTARTS(s, l @ StringVal.LANG_STRING(_, _)) =>
        STRSTARTS(s, StringVal.LANG_STRING.toString(l))
      case BuiltInFunc.STRSTARTS(s, t @ StringVal.DT_STRING(_, _)) =>
        STRSTARTS(s, StringVal.DT_STRING.toString(t))
      case BuiltInFunc.STRDT(s, StringVal.URIVAL(uri)) => STRDT(s, uri)
      case BuiltInFunc.ISBLANK(s)                      => ISBLANK(s)
      case BuiltInFunc.ISNUMERIC(s)                    => ISNUMERIC(s)
      case BuiltInFunc.ENCODE_FOR_URI(s)               => ENCODE_FOR_URI(s)
      case BuiltInFunc.MD5(s)                          => MD5(s)
      case BuiltInFunc.SHA1(s)                         => SHA1(s)
      case BuiltInFunc.SHA256(s)                       => SHA256(s)
      case BuiltInFunc.SHA384(s)                       => SHA384(s)
      case BuiltInFunc.SHA512(s)                       => SHA512(s)
      case Aggregate.COUNT(e)                          => COUNT(e)
      case Aggregate.SUM(e)                            => SUM(e)
      case Aggregate.MIN(e)                            => MIN(e)
      case Aggregate.MAX(e)                            => MAX(e)
      case Aggregate.AVG(e)                            => AVG(e)
      case Aggregate.SAMPLE(e)                         => SAMPLE(e)
      case Aggregate.GROUP_CONCAT(e, separator)        => GROUP_CONCAT(e, separator)
      case StringVal.STRING(s)                         => STRING(s)
      case StringVal.DT_STRING(s, tag)                 => DT_STRING(s, tag)
      case StringVal.LANG_STRING(s, tag)               => LANG_STRING(s, tag)
      case StringVal.NUM(s)                            => NUM(s)
      case StringVal.VARIABLE(s)                       => VARIABLE(s)
      case StringVal.URIVAL(s)                         => URIVAL(s)
      case StringVal.BLANK(s)                          => BLANK(s)
      case StringVal.BOOL(s)                           => BOOL(s)
      case ConditionOrder.ASC(e)                       => ASC(e)
      case ConditionOrder.DESC(e)                      => DESC(e)
      case BuiltInFunc.UUID()                          => UUID()
      case BuiltInFunc.CEIL(s)                         => CEIL(s)
      case BuiltInFunc.ROUND(s)                        => ROUND(s)
      case BuiltInFunc.RAND()                          => RAND()
      case BuiltInFunc.ABS(s)                          => ABS(s)
      case BuiltInFunc.FLOOR(s)                        => FLOOR(s)
    }

  val toExpressionAlgebra: Algebra[ExpressionF, Expression] =
    Algebra {
      case ADD(l, r)                => Arithmetic.ADD(l, r)
      case SUBTRACT(l, r)           => Arithmetic.SUBTRACT(l, r)
      case MULTIPLY(l, r)           => Arithmetic.MULTIPLY(l, r)
      case DIVIDE(l, r)             => Arithmetic.DIVIDE(l, r)
      case EQUALS(l, r)             => Conditional.EQUALS(l, r)
      case GT(l, r)                 => Conditional.GT(l, r)
      case LT(l, r)                 => Conditional.LT(l, r)
      case GTE(l, r)                => Conditional.GTE(l, r)
      case LTE(l, r)                => Conditional.LTE(l, r)
      case OR(l, r)                 => Conditional.OR(l, r)
      case AND(l, r)                => Conditional.AND(l, r)
      case NEGATE(s)                => Conditional.NEGATE(s)
      case IN(e, xs)                => Conditional.IN(e, xs)
      case SAMETERM(l, r)           => Conditional.SAMETERM(l, r)
      case IF(cnd, ifTrue, ifFalse) => Conditional.IF(cnd, ifTrue, ifFalse)
      case BOUND(e)                 => Conditional.BOUND(e)
      case COALESCE(xs)             => Conditional.COALESCE(xs)
      case UCASE(s) =>
        BuiltInFunc.UCASE(s.asInstanceOf[StringLike])
      case LANG(s) =>
        BuiltInFunc.LANG(s.asInstanceOf[StringLike])
      case LANGMATCHES(s, range) =>
        BuiltInFunc.LANGMATCHES(
          s.asInstanceOf[StringLike],
          range.asInstanceOf[StringLike]
        )
      case LCASE(s) =>
        BuiltInFunc.LCASE(s.asInstanceOf[StringLike])
      case ISLITERAL(s) =>
        BuiltInFunc.ISLITERAL(s.asInstanceOf[StringLike])
      case REGEX(s, pattern, flags) =>
        BuiltInFunc.REGEX(
          s.asInstanceOf[StringLike],
          pattern.asInstanceOf[StringLike],
          flags.asInstanceOf[StringLike]
        )
      case STRENDS(s, f) =>
        BuiltInFunc.STRENDS(
          s.asInstanceOf[StringLike],
          f.asInstanceOf[StringLike]
        )
      case STRSTARTS(s, f) =>
        BuiltInFunc.STRSTARTS(
          s.asInstanceOf[StringLike],
          f.asInstanceOf[StringLike]
        )
      case STRDT(s, uri) =>
        BuiltInFunc.STRDT(
          s,
          StringVal.URIVAL(uri)
        )
      case URI(s) => BuiltInFunc.URI(s.asInstanceOf[StringLike])
      case CONCAT(appendTo, append) =>
        BuiltInFunc.CONCAT(
          appendTo.asInstanceOf[StringLike],
          append.map(_.asInstanceOf[StringLike]).toList
        )
      case STR(s) => BuiltInFunc.STR(s.asInstanceOf[StringLike])
      case STRAFTER(s, f) =>
        BuiltInFunc.STRAFTER(
          s.asInstanceOf[StringLike],
          f.asInstanceOf[StringLike]
        )
      case STRBEFORE(s, f) =>
        BuiltInFunc.STRBEFORE(
          s.asInstanceOf[StringLike],
          f.asInstanceOf[StringLike]
        )
      case SUBSTR(s, pos, len) =>
        BuiltInFunc.SUBSTR(
          s.asInstanceOf[StringLike],
          pos.asInstanceOf[StringLike],
          len.asInstanceOf[Option[StringLike]]
        )
      case REPLACE(st, pattern, by, flags) =>
        BuiltInFunc.REPLACE(
          st.asInstanceOf[StringLike],
          pattern.asInstanceOf[StringLike],
          by.asInstanceOf[StringLike],
          flags.asInstanceOf[StringLike]
        )
      case STRLEN(s)    => BuiltInFunc.STRLEN(s.asInstanceOf[StringLike])
      case ISBLANK(s)   => BuiltInFunc.ISBLANK(s.asInstanceOf[StringLike])
      case ISNUMERIC(s) => BuiltInFunc.ISNUMERIC(s.asInstanceOf[StringLike])
      case ENCODE_FOR_URI(s) =>
        BuiltInFunc.ENCODE_FOR_URI(s.asInstanceOf[StringLike])
      case MD5(s) =>
        BuiltInFunc.MD5(s.asInstanceOf[StringLike])
      case SHA1(s) =>
        BuiltInFunc.SHA1(s.asInstanceOf[StringLike])
      case SHA256(s) =>
        BuiltInFunc.SHA256(s.asInstanceOf[StringLike])
      case SHA384(s) =>
        BuiltInFunc.SHA384(s.asInstanceOf[StringLike])
      case SHA512(s) =>
        BuiltInFunc.SHA512(s.asInstanceOf[StringLike])
      case COUNT(e)                   => Aggregate.COUNT(e)
      case SUM(e)                     => Aggregate.SUM(e)
      case MIN(e)                     => Aggregate.MIN(e)
      case MAX(e)                     => Aggregate.MAX(e)
      case AVG(e)                     => Aggregate.AVG(e)
      case SAMPLE(e)                  => Aggregate.SAMPLE(e)
      case GROUP_CONCAT(e, separator) => Aggregate.GROUP_CONCAT(e, separator)
      case STRING(s)                  => StringVal.STRING(s)
      case DT_STRING(s, tag)          => StringVal.DT_STRING(s, tag)
      case LANG_STRING(s, tag)        => StringVal.LANG_STRING(s, tag)
      case NUM(s)                     => StringVal.NUM(s)
      case VARIABLE(s)                => StringVal.VARIABLE(s)
      case URIVAL(s)                  => StringVal.URIVAL(s)
      case BLANK(s)                   => StringVal.BLANK(s)
      case BOOL(s)                    => StringVal.BOOL(s)
      case ASC(e)                     => ConditionOrder.ASC(e)
      case DESC(e)                    => ConditionOrder.DESC(e)
      case UUID()                     => BuiltInFunc.UUID()
      case CEIL(s)                    => BuiltInFunc.CEIL(s)
      case ROUND(s)                   => BuiltInFunc.ROUND(s)
      case RAND()                     => BuiltInFunc.RAND()
      case ABS(s)                     => BuiltInFunc.ABS(s)
      case FLOOR(s)                   => BuiltInFunc.FLOOR(s)
    }

  implicit val basis: Basis[ExpressionF, Expression] =
    Basis.Default[ExpressionF, Expression](
      algebra = toExpressionAlgebra,
      coalgebra = fromExpressionCoalg
    )

  // scalastyle:off
  def compile[T](
      t: T,
      config: Config
  )(implicit T: Basis[ExpressionF, T]): DataFrame => Result[Column] = df => {
    val algebraM: AlgebraM[M, ExpressionF, Column] =
      AlgebraM.apply[M, ExpressionF, Column] {
        case ADD(l, r)      => FuncArithmetics.add(l, r).pure[M]
        case SUBTRACT(l, r) => FuncArithmetics.subtract(l, r).pure[M]
        case MULTIPLY(l, r) => FuncArithmetics.multiply(l, r).pure[M]
        case DIVIDE(l, r)   => FuncArithmetics.divide(l, r).pure[M]
        case REGEX(s, pattern, flags) =>
          FuncStrings.regex(s, pattern, flags).pure[M]
        case REPLACE(st, pattern, by, flags) =>
          FuncStrings.replace(st, pattern, by, flags).pure[M]
        case STRENDS(s, f)       => FuncStrings.strends(s, f).pure[M]
        case STRSTARTS(s, f)     => FuncStrings.strstarts(s, f).pure[M]
        case STRAFTER(s, f)      => FuncStrings.strafter(s, f).pure[M]
        case STRBEFORE(s, f)     => FuncStrings.strbefore(s, f).pure[M]
        case SUBSTR(s, pos, len) => FuncStrings.substr(s, pos, len).pure[M]
        case STRLEN(s)           => FuncStrings.strlen(s).pure[M]
        case CONCAT(appendTo, append) =>
          FuncStrings.concat(appendTo, append).pure[M]
        case LANGMATCHES(s, range) => FuncStrings.langMatches(s, range).pure[M]
        case LCASE(s)              => FuncStrings.lcase(s).pure[M]
        case UCASE(s)              => FuncStrings.ucase(s).pure[M]
        case ENCODE_FOR_URI(s)     => FuncStrings.encodeForURI(s).pure[M]
        case EQUALS(l, r)          => FuncForms.equals(l, r).pure[M]
        case GT(l, r)              => FuncForms.gt(l, r).pure[M]
        case LT(l, r)              => FuncForms.lt(l, r).pure[M]
        case GTE(l, r)             => FuncForms.gte(l, r).pure[M]
        case LTE(l, r)             => FuncForms.lte(l, r).pure[M]
        case OR(l, r)              => FuncForms.or(l, r).pure[M]
        case AND(l, r)             => FuncForms.and(l, r).pure[M]
        case NEGATE(s)             => FuncForms.negate(s).pure[M]
        case IN(e, xs)             => FuncForms.in(e, xs).pure[M]
        case SAMETERM(l, r)        => FuncForms.sameTerm(l, r).pure[M]
        case IF(cnd, ifTrue, ifFalse) =>
          FuncForms.`if`(cnd, ifTrue, ifFalse).pure[M]
        case BOUND(e)                   => FuncForms.bound(e).pure[M]
        case COALESCE(xs)               => FuncForms.coalesce(xs).pure[M]
        case STR(s)                     => FuncTerms.str(s).pure[M]
        case STRDT(e, uri)              => FuncTerms.strdt(e, uri).pure[M]
        case URI(s)                     => FuncTerms.iri(s).pure[M]
        case LANG(s)                    => FuncTerms.lang(s).pure[M]
        case ISLITERAL(s)               => FuncTerms.isLiteral(s).pure[M]
        case ISBLANK(s)                 => FuncTerms.isBlank(s).pure[M]
        case ISNUMERIC(s)               => FuncTerms.isNumeric(s).pure[M]
        case UUID()                     => FuncTerms.uuid().pure[M]
        case MD5(s)                     => FuncHash.md5(s).pure[M]
        case SHA1(s)                    => FuncHash.sha1(s).pure[M]
        case SHA256(s)                  => FuncHash.sha256(s).pure[M]
        case SHA384(s)                  => FuncHash.sha384(s).pure[M]
        case SHA512(s)                  => FuncHash.sha512(s).pure[M]
        case COUNT(e)                   => unknownFunction("COUNT")
        case SUM(e)                     => unknownFunction("SUM")
        case MIN(e)                     => unknownFunction("MIN")
        case MAX(e)                     => unknownFunction("MAX")
        case AVG(e)                     => unknownFunction("AVG")
        case SAMPLE(e)                  => unknownFunction("SAMPLE")
        case GROUP_CONCAT(e, separator) => unknownFunction("GROUP_CONCAT")
        case STRING(s)                  => lit(s).pure[M]
        case DT_STRING(s, tag)          => lit(s""""$s"^^$tag""").pure[M]
        case LANG_STRING(s, tag)        => lit(s""""$s"@$tag""").pure[M]
        case NUM(s)                     => lit(s).pure[M]
        case VARIABLE(s) =>
          M.inspect[Result, Config, Log, DataFrame, Column](_(s))
        case URIVAL(s) => lit(s).pure[M]
        case BLANK(s)  => lit(s).pure[M]
        case BOOL(s)   => lit(s).pure[M]
        case ASC(e)    => unknownFunction("ASC")
        case DESC(e)   => unknownFunction("DESC")
        case CEIL(s)   => FuncNumerics.ceil(s).pure[M]
        case ROUND(s)  => FuncNumerics.round(s).pure[M]
        case RAND()    => FuncNumerics.rand.pure[M]
        case ABS(s)    => FuncNumerics.abs(s).pure[M]
        case FLOOR(s)  => FuncNumerics.floor(s).pure[M]
      }
    // scalastyle:on

    val eval = scheme.cataM[M, ExpressionF, T, Column](algebraM)

    eval(t).runA(config, df)
  }

  private def unknownFunction(name: String): M[Column] =
    M.liftF[Result, Config, Log, DataFrame, Column](
      EngineError.UnknownFunction(name).asLeft[Column]
    )

}
