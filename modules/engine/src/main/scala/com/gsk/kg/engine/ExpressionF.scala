package com.gsk.kg.engine

import cats.implicits._

import higherkindness.droste._
import higherkindness.droste.macros.deriveTraverse

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import com.gsk.kg.sparqlparser.BuiltInFunc
import com.gsk.kg.sparqlparser.Conditional
import com.gsk.kg.sparqlparser.Expression
import com.gsk.kg.sparqlparser.StringLike
import com.gsk.kg.sparqlparser.StringVal

/** [[ExpressionF]] is a pattern functor for the recursive
  * [[Expression]].
  *
  * Using Droste's syntax, we get tree traversals for free such as the
  * ones seen in [[getVariable]] or [[getString]]
  */
@deriveTraverse sealed trait ExpressionF[+A]

object ExpressionF {

  final case class EQUALS[A](l: A, r: A)             extends ExpressionF[A]
  final case class REGEX[A](l: A, r: A)              extends ExpressionF[A]
  final case class STRSTARTS[A](l: A, r: A)          extends ExpressionF[A]
  final case class GT[A](l: A, r: A)                 extends ExpressionF[A]
  final case class LT[A](l: A, r: A)                 extends ExpressionF[A]
  final case class GTE[A](l: A, r: A)                extends ExpressionF[A]
  final case class LTE[A](l: A, r: A)                extends ExpressionF[A]
  final case class OR[A](l: A, r: A)                 extends ExpressionF[A]
  final case class AND[A](l: A, r: A)                extends ExpressionF[A]
  final case class NEGATE[A](s: A)                   extends ExpressionF[A]
  final case class URI[A](s: A)                      extends ExpressionF[A]
  final case class CONCAT[A](appendTo: A, append: A) extends ExpressionF[A]
  final case class STR[A](s: A)                      extends ExpressionF[A]
  final case class STRAFTER[A](s: A, f: String)      extends ExpressionF[A]
  final case class ISBLANK[A](s: A)                  extends ExpressionF[A]
  final case class REPLACE[A](st: A, pattern: String, by: String)
      extends ExpressionF[A]
  final case class STRING[A](s: String, tag: Option[String])
      extends ExpressionF[A]
  final case class NUM[A](s: String)      extends ExpressionF[A]
  final case class VARIABLE[A](s: String) extends ExpressionF[A]
  final case class URIVAL[A](s: String)   extends ExpressionF[A]
  final case class BLANK[A](s: String)    extends ExpressionF[A]
  final case class BOOL[A](s: String)     extends ExpressionF[A]

  val fromExpressionCoalg: Coalgebra[ExpressionF, Expression] =
    Coalgebra {
      case Conditional.EQUALS(l, r)                        => EQUALS(l, r)
      case Conditional.GT(l, r)                            => GT(l, r)
      case Conditional.LT(l, r)                            => LT(l, r)
      case Conditional.GTE(l, r)                           => GTE(l, r)
      case Conditional.LTE(l, r)                           => LTE(l, r)
      case Conditional.OR(l, r)                            => OR(l, r)
      case Conditional.AND(l, r)                           => AND(l, r)
      case Conditional.NEGATE(s)                           => NEGATE(s)
      case BuiltInFunc.URI(s)                              => URI(s)
      case BuiltInFunc.CONCAT(appendTo, append)            => CONCAT(appendTo, append)
      case BuiltInFunc.STR(s)                              => STR(s)
      case BuiltInFunc.STRAFTER(s, StringVal.STRING(f, _)) => STRAFTER(s, f)
      case BuiltInFunc.ISBLANK(s)                          => ISBLANK(s)
      case BuiltInFunc.REPLACE(
            st,
            StringVal.STRING(pattern, _),
            StringVal.STRING(by, _)
          ) =>
        REPLACE(st, pattern, by)
      case BuiltInFunc.REGEX(l, r)     => REGEX(l, r)
      case BuiltInFunc.STRSTARTS(l, r) => STRSTARTS(l, r)
      case StringVal.STRING(s, tag)    => STRING(s, tag)
      case StringVal.NUM(s)            => NUM(s)
      case StringVal.VARIABLE(s)       => VARIABLE(s)
      case StringVal.URIVAL(s)         => URIVAL(s)
      case StringVal.BLANK(s)          => BLANK(s)
      case StringVal.BOOL(s)           => BOOL(s)
    }

  val toExpressionAlgebra: Algebra[ExpressionF, Expression] =
    Algebra {
      case EQUALS(l, r)    => Conditional.EQUALS(l, r)
      case GT(l, r)        => Conditional.GT(l, r)
      case LT(l, r)        => Conditional.LT(l, r)
      case GTE(l, r)       => Conditional.GTE(l, r)
      case LTE(l, r)       => Conditional.LTE(l, r)
      case OR(l, r)        => Conditional.OR(l, r)
      case AND(l, r)       => Conditional.AND(l, r)
      case NEGATE(s)       => Conditional.NEGATE(s)
      case REGEX(l, r)     => BuiltInFunc.REGEX(l, r)
      case STRSTARTS(l, r) => BuiltInFunc.STRSTARTS(l, r)
      case URI(s)          => BuiltInFunc.URI(s.asInstanceOf[StringLike])
      case CONCAT(appendTo, append) =>
        BuiltInFunc.CONCAT(
          appendTo.asInstanceOf[StringLike],
          append.asInstanceOf[StringLike]
        )
      case STR(s) => BuiltInFunc.STR(s.asInstanceOf[StringLike])
      case STRAFTER(s, f) =>
        BuiltInFunc.STRAFTER(
          s.asInstanceOf[StringLike],
          f.asInstanceOf[StringLike]
        )
      case ISBLANK(s) => BuiltInFunc.ISBLANK(s.asInstanceOf[StringLike])
      case REPLACE(st, pattern, by) =>
        BuiltInFunc.REPLACE(
          st.asInstanceOf[StringLike],
          pattern.asInstanceOf[StringLike],
          by.asInstanceOf[StringLike]
        )
      case STRING(s, tag) => StringVal.STRING(s, tag)
      case NUM(s)         => StringVal.NUM(s)
      case VARIABLE(s)    => StringVal.VARIABLE(s)
      case URIVAL(s)      => StringVal.URIVAL(s)
      case BLANK(s)       => StringVal.BLANK(s)
      case BOOL(s)        => StringVal.BOOL(s)
    }

  implicit val basis: Basis[ExpressionF, Expression] =
    Basis.Default[ExpressionF, Expression](
      algebra = toExpressionAlgebra,
      coalgebra = fromExpressionCoalg
    )

  def compile[T](
      t: T
  )(implicit T: Basis[ExpressionF, T]): DataFrame => Result[Column] = df => {
    val algebraM: AlgebraM[M, ExpressionF, Column] =
      AlgebraM.apply[M, ExpressionF, Column] {
        case EQUALS(l, r) => Func.equals(l, r).pure[M]
        case REGEX(l, r) =>
          M.liftF[Result, DataFrame, Column](
            EngineError.UnknownFunction("REGEX").asLeft[Column]
          )
        case STRSTARTS(l, r) =>
          M.liftF[Result, DataFrame, Column](
            EngineError.UnknownFunction("STRSTARTS").asLeft[Column]
          )
        case GT(l, r)  => Func.gt(l, r).pure[M]
        case LT(l, r)  => Func.lt(l, r).pure[M]
        case GTE(l, r) => Func.gte(l, r).pure[M]
        case LTE(l, r) => Func.lte(l, r).pure[M]
        case OR(l, r)  => Func.or(l, r).pure[M]
        case AND(l, r) => Func.and(l, r).pure[M]
        case NEGATE(s) => Func.negate(s).pure[M]

        case URI(s)                   => Func.iri(s).pure[M]
        case CONCAT(appendTo, append) => Func.concat(appendTo, append).pure[M]
        case STR(s)                   => s.pure[M]
        case STRAFTER(s, f)           => Func.strafter(s, f).pure[M]
        case ISBLANK(s)               => Func.isBlank(s).pure[M]
        case REPLACE(st, pattern, by) => Func.replace(st, pattern, by).pure[M]

        case STRING(s, None)      => lit(s).pure[M]
        case STRING(s, Some(tag)) => lit(s""""$s"^^$tag""").pure[M]
        case NUM(s)               => lit(s).pure[M]
        case VARIABLE(s)          => M.inspect[Result, DataFrame, Column](_(s))
        case URIVAL(s)            => lit(s).pure[M]
        case BLANK(s)             => lit(s).pure[M]
        case BOOL(s)              => lit(s).pure[M]
      }

    val eval = scheme.cataM[M, ExpressionF, T, Column](algebraM)

    eval(t).runA(df)
  }

}
