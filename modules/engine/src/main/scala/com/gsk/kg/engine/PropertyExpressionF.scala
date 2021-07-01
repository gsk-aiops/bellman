package com.gsk.kg.engine

import cats.implicits._

import higherkindness.droste._
import higherkindness.droste.macros.deriveTraverse

import org.apache.spark.sql.DataFrame

import com.gsk.kg.config.Config
import com.gsk.kg.sparqlparser.EngineError
import com.gsk.kg.sparqlparser.PropertyExpression
import com.gsk.kg.sparqlparser.Result

@deriveTraverse sealed trait PropertyExpressionF[+A]

object PropertyExpressionF {

  final case class Alternative[A](pel: A, per: A) extends PropertyExpressionF[A]
  final case class Reverse[A](e: A)               extends PropertyExpressionF[A]
  final case class SeqExpression[A](pel: A, per: A)
      extends PropertyExpressionF[A]
  final case class OneOrMore[A](e: A)       extends PropertyExpressionF[A]
  final case class ZeroOrMore[A](e: A)      extends PropertyExpressionF[A]
  final case class ZeroOrOne[A](e: A)       extends PropertyExpressionF[A]
  final case class NotOneOf[A](es: List[A]) extends PropertyExpressionF[A]
  final case class BetweenNAndM[A](n: Int, m: Int, e: A)
      extends PropertyExpressionF[A]
  final case class ExactlyN[A](n: Int, e: A) extends PropertyExpressionF[A]
  final case class NOrMore[A](n: Int, e: A)  extends PropertyExpressionF[A]
  final case class BetweenZeroAndN[A](n: Int, e: A)
      extends PropertyExpressionF[A]
  final case class Uri[A](s: String) extends PropertyExpressionF[A]

  val fromPropExprCoalg: Coalgebra[PropertyExpressionF, PropertyExpression] =
    Coalgebra {
      case PropertyExpression.Alternative(pel, per)   => Alternative(pel, per)
      case PropertyExpression.Reverse(e)              => Reverse(e)
      case PropertyExpression.SeqExpression(pel, per) => SeqExpression(pel, per)
      case PropertyExpression.OneOrMore(e)            => OneOrMore(e)
      case PropertyExpression.ZeroOrMore(e)           => ZeroOrMore(e)
      case PropertyExpression.ZeroOrOne(e)            => ZeroOrOne(e)
      case PropertyExpression.NotOneOf(es)            => NotOneOf(es)
      case PropertyExpression.BetweenNAndM(n, m, e)   => BetweenNAndM(n, m, e)
      case PropertyExpression.ExactlyN(n, e)          => ExactlyN(n, e)
      case PropertyExpression.NOrMore(n, e)           => NOrMore(n, e)
      case PropertyExpression.BetweenZeroAndN(n, e)   => BetweenZeroAndN(n, e)
      case PropertyExpression.Uri(s)                  => Uri(s)
    }

  val toPropExprAlg: Algebra[PropertyExpressionF, PropertyExpression] =
    Algebra {
      case Alternative(pel, per)   => PropertyExpression.Alternative(pel, per)
      case Reverse(e)              => PropertyExpression.Reverse(e)
      case SeqExpression(pel, per) => PropertyExpression.SeqExpression(pel, per)
      case OneOrMore(e)            => PropertyExpression.OneOrMore(e)
      case ZeroOrMore(e)           => PropertyExpression.ZeroOrMore(e)
      case ZeroOrOne(e)            => PropertyExpression.ZeroOrOne(e)
      case NotOneOf(es)            => PropertyExpression.NotOneOf(es)
      case BetweenNAndM(n, m, e)   => PropertyExpression.BetweenNAndM(n, m, e)
      case ExactlyN(n, e)          => PropertyExpression.ExactlyN(n, e)
      case NOrMore(n, e)           => PropertyExpression.NOrMore(n, e)
      case BetweenZeroAndN(n, e)   => PropertyExpression.BetweenZeroAndN(n, e)
      case Uri(s)                  => PropertyExpression.Uri(s)
    }

  implicit val basis: Basis[PropertyExpressionF, PropertyExpression] =
    Basis.Default[PropertyExpressionF, PropertyExpression](
      algebra = toPropExprAlg,
      coalgebra = fromPropExprCoalg
    )

  def compile[T](
      t: T,
      config: Config
  )(implicit T: Basis[PropertyExpressionF, T]): DataFrame => Result[DataFrame] =
    df => {
      val algebraM: AlgebraM[M, PropertyExpressionF, DataFrame] =
        AlgebraM.apply[M, PropertyExpressionF, DataFrame] {
          case Alternative(pel, per)   => unknownPropertyPath("alternative")
          case Reverse(e)              => unknownPropertyPath("reverse")
          case SeqExpression(pel, per) => unknownPropertyPath("seqExpression")
          case OneOrMore(e)            => unknownPropertyPath("oneOrMore")
          case ZeroOrMore(e)           => unknownPropertyPath("zeroOrMore")
          case ZeroOrOne(e)            => unknownPropertyPath("zeroOrOne")
          case NotOneOf(es)            => unknownPropertyPath("notOneOf")
          case BetweenNAndM(n, m, e)   => unknownPropertyPath("betweenNAndM")
          case ExactlyN(n, e)          => unknownPropertyPath("exactlyN")
          case NOrMore(n, e)           => unknownPropertyPath("nOrMore")
          case BetweenZeroAndN(n, e)   => unknownPropertyPath("betweenZeroAndN")
          case Uri(s)                  => unknownPropertyPath("uri")
        }

      val eval = scheme.cataM[M, PropertyExpressionF, T, DataFrame](algebraM)

      eval(t).runA(config, df)
    }

  private def unknownPropertyPath(name: String): M[DataFrame] =
    M.liftF[Result, Config, Log, DataFrame, DataFrame](
      EngineError.UnknownPropertyPath(name).asLeft[DataFrame]
    )
}
