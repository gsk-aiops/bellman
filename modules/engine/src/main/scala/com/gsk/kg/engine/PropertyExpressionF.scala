package com.gsk.kg.engine

import cats.implicits._

import higherkindness.droste._

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame

import com.gsk.kg.config.Config
import com.gsk.kg.engine.properties.FuncProperty
import com.gsk.kg.sparqlparser.EngineError
import com.gsk.kg.sparqlparser.PropertyExpression.fixedpoint._
import com.gsk.kg.sparqlparser.Result

object PropertyExpressionF {

  def compile[T](
      t: T,
      config: Config
  )(implicit T: Basis[PropertyExpressionF, T]): DataFrame => Result[Column] =
    df => {
      val algebraM: AlgebraM[M, PropertyExpressionF, Column] =
        AlgebraM.apply[M, PropertyExpressionF, Column] {
          case AlternativeF(pel, per) =>
            FuncProperty.alternative(df, pel, per).pure[M]
          case ReverseF(e)              => unknownPropertyPath("reverse")
          case SeqExpressionF(pel, per) => unknownPropertyPath("seqExpression")
          case OneOrMoreF(e)            => unknownPropertyPath("oneOrMore")
          case ZeroOrMoreF(e)           => unknownPropertyPath("zeroOrMore")
          case ZeroOrOneF(e)            => unknownPropertyPath("zeroOrOne")
          case NotOneOfF(es)            => unknownPropertyPath("notOneOf")
          case BetweenNAndMF(n, m, e)   => unknownPropertyPath("betweenNAndM")
          case ExactlyNF(n, e)          => unknownPropertyPath("exactlyN")
          case NOrMoreF(n, e)           => unknownPropertyPath("nOrMore")
          case BetweenZeroAndNF(n, e)   => unknownPropertyPath("betweenZeroAndN")
          case UriF(s)                  => FuncProperty.uri(s).pure[M]
        }

      val eval = scheme.cataM[M, PropertyExpressionF, T, Column](algebraM)

      eval(t).runA(config, df)
    }

  private def unknownPropertyPath(name: String): M[Column] =
    M.liftF[Result, Config, Log, DataFrame, Column](
      EngineError.UnknownPropertyPath(name).asLeft[Column]
    )
}
