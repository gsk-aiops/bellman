package com.gsk.kg.engine

import cats.implicits._

import higherkindness.droste._

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

import com.gsk.kg.config.Config
import com.gsk.kg.engine.properties.FuncProperty
import com.gsk.kg.sparqlparser.EngineError
import com.gsk.kg.sparqlparser.PropertyExpression.fixedpoint._
import com.gsk.kg.sparqlparser.Result

object PropertyExpressionF {

  type ColOrDf = Either[Column, DataFrame]

  def compile[T](
      t: T,
      config: Config
  )(implicit
      T: Basis[PropertyExpressionF, T],
      sc: SQLContext
  ): DataFrame => Result[ColOrDf] =
    df => {
      val algebraM: AlgebraM[M, PropertyExpressionF, ColOrDf] =
        AlgebraM.apply[M, PropertyExpressionF, ColOrDf] {
          case AlternativeF(pel, per) =>
            M.liftF(FuncProperty.alternative(df, pel, per))
          case ReverseF(e) => unknownPropertyPath("reverse")
          case SeqExpressionF(pel, per) =>
            M.liftF(FuncProperty.seq(df, pel, per))
          case OneOrMoreF(e) =>
            M.liftF(FuncProperty.oneOrMore(df, e))
          case ZeroOrMoreF(e)         => unknownPropertyPath("zeroOrMore")
          case ZeroOrOneF(e)          => unknownPropertyPath("zeroOrOne")
          case NotOneOfF(es)          => unknownPropertyPath("notOneOf")
          case BetweenNAndMF(n, m, e) => unknownPropertyPath("betweenNAndM")
          case ExactlyNF(n, e)        => unknownPropertyPath("exactlyN")
          case NOrMoreF(n, e)         => unknownPropertyPath("nOrMore")
          case BetweenZeroAndNF(n, e) => unknownPropertyPath("betweenZeroAndN")
          case UriF(s)                => FuncProperty.uri(s).pure[M]
        }

      val eval = scheme.cataM[M, PropertyExpressionF, T, ColOrDf](algebraM)

      eval(t).runA(config, df)
    }

  private def unknownPropertyPath(name: String): M[ColOrDf] =
    M.liftF[Result, Config, Log, DataFrame, ColOrDf](
      EngineError.UnknownPropertyPath(name).asLeft[ColOrDf]
    )
}
