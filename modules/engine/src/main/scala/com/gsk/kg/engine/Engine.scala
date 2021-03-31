package com.gsk.kg.engine

import cats.Foldable
import cats.data.NonEmptyList
import cats.instances.all._
import cats.syntax.applicative._
import cats.syntax.either._

import higherkindness.droste._

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._

import com.gsk.kg.engine.data.ChunkedList
import com.gsk.kg.sparqlparser.Expression
import com.gsk.kg.sparqlparser.StringVal
import com.gsk.kg.sparqlparser.StringVal._
import com.gsk.kg.sparqlparser._

import java.{util => ju}

object Engine {

  def evaluateAlgebraM(implicit sc: SQLContext): AlgebraM[M, DAG, Multiset] =
    AlgebraM[M, DAG, Multiset] {
      case DAG.Describe(vars, r) => notImplemented("Describe")
      case DAG.Ask(r)            => notImplemented("Ask")
      case DAG.Construct(bgp, r) => evaluateConstruct(bgp, r)
      case DAG.Scan(graph, expr) =>
        evaluateScan(graph, expr)
      case DAG.Project(variables, r) => r.select(variables: _*).pure[M]
      case DAG.Bind(variable, expression, r) =>
        evaluateBind(variable, expression, r)
      case DAG.BGP(quads)              => evaluateBGP(quads)
      case DAG.LeftJoin(l, r, filters) => evaluateLeftJoin(l, r, filters)
      case DAG.Union(l, r)             => evaluateUnion(l, r)
      case DAG.Filter(funcs, expr)     => evaluateFilter(funcs, expr)
      case DAG.Join(l, r)              => evaluateJoin(l, r)
      case DAG.Offset(offset, r)       => evaluateOffset(offset, r)
      case DAG.Limit(limit, r)         => evaluateLimit(limit, r)
      case DAG.Distinct(r)             => evaluateDistinct(r)
      case DAG.Noop(str)               => notImplemented("Noop")
    }

  def evaluate[T: Basis[DAG, *]](
      dataframe: DataFrame,
      dag: T
  )(implicit
      sc: SQLContext
  ): Result[DataFrame] = {
    val eval =
      scheme.cataM[M, DAG, T, Multiset](evaluateAlgebraM)

    validateInputDataFrame(dataframe).flatMap { df =>
      eval(dag)
        .runA(df)
        .map(_.dataframe)
    }
  }

  private def validateInputDataFrame(df: DataFrame): Result[DataFrame] = {
    val hasThreeOrFourColumns = df.columns.length == 3 || df.columns.length == 4

    for {
      _ <- Either.cond(
        hasThreeOrFourColumns,
        df,
        EngineError.InvalidInputDataFrame("Input DF must have 3 or 4 columns")
      )
      dataFrame =
        if (df.columns.length == 3) {
          df.withColumn("g", lit(""))
        } else {
          df
        }
    } yield dataFrame
  }

  implicit val spoEncoder: Encoder[Row] = RowEncoder(
    StructType(
      List(
        StructField("s", StringType),
        StructField("p", StringType),
        StructField("o", StringType)
      )
    )
  )

  private def evaluateJoin(l: Multiset, r: Multiset): M[Multiset] =
    l.join(r).pure[M]

  private def evaluateUnion(l: Multiset, r: Multiset): M[Multiset] =
    l.union(r).pure[M]

  private def evaluateScan(graph: String, expr: Multiset): M[Multiset] = {
    val df = expr.dataframe
      .filter(expr.dataframe(GRAPH_VARIABLE.s) === graph)
//      .withColumn(GRAPH_VARIABLE.s, lit(""))
    expr.copy(dataframe = df).pure[M]
  }

  private def evaluateBGP(
      quads: ChunkedList[Expr.Quad]
  )(implicit sc: SQLContext): M[Multiset] = {
    import sc.implicits._
    import org.apache.spark.sql.functions._
    M.get[Result, DataFrame].map { df =>
      Foldable[ChunkedList].fold(
        quads.mapChunks { chunk =>
          val condition: Column =
            chunk
              .map(_.getPredicates)
              .map(
                _.map({ case (pred, position) =>
                  df(position) === pred.s
                }).foldLeft(lit(true))((acc, current) => acc && current)
              )
              .foldLeft(lit(false))((acc, current) => acc || current)

          val current = df.filter(condition)

          val vars =
            chunk.map(_.getNamesAndPositions).toChain.toList.flatten

          val selected =
            current.select(vars.map(v => $"${v._2}".as(v._1.s)): _*)

          Multiset(
            vars.map {
              case (GRAPH_VARIABLE, _) =>
                VARIABLE(GRAPH_VARIABLE.s)
              case (other, _) =>
                other.asInstanceOf[VARIABLE]
            }.toSet,
            selected
          )
        }
      )
    }
  }

  private def evaluateDistinct(r: Multiset): M[Multiset] =
    M.liftF(r.distinct)

  private def evaluateLeftJoin(
      l: Multiset,
      r: Multiset,
      filters: List[Expression]
  ): M[Multiset] = {
    NonEmptyList
      .fromList(filters)
      .map { nelFilters =>
        evaluateFilter(nelFilters, r).flatMapF(l.leftJoin)
      }
      .getOrElse {
        M.liftF(l.leftJoin(r))
      }
  }

  private def evaluateFilter(
      funcs: NonEmptyList[Expression],
      expr: Multiset
  ): M[Multiset] = {
    val compiledFuncs: NonEmptyList[DataFrame => Result[Column]] =
      funcs.map(ExpressionF.compile[Expression])

    M.liftF[Result, DataFrame, Multiset] {
      compiledFuncs.foldLeft(expr.asRight: Result[Multiset]) {
        case (eitherAcc, f) =>
          for {
            acc       <- eitherAcc
            filterCol <- f(acc.dataframe)
            result <-
              expr
                .filter(filterCol)
                .map(r =>
                  expr.copy(dataframe = r.dataframe intersect acc.dataframe)
                )
          } yield result
      }
    }
  }

  private def evaluateOffset(offset: Long, r: Multiset): M[Multiset] =
    M.liftF(r.offset(offset))

  private def evaluateLimit(limit: Long, r: Multiset): M[Multiset] =
    M.liftF(r.limit(limit))

  /** Evaluate a construct expression.
    *
    * Something we do in this that differs from the spec is that we
    * apply a default ordering to all solutions generated by the
    * [[bgp]], so that LIMIT and OFFSET can return meaningful results.
    */
  private def evaluateConstruct(bgp: Expr.BGP, r: Multiset)(implicit
      sc: SQLContext
  ): M[Multiset] = {

    // Extracting the triples to something that can be serialized in
    // Spark jobs
    val templateValues: List[List[(StringVal, Int)]] =
      bgp.quads
        .map(quad => List(quad.s -> 1, quad.p -> 2, quad.o -> 3))
        .toList

    val df = r.dataframe
      .flatMap { solution =>
        val extractBlanks: List[(StringVal, Int)] => List[StringVal] =
          triple => triple.filter(x => x._1.isBlank).map(_._1)

        val blankNodes: Map[String, String] =
          templateValues
            .flatMap(extractBlanks)
            .distinct
            .map(blankLabel => (blankLabel.s, ju.UUID.randomUUID().toString()))
            .toMap

        templateValues.map { triple =>
          val fields: List[Any] = triple
            .map({
              case (VARIABLE(s), pos) =>
                (solution.get(solution.fieldIndex(s)), pos)
              case (BLANK(x), pos) =>
                (blankNodes.get(x).get, pos)
              case (x, pos) =>
                (x.s, pos)
            })
            .sortBy(_._2)
            .map(_._1)

          Row.fromSeq(fields)
        }
      }
      .orderBy("s", "p")

    Multiset(
      Set.empty,
      df
    ).pure[M]
  }

  private def evaluateBind(
      bindTo: VARIABLE,
      bindFrom: Expression,
      r: Multiset
  ) = {
    val getColumn = ExpressionF.compile(bindFrom)

    M.liftF[Result, DataFrame, Multiset](
      getColumn(r.dataframe).map { col =>
        r.withColumn(bindTo, col)
      }
    )
  }

  private def notImplemented(constructor: String): M[Multiset] =
    M.liftF[Result, DataFrame, Multiset](
      EngineError.General(s"$constructor not implemented").asLeft[Multiset]
    )

}
