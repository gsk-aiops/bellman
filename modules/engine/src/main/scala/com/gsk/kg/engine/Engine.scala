package com.gsk.kg.engine

import cats.Foldable
import cats.data.NonEmptyList
import cats.implicits.toTraverseOps
import cats.instances.all._
import cats.syntax.applicative._
import cats.syntax.either._

import higherkindness.droste._

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.RelationalGroupedDataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import com.gsk.kg.config.Config
import com.gsk.kg.engine.data.ChunkedList
import com.gsk.kg.engine.data.ChunkedList.Chunk
import com.gsk.kg.sparqlparser.ConditionOrder.ASC
import com.gsk.kg.sparqlparser.ConditionOrder.DESC
import com.gsk.kg.sparqlparser.Expr.Quad
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
      case DAG.Group(vars, func, r)    => evaluateGroup(vars, func, r)
      case DAG.Order(conds, r)         => evaluateOrder(conds, r)
      case DAG.Noop(str)               => notImplemented("Noop")
    }

  def evaluate[T: Basis[DAG, *]](
      dataframe: DataFrame,
      dag: T,
      config: Config
  )(implicit
      sc: SQLContext
  ): Result[DataFrame] = {
    val eval =
      scheme.cataM[M, DAG, T, Multiset](evaluateAlgebraM)

    validateInputDataFrame(dataframe).flatMap { df =>
      eval(dag)
        .runA(config, df)
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

  private def evaluateJoin(l: Multiset, r: Multiset)(implicit
      sc: SQLContext
  ): M[Multiset] =
    l.join(r).pure[M]

  private def evaluateUnion(l: Multiset, r: Multiset): M[Multiset] =
    l.union(r).pure[M]

  private def evaluateScan(graph: String, expr: Multiset): M[Multiset] = {
    val bindings =
      expr.bindings.filter(_.s != GRAPH_VARIABLE.s) + VARIABLE(graph)
    val df = expr.dataframe.withColumn(graph, expr.dataframe(GRAPH_VARIABLE.s))
    Multiset(
      bindings,
      df
    )
  }.pure[M]

  private def evaluateBGP(
      quads: ChunkedList[Expr.Quad]
  )(implicit sc: SQLContext): M[Multiset] = {
    import sc.implicits._

    M.get[Result, Config, Log, DataFrame].map { df =>
      Foldable[ChunkedList].fold(
        quads.mapChunks { chunk =>
          val condition = composedConditionFromChunk(df, chunk)
          val current   = df.filter(condition)
          val vars =
            chunk
              .map(_.getNamesAndPositions :+ (GRAPH_VARIABLE, "g"))
              .toChain
              .toList
              .flatten
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

  /** This method takes all the predicates from a chunk of Quads and generates a Spark condition as
    * a Column with the next constraints:
    * - Predicates on same column are composed with OR operations between conditions. Eg:
    *   (col1, List(p1, p2)) => (false OR (p1 OR p2))
    * - Predicates on different columns are composed with AND operations between conditions. Eg:
    *   ((col1, List(p1)), (col2, List(p2)) => (true && (p1 && p2))
    * - Predicates in the same chunk are composed with OR operation. Eg:
    *   (c1 -> (true && p1 && (p2 || p3)), c2 -> (true && p4)) =>
    *     (false || ((true && p1 && (p2 || p3)) || (true && p4)))
    * @param df
    * @param chunk
    * @return
    */
  def composedConditionFromChunk(
      df: DataFrame,
      chunk: Chunk[Quad]
  ): Column = {
    chunk
      .map { quad =>
        quad.getPredicates
          .groupBy(_._2)
          .map { case (_, vs) =>
            vs.map { case (pred, position) =>
              df(position) === pred.s
            }.foldLeft(lit(false))(_ || _)
          }
          .foldLeft(lit(true))(_ && _)
      }
      .foldLeft(lit(false))(_ || _)
  }

  private def evaluateDistinct(r: Multiset): M[Multiset] =
    M.liftF(r.distinct)

  private def evaluateGroup(
      vars: List[VARIABLE],
      func: Option[(VARIABLE, Expression)],
      r: Multiset
  ): M[Multiset] = {
    val df = r.dataframe

    val groupedDF = df.groupBy(
      (vars :+ VARIABLE(GRAPH_VARIABLE.s)).map(_.s).map(df.apply): _*
    )

    evaluateAggregation(vars :+ VARIABLE(GRAPH_VARIABLE.s), groupedDF, func)
      .map(df =>
        r.copy(
          dataframe = df,
          bindings =
            r.bindings.union(func.toSet[(VARIABLE, Expression)].map(x => x._1))
        )
      )
  }

  private def evaluateAggregation(
      vars: List[VARIABLE],
      df: RelationalGroupedDataset,
      func: Option[(VARIABLE, Expression)]
  ): M[DataFrame] = func match {
    case None =>
      val cols: List[Column] = vars.map(_.s).map(col).map(Func.sample)
      df.agg(cols.head, cols.tail: _*).pure[M]
    case Some((VARIABLE(name), Aggregate.COUNT(VARIABLE(v)))) =>
      df.agg(count(v).cast("int").as(name)).pure[M]
    case Some((VARIABLE(name), Aggregate.SUM(VARIABLE(v)))) =>
      df.agg(sum(v).cast("float").as(name)).pure[M]
    case Some((VARIABLE(name), Aggregate.MIN(VARIABLE(v)))) =>
      df.agg(min(v).as(name)).pure[M]
    case Some((VARIABLE(name), Aggregate.MAX(VARIABLE(v)))) =>
      df.agg(max(v).as(name)).pure[M]
    case Some((VARIABLE(name), Aggregate.AVG(VARIABLE(v)))) =>
      df.agg(avg(v).cast("float").as(name)).pure[M]
    case Some((VARIABLE(name), Aggregate.SAMPLE(VARIABLE(v)))) =>
      df.agg(Func.sample(col(v)).as(name)).pure[M]
    case Some(
          (VARIABLE(name), Aggregate.GROUP_CONCAT(VARIABLE(v), separator))
        ) =>
      df.agg(Func.groupConcat(col(v), separator))
        .withColumnRenamed(v, name)
        .pure[M]
    case fn =>
      M.liftF[Result, Config, Log, DataFrame, DataFrame](
        EngineError
          .UnknownFunction("Aggregate function: " + fn.toString)
          .asLeft[DataFrame]
      )
  }

  private def evaluateOrder(
      conds: NonEmptyList[ConditionOrder],
      r: Multiset
  ): M[Multiset] = {
    M.ask[Result, Config, Log, DataFrame].flatMapF { config =>
      conds
        .map {
          case ASC(VARIABLE(v)) =>
            col(v).asc.asRight
          case ASC(e) =>
            ExpressionF
              .compile[Expression](e, config)
              .apply(r.dataframe)
              .map(_.asc)
          case DESC(VARIABLE(v)) =>
            col(v).desc.asRight
          case DESC(e) =>
            ExpressionF
              .compile[Expression](e, config)
              .apply(r.dataframe)
              .map(_.desc)
        }
        .toList
        .sequence[Either[EngineError, *], Column]
        .map(columns => r.copy(dataframe = r.dataframe.orderBy(columns: _*)))
    }
  }

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
    val compiledFuncs: M[NonEmptyList[DataFrame => Result[Column]]] =
      M.ask[Result, Config, Log, DataFrame].map { config =>
        funcs.map(t => ExpressionF.compile[Expression](t, config))
      }

    compiledFuncs.flatMapF(_.foldLeft(expr.asRight: Result[Multiset]) {
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
    })
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
      .distinct()

    Multiset(
      Set.empty,
      df
    ).pure[M]
  }

  private def evaluateBind(
      bindTo: VARIABLE,
      bindFrom: Expression,
      r: Multiset
  ): M[Multiset] = {
    M.ask[Result, Config, Log, DataFrame].flatMapF { config =>
      val getColumn = ExpressionF.compile(bindFrom, config)
      getColumn(r.dataframe).map { col =>
        r.withColumn(bindTo, col)
      }
    }
  }

  private def notImplemented(constructor: String): M[Multiset] =
    M.liftF[Result, Config, Log, DataFrame, Multiset](
      EngineError.General(s"$constructor not implemented").asLeft[Multiset]
    )

}
