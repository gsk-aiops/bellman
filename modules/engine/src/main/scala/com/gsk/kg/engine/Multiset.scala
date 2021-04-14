package com.gsk.kg.engine

import cats.kernel.Monoid
import cats.kernel.Semigroup
import cats.syntax.all._

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._

import com.gsk.kg.engine.Multiset._
import com.gsk.kg.engine.Multiset.filterGraph
import com.gsk.kg.sparqlparser.StringVal.GRAPH_VARIABLE
import com.gsk.kg.sparqlparser.StringVal.VARIABLE

/** A [[Multiset]], as expressed in SparQL terms.
  *
  * @see See [[https://www.w3.org/2001/sw/DataAccess/rq23/rq24-algebra.html]]
  * @param bindings the variables this multiset expose
  * @param dataframe the underlying data that the multiset contains
  */
final case class Multiset(
    bindings: Set[VARIABLE],
    dataframe: DataFrame
) {

  /** Join two multisets following SparQL semantics.  If two multisets
    * share some bindings, it performs an _inner join_ between them.
    * If they don't share any common binding, it performs a cross join
    * instead.
    *
    * =Spec=
    *
    * Defn: Join
    * Let Ω1 and Ω2 be multisets of mappings. We define:
    * Join(Ω1, Ω2) = { merge(μ1, μ2) | μ1 in Ω1and μ2 in Ω2, and μ1 and μ2 are compatible }
    * card[Join(Ω1, Ω2)](μ) = sum over μ in (Ω1 set-union Ω2), card[Ω1](μ1)*card[Ω2](μ2)
    *
    * @param other
    * @return the join result of both multisets
    */
  def join(other: Multiset)(implicit sc: SQLContext): Multiset =
    (this, other) match {
      case (l, r) if l.isEmpty => r
      case (l, r) if r.isEmpty => l
      case (l, r) if noCommonBindings(l, r) =>
        Multiset(
          l.bindings union r.bindings,
          l.dataframe.crossJoin(r.dataframe)
        )
      case (l, r) =>
        Multiset(
          l.bindings union r.bindings,
          innerJoinWithGraphsColumn(l.dataframe, r.dataframe)
        )
    }

  /** A left join returns all values from the left relation and the matched values from the right relation,
    * or appends NULL if there is no match. It is also referred to as a left outer join.
    * @param other
    * @return
    */
  def leftJoin(other: Multiset): Result[Multiset] = ((this, other) match {
    case (l, r) if l.isEmpty => l
    case (l, r) if r.isEmpty => l
    case (l, r) if noCommonBindings(l, r) =>
      val df = l.dataframe.crossJoin(r.dataframe)
      Multiset(l.bindings union r.bindings, df)
    case (l, r) =>
      val df =
        l.dataframe.join(
          filterGraph(r).dataframe,
          (l.bindings intersect filterGraph(r).bindings).toSeq
            .map(_.s),
          "left"
        )
      Multiset(l.bindings union r.bindings, df)
  }).asRight

  /** Perform a union between [[this]] and [[other]], as described in
    * SparQL Algebra doc.
    *
    * =Spec=
    *
    * Defn: Union
    * Let Ω1 and Ω2 be multisets of mappings. We define:
    * Union(Ω1, Ω2) = { μ | μ in Ω1 or μ in Ω2 }
    * card[Union(Ω1, Ω2)](μ) = card[Ω1](μ) + card[Ω2](μ)
    *
    * @param other
    * @return the Union of both multisets
    */
  def union(other: Multiset): Multiset = (this, other) match {
    case (a, b) if a.isEmpty => b
    case (a, b) if b.isEmpty => a
    case (Multiset(aBindings, aDF), Multiset(bBindings, bDF))
        if aDF.columns == bDF.columns =>
      Multiset(
        aBindings.union(bBindings),
        aDF.union(bDF)
      )
    case (Multiset(aBindings, aDF), Multiset(bBindings, bDF)) =>
      val colsA     = aDF.columns.toSet
      val colsB     = bDF.columns.toSet
      val colsUnion = colsA.union(colsB)

      def genColumns(current: Set[String], total: Set[String]): Seq[Column] = {
        total.toList.sorted.map {
          case x if current.contains(x) => col(x)
          case x                        => lit(null).as(x) // scalastyle:ignore
        }
      }

      val bindingsUnion = aBindings union bBindings
      val selectionA    = aDF.select(genColumns(colsA, colsUnion): _*)
      val selectionB    = bDF.select(genColumns(colsB, colsUnion): _*)

      Multiset(
        bindingsUnion,
        selectionA.unionByName(selectionB)
      )
  }

  /** Return wether both the dataframe & bindings are empty
    *
    * @return
    */
  def isEmpty: Boolean = bindings.isEmpty && dataframe.isEmpty

  /** Get a new multiset with only the projected [[vars]].
    *
    * @param vars
    * @return
    */
  def select(vars: VARIABLE*): Multiset =
    Multiset(
      bindings.intersect(vars.toSet),
      dataframe.select(vars.map(v => dataframe(v.s)): _*)
    )

  /** Add a new column to the multiset, with the given binding
    *
    * @param binding
    * @param col
    * @param fn
    * @return
    */
  def withColumn(
      binding: VARIABLE,
      column: Column
  ): Multiset =
    Multiset(
      bindings + binding,
      dataframe
        .withColumn(binding.s, column)
    )

  /** A limited solutions sequence has at most given, fixed number of members.
    * The limit solution sequence S = (S0, S1, ..., Sn) is
    * limit(S, m) =
    * (S0, S1, ..., Sm-1) if n > m
    * (S0, S1, ..., Sn) if n <= m-1
    * @param limit
    * @return
    */
  def limit(limit: Long): Result[Multiset] = limit match {
    case l if l < 0 =>
      EngineError.UnexpectedNegative("Negative limit: $l").asLeft
    case l if l > Int.MaxValue.toLong =>
      EngineError
        .NumericTypesDoNotMatch(s"$l to big to be converted to an Int")
        .asLeft
    case l =>
      this.copy(dataframe = dataframe.limit(l.toInt)).asRight
  }

  /** An offset solution sequence with respect to another solution sequence S, is one which starts at a given index of S.
    * For solution sequence S = (S0, S1, ..., Sn), the offset solution sequence
    * offset(S, k), k >= 0 is
    * (Sk, Sk+1, ..., Sn) if n >= k
    * (), the empty sequence, if k > n
    * @param offset
    * @return
    */
  def offset(offset: Long): Result[Multiset] = if (offset < 0) {
    EngineError.UnexpectedNegative(s"Negative offset: $offset").asLeft
  } else {
    val rdd = dataframe.rdd.zipWithIndex
      .filter { case (_, idx) => idx >= offset }
      .map(_._1)
    val df =
      dataframe.sqlContext.sparkSession.createDataFrame(rdd, dataframe.schema)
    this.copy(dataframe = df).asRight
  }

  /** Filter restrict the set of solutions according to a given expression.
    * @param col
    * @return
    */
  def filter(col: Column): Result[Multiset] = {
    val filtered = dataframe.filter(col)
    this.copy(dataframe = filtered).asRight
  }

  /** Eliminates duplicates from the dataframe that matches the same variable binding
    * @return
    */
  def distinct: Result[Multiset] = {
    this
      .copy(
        dataframe = this.dataframe.distinct()
      )
      .asRight
  }
}

object Multiset {

  /** Removes graph bindings and graph columns from Multiset
    */
  private val filterGraph: Multiset => Multiset = { m =>
    if (m.isEmpty) {
      m
    } else {
      m.copy(
        bindings = m.bindings.filter(_.s != GRAPH_VARIABLE.s),
        dataframe = m.dataframe.drop(GRAPH_VARIABLE.s)
      )
    }
  }

  /** Adds graph binding and column to the Multiset (with default graph as the default value)
    */
  private val addDefaultGraph: Multiset => Multiset = { m =>
    if (m.isEmpty) {
      m
    } else {
      m.copy(
        bindings = m.bindings + VARIABLE(GRAPH_VARIABLE.s),
        dataframe = m.dataframe.withColumn(GRAPH_VARIABLE.s, lit(""))
      )
    }
  }

  /** Checks whether two Multisets has no common bindings without having into account graph bindings
    * @param l
    * @param r
    * @return
    */
  private def noCommonBindings(l: Multiset, r: Multiset): Boolean =
    (filterGraph(l).bindings intersect filterGraph(r).bindings).isEmpty

  /** Checks whether two Multisets both contains a graph variable column
    * @param l
    * @param r
    * @return
    */
  private def containsGraphVariables(l: Multiset, r: Multiset): Boolean =
    l.dataframe.columns.contains(GRAPH_VARIABLE.s) &&
      r.dataframe.columns.contains(GRAPH_VARIABLE.s)

  /** This methods is a utility to perform operations like [[union]], [[join]], [[leftJoin]] on Multisets without
    * taking into account graph bindings and graph columns on dataframes by:
    * removing from bindings and dataframe -> operate -> adding binding and column to final dataframe as default graph
    * @param left
    * @param right
    * @param f
    * @return
    */
  private def graphAgnostic(left: Multiset, right: Multiset)(
      f: (Multiset, Multiset) => Multiset
  ): Multiset =
    addDefaultGraph(f(filterGraph(left), filterGraph(right)))

  /** This method performs a cross join between graphs by merging the graph columns of each dataframes into an array
    * column, this way we generate new rows for each graph that is in the array. Eg:
    * Initial dataframes:
    * l.dataframe = List(
    *   ("a", "graph1"),
    *   ("b", "graph1")
    * ).toDF("?x", "*g")
    * r.dataframe = List((1, "graph2"), (2, "graph2")).toDF("?y", "*g")
    *
    * Step1:
    * step1Dataframe = List(
    *   ("a", 1, "graph1", "graph2"),
    *   ("a", 2, "graph1", "graph2"),
    *   ("b", 1, "graph1", "graph2"),
    *   ("b", 2, "graph1", "graph2")
    * ).toDF("?x", "?y", "*l", "*r")
    *
    * Step2:
    * step2Dataframe = List(
    *   ("a", 1, List("graph1", "graph2")),
    *   ("a", 2, List("graph1", "graph2")),
    *   ("b", 1, List("graph1", "graph2")),
    *   ("b", 2, List("graph1", "graph2"))
    * ).toDF("?x", "?y", "*g")
    *
    * Final:
    * final = List(
    *  ("a", 1, "graph1"),
    *  ("a", 1, "graph2"),
    *  ("a", 2, "graph1"),
    *  ("a", 2, "graph2"),
    *  ("b", 1, "graph1"),
    *  ("b", 1, "graph2"),
    *  ("b", 2, "graph1"),
    *  ("b", 2, "graph2")
    * ).toDF("?x", "?y", "*g")
    *
    * IMPORTANT!: This method could have some performance issues as it traverses entire dataframe, and should be revisited
    * for performance improvements.
    * @param l
    * @param r
    * @return
    */
  private def crossJoinWithGraphsColumns(
      l: DataFrame,
      r: DataFrame
  )(implicit sc: SQLContext): DataFrame = {
    val leftGraphCol  = "*l"
    val rightGraphCol = "*r"

    // Generates DF with two different columns of graphs *l, *r
    val productWithGraphs = l
      .as("a")
      .withColumnRenamed(GRAPH_VARIABLE.s, leftGraphCol)
      .crossJoin(
        r
          .withColumnRenamed(GRAPH_VARIABLE.s, rightGraphCol)
          .as("b")
      )

    // Merges the *l and *r columns into a *g column with Array(l, r)
    val productWithMergedGraphColunns = productWithGraphs
      .withColumn(GRAPH_VARIABLE.s, array(leftGraphCol, rightGraphCol))
      .drop(leftGraphCol, rightGraphCol)

    // Generates a schema for the final DF (needed for the flatMap)
    val resultSchema = productWithGraphs
      .drop(leftGraphCol, rightGraphCol)
      .withColumn(GRAPH_VARIABLE.s, lit(""))
      .schema

    // For each element on the array of *g column it generates a new row with the graph (unfolds *g column)
    val result = productWithMergedGraphColunns.flatMap { r =>
      val index  = r.fieldIndex(GRAPH_VARIABLE.s)
      val graphs = r.getSeq[String](index)
      graphs.distinct.map(g => Row.fromSeq(r.toSeq.dropRight(1) :+ g))
    }(RowEncoder(resultSchema))

    // Needed to keep Schema information on each Row. We will have GenericRowWithSchema instead of GenericRow
    sc.sparkSession.createDataFrame(result.toJavaRDD, resultSchema)
  }

  /** This method performs a inner join between graphs by merging the graph columns of each dataframes into an array
    * column, if they have same graph then the graph is preserved, if not it is added as if in the default graph. Eg:
    * Initial dataframes:
    * l.dataframe = List(
    *   ("a", "graph1"),
    *   ("b", "graph1"),
    *   ("c", "graph1")
    * ).toDF("?x", "*g")
    * r.dataframe = List(
    *   ("b", "graph1"),
    *   ("c", "graph2"),
    *   ("d", "graph2")
    * ).toDF("?x", "*g")
    *
    * Step1:
    * step1Dataframe = List(
    *   ("b", "graph1", "graph1"),
    *   ("c", "graph1", "graph2")
    * ).toDF("?x", "*l", "*r")
    *
    * Step2:
    * step2Dataframe = List(
    *   ("b", List("graph1", "graph1")),
    *   ("c", List("graph1", "graph2"))
    * ).toDF("?x", "*g")
    *
    * Final:
    * final = List(
    *  ("b", "graph1"),
    *  ("c", ""),
    * ).toDF("?x", "*g")
    *
    * IMPORTANT!: This method could have some performance issues as it traverses entire dataframe, and should be revisited
    * for performance improvements.
    * @param l
    * @param r
    * @return
    */
  def innerJoinWithGraphsColumn(l: DataFrame, r: DataFrame)(implicit
      sc: SQLContext
  ): DataFrame = {
    val leftGraphCol  = "*l"
    val rightGraphCol = "*r"

    val renamedLeft =
      l.as("l").withColumnRenamed(GRAPH_VARIABLE.s, leftGraphCol)
    val renamedRight =
      r.as("r").withColumnRenamed(GRAPH_VARIABLE.s, rightGraphCol)

    val intersect = renamedLeft.columns intersect renamedRight.columns
    val innerJoin = renamedLeft.join(renamedRight, intersect, "inner")

    // Merges the *l and *r columns into a *g column with Array(l, r)
    val innerWithMergedGraphColunns = innerJoin
      .withColumn(GRAPH_VARIABLE.s, array(leftGraphCol, rightGraphCol))
      .drop(leftGraphCol, rightGraphCol)

    // Generates a schema for the final DF (needed for the flatMap)
    val resultSchema = innerWithMergedGraphColunns
      .withColumn(GRAPH_VARIABLE.s, lit(""))
      .schema

    // For each element on the array of *g column if all the graphs are the same we assign the graph
    // if not we assign default graph
    val result = innerWithMergedGraphColunns.map { r =>
      val index  = r.fieldIndex(GRAPH_VARIABLE.s)
      val graphs = r.getSeq[String](index)
      if (graphs.forall(_ == graphs.head)) {
        Row.fromSeq(r.toSeq.dropRight(1) :+ graphs.head)
      } else {
        Row.fromSeq(r.toSeq.dropRight(1) :+ "")
      }
    }(RowEncoder(resultSchema))

    // Needed to keep Schema information on each Row. We will have GenericRowWithSchema instead of GenericRow
    sc.sparkSession.createDataFrame(result.toJavaRDD, resultSchema)
  }

  def empty(implicit sc: SQLContext): Multiset =
    Multiset(Set.empty, sc.sparkSession.emptyDataFrame)

  implicit def semigroup(implicit sc: SQLContext): Semigroup[Multiset] =
    new Semigroup[Multiset] {
      def combine(x: Multiset, y: Multiset): Multiset = x.join(y)
    }

  implicit def monoid(implicit
      sc: SQLContext
  ): Monoid[Multiset] =
    new Monoid[Multiset] {
      def combine(x: Multiset, y: Multiset): Multiset = x.join(y)
      def empty: Multiset                             = Multiset(Set.empty, sc.emptyDataFrame)
    }

}
