package com.gsk.kg.engine
package data

import cats.Show
import cats.data.NonEmptyChain
import cats.data.NonEmptyList
import cats.implicits._

import higherkindness.droste.Algebra
import higherkindness.droste.Basis
import higherkindness.droste.scheme

import com.gsk.kg.sparqlparser.ConditionOrder
import com.gsk.kg.sparqlparser.ConditionOrder.ASC
import com.gsk.kg.sparqlparser.ConditionOrder.DESC
import com.gsk.kg.sparqlparser.Expr
import com.gsk.kg.sparqlparser.Expression

import scala.collection.immutable.Nil

/** Typeclass that allows you converting values of type T to
  * [[TreeRep]].  The benefit of doing so is that we'll be able to
  * render them nicely wit the drawTree method.
  */
trait ToTree[T] {
  def toTree(t: T): TreeRep[String]
}

object ToTree extends LowPriorityToTreeInstances0 {

  def apply[T](implicit T: ToTree[T]): ToTree[T] = T

  implicit class ToTreeOps[T](private val t: T)(implicit T: ToTree[T]) {
    def toTree: TreeRep[String] = ToTree[T].toTree(t)
  }

  implicit val quadToTree: ToTree[Expr.Quad] = new ToTree[Expr.Quad] {
    def toTree(t: Expr.Quad): TreeRep[String] =
      TreeRep.Node(
        s"Quad",
        Stream(
          t.s.s.toTree,
          t.p.s.toTree,
          t.o.s.toTree,
          t.g.toString().toTree
        )
      )
  }

  implicit val conditionOrderToTree: ToTree[ConditionOrder] =
    new ToTree[ConditionOrder] {
      override def toTree(t: ConditionOrder): TreeRep[String] = t match {
        case ASC(e)  => TreeRep.Node("Asc", Stream(e.toTree))
        case DESC(e) => TreeRep.Node("Desc", Stream(e.toTree))
      }
    }

  // scalastyle:off
  implicit def dagToTree[T: Basis[DAG, *]]: ToTree[T] =
    new ToTree[T] {
      def toTree(tree: T): TreeRep[String] = {
        import TreeRep._
        val alg = Algebra[DAG, TreeRep[String]] {
          case DAG.Describe(vars, r) =>
            Node("Describe", vars.map(_.s.toTree).toStream #::: Stream(r))
          case DAG.Ask(r) => Node("Ask", Stream(r))
          case DAG.Construct(bgp, r) =>
            Node(
              "Construct",
              Stream(DAG.fromExpr.apply(bgp.asInstanceOf[Expr]).toTree, r)
            )
          case DAG.Scan(graph, expr) => Node("Scan", Stream(graph.toTree, expr))
          case DAG.Project(variables, r) =>
            val v: List[Expression] = variables
            Node("Project", Stream(v.toTree, r))
          case DAG.Bind(variable, expression, r) =>
            Node(
              "Bind",
              Stream(Leaf(variable.toString), expression.toTree, r)
            )
          case DAG.BGP(quads) => Node("BGP", Stream(quads.toTree))
          case DAG.LeftJoin(l, r, filters) =>
            Node("LeftJoin", Stream(l, r) #::: filters.map(_.toTree).toStream)
          case DAG.Union(l, r) => Node("Union", Stream(l, r))
          case DAG.Filter(funcs, expr) =>
            Node(
              "Filter",
              funcs.map(_.toTree).toList.toStream #::: Stream(expr)
            )
          case DAG.Join(l, r) => Node("Join", Stream(l, r))
          case DAG.Offset(offset, r) =>
            Node(
              "Offset",
              Stream(offset.toTree, r)
            )
          case DAG.Limit(limit, r) =>
            Node(
              "Limit",
              Stream(limit.toTree, r)
            )
          case DAG.Group(vars, func, r) =>
            val v: List[Expression]                 = vars
            val f: Option[(Expression, Expression)] = func
            Node("Group", Stream(v.toTree, f.toTree, r))
          case DAG.Order(conds, r) =>
            Node("Order", conds.map(_.toTree).toList.toStream #::: Stream(r))
          case DAG.Distinct(r) => Node("Distinct", Stream(r))
          case DAG.Noop(str)   => Leaf(s"Noop($str)")
        }

        val t = scheme.cata(alg)

        t(tree)
      }
    }
  // scalastyle:on

  // scalastyle:off
  implicit def expressionfToTree[T: Basis[ExpressionF, *]]: ToTree[T] =
    new ToTree[T] {
      def toTree(tree: T): TreeRep[String] = {
        import TreeRep._
        val alg = Algebra[ExpressionF, TreeRep[String]] {
          case ExpressionF.EQUALS(l, r) => Node("EQUALS", Stream(l, r))
          case ExpressionF.GT(l, r)     => Node("GT", Stream(l, r))
          case ExpressionF.LT(l, r)     => Node("LT", Stream(l, r))
          case ExpressionF.GTE(l, r)    => Node("GTE", Stream(l, r))
          case ExpressionF.LTE(l, r)    => Node("LTE", Stream(l, r))
          case ExpressionF.OR(l, r)     => Node("OR", Stream(l, r))
          case ExpressionF.AND(l, r)    => Node("AND", Stream(l, r))
          case ExpressionF.NEGATE(s)    => Node("NEGATE", Stream(s))
          case ExpressionF.REGEX(s, pattern, flags) =>
            Node(
              "REGEX",
              Stream(s, Leaf(pattern.toString), Leaf(flags.toString))
            )
          case ExpressionF.REPLACE(st, pattern, by, flags) =>
            Node("REPLACE", Stream(st, Leaf(pattern), Leaf(by), Leaf(flags)))
          case ExpressionF.STRENDS(s, f) =>
            Node("STRENDS", Stream(s, Leaf(f.toString)))
          case ExpressionF.STRSTARTS(s, f) =>
            Node("STRSTARTS", Stream(s, Leaf(f.toString)))
          case ExpressionF.URI(s) => Node("URI", Stream(s))
          case ExpressionF.CONCAT(appendTo, append) =>
            Node("CONCAT", Stream(appendTo) #::: append.toList.toStream)
          case ExpressionF.STR(s) => Node("STR", Stream(s))
          case ExpressionF.STRAFTER(s, f) =>
            Node("STRAFTER", Stream(s, Leaf(f.toString)))
          case ExpressionF.STRBEFORE(s, f) =>
            Node("STRBEFORE", Stream(s, Leaf(f.toString)))
          case ExpressionF.STRDT(s, uri) =>
            Node("STRDT", Stream(s, Leaf(uri)))
          case ExpressionF.SUBSTR(s, pos, len) =>
            Node(
              "SUBSTR",
              Stream(s, Leaf(pos.toString), Leaf(len.toString))
            )
          case ExpressionF.STRLEN(s)  => Node("STRLEN", Stream(s))
          case ExpressionF.ISBLANK(s) => Node("ISBLANK", Stream(s))
          case ExpressionF.COUNT(e)   => Node("COUNT", Stream(e))
          case ExpressionF.SUM(e)     => Node("SUM", Stream(e))
          case ExpressionF.MIN(e)     => Node("MIN", Stream(e))
          case ExpressionF.MAX(e)     => Node("MAX", Stream(e))
          case ExpressionF.AVG(e)     => Node("AVG", Stream(e))
          case ExpressionF.SAMPLE(e)  => Node("SAMPLE", Stream(e))
          case ExpressionF.GROUP_CONCAT(e, separator) =>
            Node("GROUP_CONCAT", Stream(e, separator.toTree))
          case ExpressionF.STRING(s) =>
            Leaf(s"STRING($s)")
          case ExpressionF.DT_STRING(s, tag) =>
            Leaf(s"DT_STRING($s, $tag)")
          case ExpressionF.LANG_STRING(s, tag) =>
            Leaf(s"LANG_STRING($s, $tag")
          case ExpressionF.NUM(s)      => Leaf(s"NUM($s)")
          case ExpressionF.VARIABLE(s) => Leaf(s"VARIABLE($s)")
          case ExpressionF.URIVAL(s)   => Leaf(s"URIVAL($s)")
          case ExpressionF.BLANK(s)    => Leaf(s"BLANK($s)")
          case ExpressionF.BOOL(s)     => Leaf(s"BOOL($s)")
          case ExpressionF.ASC(e)      => Node(s"ASC", Stream(e))
          case ExpressionF.DESC(e)     => Node(s"DESC", Stream(e))
        }

        val t = scheme.cata(alg)

        t(tree)
      }

    }
  // scalastyle:on

  implicit def listToTree[A: ToTree]: ToTree[List[A]] =
    new ToTree[List[A]] {
      def toTree(t: List[A]): TreeRep[String] =
        t match {
          case Nil => TreeRep.Leaf("List.empty")
          case nonempty =>
            TreeRep.Node("List", nonempty.map(_.toTree).toStream)
        }
    }

  implicit def nelToTree[A: ToTree]: ToTree[NonEmptyList[A]] =
    new ToTree[NonEmptyList[A]] {
      def toTree(t: NonEmptyList[A]): TreeRep[String] =
        TreeRep.Node("NonEmptyList", t.map(_.toTree).toList.toStream)
    }

  implicit def necToTree[A: ToTree]: ToTree[NonEmptyChain[A]] =
    new ToTree[NonEmptyChain[A]] {
      def toTree(t: NonEmptyChain[A]): TreeRep[String] =
        TreeRep.Node("NonEmptyChain", t.map(_.toTree).toList.toStream)
    }

  implicit def tupleToTree[A: ToTree, B: ToTree]: ToTree[(A, B)] =
    new ToTree[(A, B)] {
      def toTree(t: (A, B)): TreeRep[String] =
        TreeRep.Node(
          "Tuple",
          Stream(t._1.toTree, t._2.toTree)
        )
    }

  implicit def optionToTree[A: ToTree]: ToTree[Option[A]] =
    new ToTree[Option[A]] {
      def toTree(t: Option[A]): TreeRep[String] =
        t.fold[TreeRep[String]](TreeRep.Leaf("Node"))(a =>
          TreeRep.Node("Some", Stream(a.toTree))
        )
    }

}

trait LowPriorityToTreeInstances0 {

  // Instance of ToTree for anything that has a Show
  implicit def showToTree[T: Show]: ToTree[T] =
    new ToTree[T] {
      def toTree(t: T): TreeRep[String] = TreeRep.Leaf(Show[T].show(t))
    }

}
