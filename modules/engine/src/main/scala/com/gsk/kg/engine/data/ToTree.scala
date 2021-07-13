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
import com.gsk.kg.sparqlparser.PropertyExpression.fixedpoint._

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
          case DAG.Sequence(bps) => Node("Sequence", bps.toStream)
          case DAG.Path(s, p, o, g) =>
            TreeRep.Node(
              s"Path",
              Stream(
                s.s.toTree,
                p.toTree,
                o.s.toTree,
                g.toString().toTree
              )
            )
          case DAG.BGP(quads) => Node("BGP", Stream(quads.toTree))
          case DAG.LeftJoin(l, r, filters) =>
            Node("LeftJoin", Stream(l, r) #::: filters.map(_.toTree).toStream)
          case DAG.Union(l, r) => Node("Union", Stream(l, r))
          case DAG.Minus(l, r) => Node("Minus", Stream(l, r))
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
            val v: List[Expression]               = vars
            val f: List[(Expression, Expression)] = func
            Node("Group", Stream(v.toTree, f.toTree, r))
          case DAG.Order(conds, r) =>
            Node("Order", conds.map(_.toTree).toList.toStream #::: Stream(r))
          case DAG.Distinct(r) => Node("Distinct", Stream(r))
          case DAG.Reduced(r)  => Node("Reduced", Stream(r))
          case DAG.Table(vars, rows) =>
            val v: List[Expression] = vars
            val rs: List[List[(Expression, Expression)]] =
              rows.map(_.tuples.toList)
            Node("Table", Stream(v.toTree, rs.toTree))
          case DAG.Exists(not, p, r) => Node("Exists", Stream(not.toTree, p, r))
          case DAG.Noop(str)         => Leaf(s"Noop($str)")
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
          case ExpressionF.ADD(l, r)      => Node("ADD", Stream(l, r))
          case ExpressionF.SUBTRACT(l, r) => Node("SUBTRACT", Stream(l, r))
          case ExpressionF.MULTIPLY(l, r) => Node("MULTIPLY", Stream(l, r))
          case ExpressionF.DIVIDE(l, r)   => Node("DIVIDE", Stream(l, r))
          case ExpressionF.EQUALS(l, r)   => Node("EQUALS", Stream(l, r))
          case ExpressionF.GT(l, r)       => Node("GT", Stream(l, r))
          case ExpressionF.LT(l, r)       => Node("LT", Stream(l, r))
          case ExpressionF.GTE(l, r)      => Node("GTE", Stream(l, r))
          case ExpressionF.LTE(l, r)      => Node("LTE", Stream(l, r))
          case ExpressionF.OR(l, r)       => Node("OR", Stream(l, r))
          case ExpressionF.AND(l, r)      => Node("AND", Stream(l, r))
          case ExpressionF.NEGATE(s)      => Node("NEGATE", Stream(s))
          case ExpressionF.IN(e, xs)      => Node("IN", Stream(e) #::: xs.toStream)
          case ExpressionF.SAMETERM(l, r) => Node("SAMETERM", Stream(l, r))
          case ExpressionF.IF(cnd, ifTrue, ifFalse) =>
            Node("IF", Stream(cnd, ifTrue, ifFalse))
          case ExpressionF.BOUND(e)     => Node("BOUND", Stream(e))
          case ExpressionF.COALESCE(xs) => Node("COALESCE", xs.toStream)
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
          case ExpressionF.LANGMATCHES(s, range) =>
            Node("LANGMATCHES", Stream(s, Leaf(range.toString)))
          case ExpressionF.LANG(s)      => Node("LANG", Stream(s))
          case ExpressionF.LCASE(s)     => Node("LCASE", Stream(s))
          case ExpressionF.UCASE(s)     => Node("UCASE", Stream(s))
          case ExpressionF.ISLITERAL(s) => Node("ISLITERAL", Stream(s))
          case ExpressionF.CONCAT(appendTo, append) =>
            Node("CONCAT", Stream(appendTo) #::: append.toList.toStream)
          case ExpressionF.STR(s) => Node("STR", Stream(s))
          case ExpressionF.STRAFTER(s, f) =>
            Node("STRAFTER", Stream(s, Leaf(f.toString)))
          case ExpressionF.STRBEFORE(s, f) =>
            Node("STRBEFORE", Stream(s, Leaf(f.toString)))
          case ExpressionF.STRDT(s, uri) =>
            Node("STRDT", Stream(s, Leaf(uri)))
          case ExpressionF.STRLANG(s, tag) =>
            Node("STRLANG", Stream(s, Leaf(tag)))
          case ExpressionF.SUBSTR(s, pos, len) =>
            Node(
              "SUBSTR",
              Stream(s, Leaf(pos.toString), Leaf(len.toString))
            )
          case ExpressionF.STRLEN(s)    => Node("STRLEN", Stream(s))
          case ExpressionF.ISBLANK(s)   => Node("ISBLANK", Stream(s))
          case ExpressionF.ISNUMERIC(s) => Node("ISNUMERIC", Stream(s))
          case ExpressionF.COUNT(e)     => Node("COUNT", Stream(e))
          case ExpressionF.SUM(e)       => Node("SUM", Stream(e))
          case ExpressionF.MIN(e)       => Node("MIN", Stream(e))
          case ExpressionF.MAX(e)       => Node("MAX", Stream(e))
          case ExpressionF.AVG(e)       => Node("AVG", Stream(e))
          case ExpressionF.SAMPLE(e)    => Node("SAMPLE", Stream(e))
          case ExpressionF.GROUP_CONCAT(e, separator) =>
            Node("GROUP_CONCAT", Stream(e, separator.toTree))
          case ExpressionF.ENCODE_FOR_URI(s) =>
            Node("ENCODE_FOR_URI", Stream(s))
          case ExpressionF.MD5(s)    => Node("MD5", Stream(s))
          case ExpressionF.SHA1(s)   => Node("SHA1", Stream(s))
          case ExpressionF.SHA256(s) => Node("SHA256", Stream(s))
          case ExpressionF.SHA384(s) => Node("SHA384", Stream(s))
          case ExpressionF.SHA512(s) => Node("SHA512", Stream(s))
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
          case ExpressionF.UUID()      => Leaf("UUID")
          case ExpressionF.CEIL(e)     => Node(s"CEIL", Stream(e))
          case ExpressionF.ROUND(e)    => Node(s"ROUND", Stream(e))
          case ExpressionF.RAND()      => Leaf("RAND")
          case ExpressionF.ABS(e)      => Node(s"ABS", Stream(e))
          case ExpressionF.FLOOR(e)    => Node(s"FLOOR", Stream(e))
          case ExpressionF.STRUUID()   => Leaf("STRUUID")
          case ExpressionF.NOW()       => Leaf("NOW")
          case ExpressionF.YEAR(e)     => Node(s"YEAR", Stream(e))
          case ExpressionF.MONTH(e)    => Node(s"MONTH", Stream(e))
          case ExpressionF.DAY(e)      => Node(s"DAY", Stream(e))
          case ExpressionF.HOUR(e)     => Node(s"HOUR", Stream(e))
          case ExpressionF.MINUTES(e)  => Node(s"MINUTES", Stream(e))
          case ExpressionF.SECONDS(e)  => Node(s"SECONDS", Stream(e))
          case ExpressionF.TIMEZONE(e) => Node(s"TIMEZONE", Stream(e))
        }

        val t = scheme.cata(alg)

        t(tree)
      }

    }
  // scalastyle:on

  implicit def propExrpToTree[T: Basis[PropertyExpressionF, *]]: ToTree[T] =
    new ToTree[T] {
      def toTree(tree: T): TreeRep[String] = {
        import TreeRep._
        val alg = Algebra[PropertyExpressionF, TreeRep[String]] {
          case AlternativeF(pel, per) =>
            Node("Alternative", Stream(pel, per))
          case ReverseF(e) => Node("Reverse", Stream(e))
          case SeqExpressionF(pel, per) =>
            Node("SeqExpression", Stream(pel, per))
          case OneOrMoreF(e) => Node("OneOrMore", Stream(e))
          case ZeroOrMoreF(e) =>
            Node("ZeroOrMore", Stream(e))
          case ZeroOrOneF(e) => Node("ZeroOrOne", Stream(e))
          case NotOneOfF(es) =>
            Node("NotOnOf", es.toStream)
          case BetweenNAndMF(n, m, e) =>
            Node("BetweenNAndM", Stream(Leaf(n.toString), Leaf(m.toString), e))
          case ExactlyNF(n, e) =>
            Node("ExactlyN", Stream(Leaf(n.toString), e))
          case NOrMoreF(n, e) =>
            Node("NOrMore", Stream(Leaf(n.toString), e))
          case BetweenZeroAndNF(n, e) =>
            Node("BetweenZeroAndN", Stream(Leaf(n.toString), e))
          case UriF(s) => Node("Uri", Stream(Leaf(s)))
        }

        val t = scheme.cata[PropertyExpressionF, T, TreeRep[String]](alg)

        t(tree)
      }
    }

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
