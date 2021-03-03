package com.gsk.kg.engine.data

import cats.instances.function._
import cats.free.Trampoline
import cats.Show
import cats.Eval

/**
  * A generic tree representation.  It's particularly useful for its [[draw]] method.
  *
  * ported from scalaz.Tree
  */
sealed trait TreeRep[A] {

  import TreeRep._

  /** The label at the root of this tree. */
  def rootLabel: A

  /** The child nodes of this tree. */
  def subForest: Stream[TreeRep[A]]

  def drawTree(implicit ev: Show[A]): String = {
    val reversedLines: Vector[StringBuilder] = draw.run
    val first = new StringBuilder(reversedLines.head.toString.reverse)
    val rest = reversedLines.tail
    rest.foldLeft(first) { (acc, elem) =>
      acc.append("\n").append(elem.toString.reverse)
    }.append("\n").toString
  }

  /** A 2D String representation of this Tree, separated into lines.
    * Uses reversed StringBuilders for performance, because they are
    * prepended to.
    **/
  private def draw(implicit ev: Show[A]): Trampoline[Vector[StringBuilder]] = {
    import Trampoline._
    val branch = " -+" // "+- ".reverse
    val stem = " -`" // "`- ".reverse
    val trunk = "  |" // "|  ".reverse

    def drawSubTrees(s: Stream[TreeRep[A]]): Trampoline[Vector[StringBuilder]] = s match {
      case ts if ts.isEmpty       =>
        done(Vector.empty[StringBuilder])
      case t #:: ts if ts.isEmpty =>
        suspend(t.draw).map(subtree => new StringBuilder("|") +: shift(stem, "   ", subtree))
      case t #:: ts               => for {
                                       subtree <- suspend(t.draw)
                                       otherSubtrees <- suspend(drawSubTrees(ts))
                                     } yield new StringBuilder("|") +: (shift(branch, trunk, subtree) ++ otherSubtrees)
    }

    def shift(first: String, other: String, s: Vector[StringBuilder]): Vector[StringBuilder] = {
      var i = 0
      while (i < s.length) {
        if (i == 0) {
          s(i).append(first)
        } else {
          s(i).append(other)
        }
        i += 1
      }
      s
    }

    drawSubTrees(subForest).map { subtrees =>
      new StringBuilder(ev.show(rootLabel).reverse) +: subtrees
    }
  }
}

object TreeRep {


  final case class Node[A](root: A, forest: Stream[TreeRep[A]]) extends TreeRep[A] {

    override def rootLabel: A = root

    override def subForest: Stream[TreeRep[A]] = forest

  }

  final case class Leaf[A](root: A) extends TreeRep[A] {

    override def rootLabel: A = root

    override def subForest: Stream[TreeRep[A]] = Stream.empty

  }


}
