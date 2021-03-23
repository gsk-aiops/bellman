package com.gsk.kg.engine
package data

import cats._
import cats.data.NonEmptyChain

import higherkindness.droste.macros.deriveTraverse

import com.gsk.kg.engine.data.ChunkedList._
import com.gsk.kg.engine.data.ToTree._

import scala.collection.immutable.Nil
import scala.collection.immutable.SortedMap

/** A data structure like a linked [[scala.List]] but in which nodes
  * can be [[ChunkedList.Chunk]]s of elements.
  */
@deriveTraverse trait ChunkedList[A] {

  final def compact[B](f: A => B)(implicit B: Order[B]): ChunkedList[A] =
    ChunkedList.fromChunks(groupBy(f).values.toList)

  def mapChunks[B](fn: Chunk[A] => B): ChunkedList[B] =
    this match {
      case Empty() => Empty()
      case NonEmpty(head, tail) =>
        NonEmpty(Chunk.one(fn(head)), tail.mapChunks(fn))
    }

  def foldLeftChunks[B](z: B)(fn: (B, Chunk[A]) => B): B =
    this match {
      case Empty() => z
      case NonEmpty(head, tail) =>
        tail.foldLeftChunks(fn(z, head))(fn)
    }

  def foldLeft[B](z: B)(f: (B, A) => B): B =
    Foldable[ChunkedList].foldLeft(this, z)(f)

  final def groupBy[B](f: A => B)(implicit
      B: Order[B]
  ): SortedMap[B, NonEmptyChain[A]] =
    groupMap(key = f)(identity)

  final def groupMap[K, B](
      key: A => K
  )(f: A => B)(implicit K: Order[K]): SortedMap[K, NonEmptyChain[B]] = {
    implicit val ordering: Ordering[K] = K.toOrdering

    Foldable[ChunkedList].foldLeft(this, SortedMap.empty[K, NonEmptyChain[B]]) {
      (m, elem) =>
        val k = key(elem)

        m.get(k) match {
          case Some(cat) => m.updated(key = k, value = cat :+ f(elem))
          case None      => m + (k -> NonEmptyChain.one(f(elem)))
        }
    }
  }

  final def reverse: ChunkedList[A] =
    this.foldLeft[ChunkedList[A]](Empty()) { (acc, elem) =>
      NonEmpty(Chunk(elem), acc)
    }

  final def concat(other: ChunkedList[A]): ChunkedList[A] =
    this.reverse.foldLeftChunks[ChunkedList[A]](other) { (acc, elem) =>
      acc match {
        case Empty() => Empty()
        case other   => NonEmpty(elem, other)
      }
    }
}

object ChunkedList {

  type Chunk[A] = NonEmptyChain[A]
  val Chunk = NonEmptyChain

  final case class Empty[A]() extends ChunkedList[A]
  final case class NonEmpty[A](head: Chunk[A], tail: ChunkedList[A])
      extends ChunkedList[A]

  def apply[A](elems: A*): ChunkedList[A] =
    fromList(elems.toList)

  def fromList[A](list: List[A]): ChunkedList[A] =
    list match {
      case Nil => Empty()
      case head :: tl =>
        NonEmpty(Chunk.one(head), fromList(tl))
    }

  def fromChunks[A](chunks: List[NonEmptyChain[A]]): ChunkedList[A] =
    chunks match {
      case Nil        => Empty()
      case head :: tl => NonEmpty(head, fromChunks(tl))
    }

  implicit def toTree[A: ToTree]: ToTree[ChunkedList[A]] =
    new ToTree[ChunkedList[A]] {
      def toTree(t: ChunkedList[A]): TreeRep[String] =
        t match {
          case Empty() => TreeRep.Leaf("ChunkedList.Empty")
          case ne =>
            TreeRep.Node[String](
              "ChunkedList.Node",
              ne
                .mapChunks(_.toTree)
                .foldLeft[Stream[TreeRep[String]]](Stream.empty) {
                  (acc, current) =>
                    current #:: acc
                }
            )
        }
    }
}
