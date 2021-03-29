package com.gsk.kg.engine
package data

import cats._
import cats.syntax.eq._
import cats.data.NonEmptyChain

import higherkindness.droste.macros.deriveTraverse

import com.gsk.kg.engine.data.ChunkedList._
import com.gsk.kg.engine.data.ToTree._

import scala.collection.immutable.Nil

/** A data structure like a linked [[scala.List]] but in which nodes
  * can be [[ChunkedList.Chunk]]s of elements.
  */
@deriveTraverse trait ChunkedList[A] {

  def compact[B](f: A => B)(implicit B: Eq[B]): ChunkedList[A] =
    foldLeft((ChunkedList.empty[A], Option.empty[Chunk[A]])) {
      case ((list, Some(chunk)), current) =>
        if (f(chunk.head) === f(current)) {
          (list, Some(chunk.append(current)))
        } else {
          (list.appendChunk(chunk), Some(Chunk.one(current)))
        }
      case ((list, None), current) =>
        (list, Some(Chunk.one(current)))
    } match {
	    case (list, None) => list
	    case (list, Some(chunk)) => list.appendChunk(chunk)
    }

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

  final def appendChunk(chunk: Chunk[A]): ChunkedList[A] =
    this match {
	    case Empty() => NonEmpty(chunk, Empty())
      case NonEmpty(head, tail) => NonEmpty(head, tail.appendChunk(chunk))
    }
}

object ChunkedList {

  type Chunk[A] = NonEmptyChain[A]
  val Chunk = NonEmptyChain

  final case class Empty[A]() extends ChunkedList[A]
  final case class NonEmpty[A](head: Chunk[A], tail: ChunkedList[A])
      extends ChunkedList[A]

  def empty[A]: ChunkedList[A] = Empty[A]()

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
