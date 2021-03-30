package com.gsk.kg.engine.utils

import com.gsk.kg.engine.Multiset

import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher
import org.scalatest.matchers.should.Matchers

trait MultisetMatchers extends Matchers {

  def equalsMultiset(right: Multiset): Matcher[Multiset] =
    equalsBindings(right) and equalsDataframe(right)

  def equalsBindings(right: Multiset): Matcher[Multiset] =
    new Matcher[Multiset] {
      override def apply(left: Multiset): MatchResult = MatchResult(
        left.bindings === right.bindings,
        "{0} bindings are different than {1}",
        "{0} bindings are equal as {1}",
        Vector(left.bindings, right.bindings),
        Vector(left.bindings, right.bindings)
      )
    }

  def equalsDataframe(right: Multiset): Matcher[Multiset] =
    new Matcher[Multiset] {
      override def apply(left: Multiset): MatchResult = {
        lazy val leftCollected  = left.dataframe.collect().map(_.toSeq.toSet)
        lazy val rightCollected = right.dataframe.collect().map(_.toSeq.toSet)
        MatchResult(
          leftCollected === rightCollected,
          "{0} dataframes are different {1}",
          "{0} dataframes are equal {1}",
          Vector(leftCollected, rightCollected),
          Vector(leftCollected, rightCollected)
        )
      }
    }
}
