package com.gsk.kg.engine.utils

import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher
import org.scalatest.matchers.should.Matchers

trait CustomMatchers extends Matchers {

  def containExactlyOnce[A](right: A): Matcher[List[A]] = {
    new Matcher[List[A]] {
      override def apply(left: List[A]): MatchResult = {
        val count = left.count(_ === right)
        MatchResult(
          count == 1,
          "{0} does not contain exactly once element {1} it contains {2} times",
          "{0} does contain exactly once element {1}",
          Vector(left, right, count),
          Vector(left, right)
        )
      }
    }
  }
}
