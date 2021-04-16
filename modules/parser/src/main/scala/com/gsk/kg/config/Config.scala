package com.gsk.kg.config

final case class Config(
    isDefaultGraphExclusive: Boolean,
    stripQuestionMarksOnOutput: Boolean
)

object Config {
  val default: Config = Config(false, true)
}
