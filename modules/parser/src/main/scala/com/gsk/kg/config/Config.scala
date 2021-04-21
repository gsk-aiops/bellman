package com.gsk.kg.config

final case class Config(
    isDefaultGraphExclusive: Boolean,
    stripQuestionMarksOnOutput: Boolean,
    formatRdfOutput: Boolean
)

object Config {
  val default: Config = Config(true, false, false)
}
