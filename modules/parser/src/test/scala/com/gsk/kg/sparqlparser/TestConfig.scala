package com.gsk.kg.sparqlparser

import com.gsk.kg.config.Config

trait TestConfig {

  import pureconfig._
  import pureconfig.generic.auto._

  private lazy val stringConf =
    """
      |{
      | is-default-graph-exclusive = true,
      | strip-question-marks-on-output = false
      |}
      |""".stripMargin

  implicit lazy val config: Config =
    ConfigSource.string(stringConf).loadOrThrow[Config]
}
