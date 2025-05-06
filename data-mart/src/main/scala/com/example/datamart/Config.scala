package com.example.datamart

import com.typesafe.config.{ConfigFactory, Config => TSConfig}

case class Config(
  inputPath: String,
  outputPath: String,
  sparkMaster: String
)

object Config {
  private val conf: TSConfig = ConfigFactory.load()

  def load(): Config = Config(
    inputPath   = conf.getString("datamart.inputPath"),
    outputPath  = conf.getString("datamart.outputPath"),
    sparkMaster = conf.getString("datamart.sparkMaster")
  )
}
