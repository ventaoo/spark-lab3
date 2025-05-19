package com.example.datamart

import com.typesafe.config.{ConfigFactory, Config => TSConfig}

case class Config(
  inputPath: String,
  sparkMaster: String,
  dbUrl: String,
  dbUser: String,
  dbPassword: String
)

object Config {
  private val conf: TSConfig = ConfigFactory.load()

  def load(): Config = Config(
    inputPath   = conf.getString("datamart.inputPath"),
    sparkMaster = conf.getString("datamart.sparkMaster"),
    dbUrl       = conf.getString("datamart.dbUrl"),
    dbUser      = conf.getString("datamart.dbUser"),
    dbPassword  = conf.getString("datamart.dbPassword")
  )
}