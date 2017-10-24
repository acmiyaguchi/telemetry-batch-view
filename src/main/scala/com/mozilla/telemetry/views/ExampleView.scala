package com.mozilla.telemetry.views

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkContext

import org.rogach.scallop._

object ExampleView {
  private class Conf(args: Array[String]) extends ScallopConf(args) {
    val date = opt[String]("date", required=true)
    val sample_id = opt[String]("sample_id", required=true)
    val bucket = opt[String]("bucket", required=true)
    val prefix = opt[String]("prefix", required=false, default=Some("scala_example/v1"))
    val inbucket = opt[String]("inbucket", required=false, default=Some("telemetry-parquet"))
    val inprefix = opt[String]("inprefix", required=false, default=Some("main_summary/v4"))
    verify()
  }

  def extract(sc: SparkContext, bucket: String, prefix: String): DataFrame = {
    val path = s"s3://$bucket/$prefix";
    println(path)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sqlContext.read.parquet(path)
  }

  def transform(df: DataFrame, date: String, sample_id: String): DataFrame = {
    val aggregate =
      df
        .where(col("submission_date_s3") === date)
        .where(col("sample_id") === sample_id)
        .groupBy(col("normalized_channel"))
        .agg(count("*").alias("pings"))

    aggregate
      .select(
        col("normalized_channel").alias("channel"),
        col("pings")
      )
  }

  def load(df: DataFrame, bucket: String, prefix: String, date: String) {
    val path = s"s3://$bucket/$prefix/submission_date=$date"
    println(path)
    df.write.mode("overwrite").parquet(path)
  }

  def main(args: Array[String]) {
    val conf = new Conf(args)
    val sc = SparkContext.getOrCreate()

    println(s"Starting job for $conf.date() $conf.sample_id()")
    val main_summary = extract(sc, conf.inbucket(), conf.inprefix())
    val pings_per_channel = transform(main_summary, conf.date(), conf.sample_id())
    load(pings_per_channel, conf.bucket(), conf.prefix(), conf.date())
  }
}
