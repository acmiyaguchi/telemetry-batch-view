/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.experiments.analyzers

import com.mozilla.telemetry.metrics.HistogramDefinition
import org.apache.spark.sql._

import scala.collection.Map


case class HistogramRow(experiment_id: String, branch: String, subgroup: String, metric: Option[Map[Int, Int]],
                        block_id: Int) {
  def toPreAggregateRow: PreAggHistogramRow = {
    import HistogramAnalyzer._
    PreAggHistogramRow(experiment_id, branch, subgroup, metric.toLongValues, block_id)
  }
}

case class KeyedHistogramRow(experiment_id: String, branch: String, subgroup: String,
                             metric: Option[Map[String, Map[Int, Int]]], block_id: Int) {
  def toPreAggregateRow: PreAggHistogramRow = {
    import HistogramAnalyzer._
    try {
      PreAggHistogramRow(experiment_id, branch, subgroup, metric.collapse.toLongValues, block_id)
    } catch {
      case _: java.lang.NullPointerException => PreAggHistogramRow(experiment_id, branch, subgroup, None, block_id)
    }
  }
}

case class PreAggHistogramRow(experiment_id: String, branch: String, subgroup: String, metric: Option[Map[Int, Long]], block_id: Int)
extends PreAggregateRow[Int]

class HistogramAnalyzer(name: String, hd: HistogramDefinition, df: DataFrame, numJackknifeBlocks: Int)
  extends MetricAnalyzer[Int](name, hd, df, numJackknifeBlocks) {
  override type PreAggregateRowType = PreAggHistogramRow
  override val groupAggregator = GroupUintAggregator
  override val finalAggregator = UintAggregator
  val buckets = hd.getBuckets

  // Checks that 1. all the buckets keys are expected values and 2. bucket values are positive numbers
  def validateRow(row: PreAggregateRowType): Boolean = {
    row.metric match {
      case Some(m: Map[Int, Long]) =>
        (m.keys.toSet subsetOf buckets.toSet) && m.values.forall(_ >= 0L)
      case _ => false
    }
  }

  def collapseKeys(formatted: DataFrame): Dataset[PreAggHistogramRow] = {
    import df.sparkSession.implicits._
    if (hd.keyed) {
      formatted.as[KeyedHistogramRow].map(_.toPreAggregateRow)
    } else {
      formatted.as[HistogramRow].map(_.toPreAggregateRow)
    }
  }
}

object HistogramAnalyzer {
  implicit class HistogramMetric(val metric: Option[Map[Int, Int]]) {
    def toLongValues: Option[Map[Int, Long]] = metric match {
      case Some(m) => Some(m.map { case(k: Int, v: Int) => k -> v.toLong })
      case _ => None
    }
  }

  implicit class KeyedHistogramMetric(val metric: Option[Map[String, Map[Int, Int]]]) {
    private def sumKeys(l: Map[Int, Int], r: Map[Int, Int]): Map[Int, Int] = {
      l ++ r.map { case (k, v) => k -> (v + l.getOrElse(k, 0)) }
    }
    def collapse: Option[Map[Int, Int]] = metric match {
      case Some(m) => Some(m.values.reduce(sumKeys))
      case _ => None
    }
  }
}
