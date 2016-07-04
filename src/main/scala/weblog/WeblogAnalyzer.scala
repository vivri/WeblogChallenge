package weblog

import java.io.File
import java.time.{LocalDateTime, ZoneOffset}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import weblog.WeblogEntry.IP

import scala.concurrent.duration._

/**
  * The required analysis results
  */
case class WeblogAnalysisResult(avgSessionTime: FiniteDuration = 0 seconds,
                                uniqueVisits: Map[IP, Int] = Map.empty,
                                orderedByEngagement: Seq[(IP, FiniteDuration)] = Seq.empty)

/**
  * This is an access web log analyzer
  *
  * Accepts an implicit [[SparkContext]] to compute in.
  *
  */
class WeblogAnalyzer(implicit val spark: SparkContext) {

  type TimeWindow = (LocalDateTime, LocalDateTime)

  /**
    * Performs the analysis on the weblog file in a specific [[TimeWindow]] (exclusive)
    * Curries the function for reuse with different time windows.
    */
  def analysis (resourceFile: File) (timeWindow: TimeWindow): WeblogAnalysisResult = withRdd(spark.textFile(resourceFile.getPath).cache(), timeWindow)

  def withRdd (logData: RDD[String], timeWindow: TimeWindow): WeblogAnalysisResult = {
    logData.
      map (WeblogEntry.parse).
      filter (_.isSuccess).
      map {_.get }.
      filter { entry =>
        entry.timestamp.isAfter(timeWindow._1) || entry.timestamp.isBefore(timeWindow._2)
      }.
      groupBy (_.clientAddress._1).
      aggregate[WeblogAnalysisResult] (WeblogAnalysisResult ()) (foldEntriesByIP, amalgamateResults)
  }

  /**
    * Given the next set of entries-by-IP and an aggregated result, fold the entries-by-ip into the aggregate
    */
  val foldEntriesByIP: (WeblogAnalysisResult, (IP, Seq[WeblogEntry])) => WeblogAnalysisResult = (aggregate, entriesByIP) => {

    val sessionLimits: TimeWindow = entriesByIP._2.foldLeft (LocalDateTime.MAX, LocalDateTime.MIN) {(soFar, next) =>
      if (next.timestamp.isBefore(soFar._1))
        (next.timestamp, soFar._2)
      else if (next.timestamp.isAfter(soFar._2))
        (soFar._1, next.timestamp)
      else
        soFar
    }

    val sessionLegthMs = (sessionLimits._2.toEpochSecond(ZoneOffset.UTC) - sessionLimits._1.toEpochSecond(ZoneOffset.UTC)) * 1000

    val IPCount = aggregate.uniqueVisits.keySet.size

    val newSessionTimeAvg = (((aggregate.avgSessionTime.toMillis * IPCount) + sessionLegthMs) / (IPCount + 1)) millis

    val uniqUrls = entriesByIP._2.map (_.request).toSet size

    val ordered = (aggregate.orderedByEngagement :+  (entriesByIP._1, sessionLegthMs.millis)) sortWith (_._2 > _._2)

    WeblogAnalysisResult (
      avgSessionTime = newSessionTimeAvg,
      uniqueVisits = aggregate.uniqueVisits + (entriesByIP._1 -> uniqUrls),
      orderedByEngagement = ordered)
  }

  /**
    * Given two [[WeblogAnalysisResult]]s - merge them into a single [[WeblogAnalysisResult]]
    */
  val amalgamateResults: (WeblogAnalysisResult, WeblogAnalysisResult) => WeblogAnalysisResult = (a, b) => {

    val (ipsA, ipsB) = (a.uniqueVisits.keySet.size, b.uniqueVisits.keySet.size)

    val amalgamatedAvg = (ipsA + ipsB > 0) match {
      case true => ((a.avgSessionTime * ipsA) + (b.avgSessionTime * ipsB)) / (ipsA + ipsB)
      case false => 0 millis
    }

    val amalgamatedOrderedByEgagement = (a.orderedByEngagement ++ b.orderedByEngagement) sortWith (_._2 > _._2)

    WeblogAnalysisResult (
      avgSessionTime = amalgamatedAvg,
      uniqueVisits = a.uniqueVisits ++ b.uniqueVisits,
      orderedByEngagement = amalgamatedOrderedByEgagement
    )
  }

}