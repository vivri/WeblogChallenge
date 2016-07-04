package weblog

import java.io.File
import java.time.{LocalDateTime, ZoneOffset}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import weblog.WeblogEntry.{HTTPRequest, IP}

import scala.concurrent.duration._

/**
  * The expected analysis results.
  *
  * - [[avgSessionTimeSeconds]] - the average session time of all users, in seconds
  * - [[uniqueVisits]] - Shows the unique URLs visited by the IP; this definition includes the HTTP method, the query, and the HTTP protocol.
  * - [[sortedByEngagement]] - A sequence of IP -> duration, sorted by duration in descending order.
  *
  */
case class WeblogAnalysisResult(avgSessionTimeSeconds: FiniteDuration = 0 seconds,
                                uniqueVisits: Map[IP, Set[HTTPRequest]] = Map.empty,
                                sortedByEngagement: Seq[(IP, FiniteDuration)] = Seq.empty)

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

  /**
    * Perform the same analysis, but instead inject a pre-fabricated [[RDD]]
    */
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
      val min = next.timestamp.isBefore(soFar._1) match {
        case true => next.timestamp
        case false => soFar._1
      }

      val max = next.timestamp.isAfter(soFar._2) match {
        case true => next.timestamp
        case false => soFar._2
      }

      (min, max)
    }

    val sessionLengthSec: Long = (sessionLimits._2.toEpochSecond(ZoneOffset.UTC) - sessionLimits._1.toEpochSecond(ZoneOffset.UTC))

    val IPCount = aggregate.uniqueVisits.keySet.size

    val newSessionTimeAvg = (((aggregate.avgSessionTimeSeconds.toSeconds * IPCount) + sessionLengthSec) / (IPCount + 1)) seconds

    val uniqUrls = entriesByIP._2.map (_.request).toSet

    val ordered = (aggregate.sortedByEngagement :+  (entriesByIP._1, sessionLengthSec.seconds)) sortWith (_._2 > _._2)

    WeblogAnalysisResult (
      avgSessionTimeSeconds = newSessionTimeAvg,
      uniqueVisits = aggregate.uniqueVisits + (entriesByIP._1 -> uniqUrls),
      sortedByEngagement = ordered)
  }

  /**
    * Given two [[WeblogAnalysisResult]]s - merge them into a single [[WeblogAnalysisResult]]
    */
  val amalgamateResults: (WeblogAnalysisResult, WeblogAnalysisResult) => WeblogAnalysisResult = (a, b) => {

    val (ipsA, ipsB) = (a.uniqueVisits.keySet.size, b.uniqueVisits.keySet.size)

    val amalgamatedAvg = (ipsA + ipsB > 0) match {
      case true => ((a.avgSessionTimeSeconds * ipsA) + (b.avgSessionTimeSeconds * ipsB)) / (ipsA + ipsB)
      case false => 0 millis
    }

    val amalgamatedOrderedByEgagement = (a.sortedByEngagement ++ b.sortedByEngagement) sortWith (_._2 > _._2)

    WeblogAnalysisResult (
      avgSessionTimeSeconds = amalgamatedAvg,
      uniqueVisits = a.uniqueVisits ++ b.uniqueVisits,
      sortedByEngagement = amalgamatedOrderedByEgagement
    )
  }

}