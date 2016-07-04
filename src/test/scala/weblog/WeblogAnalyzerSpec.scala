package weblog

import java.time.LocalDateTime

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.io.Source

class WeblogAnalyzerSpec extends FlatSpec with Matchers with BeforeAndAfter {

  //
  // Please not this isn't a real unit test; I just wanted to run it to make sure it'd work (semi-)properly.
  //
  // Ideally, a property-based test would be used to auto-generate weblog entries, create predictions and test outcomes.
  // Less than ideally, a hand-crafted unit test can be created to presumably test all edge-cases.
  //
  it should "Analyze the demo logfile" in {
    val sample = Source.fromFile (getClass.getResource("/sample.log").getPath).getLines take(100) toSeq
    val analyzer = new WeblogAnalyzer

    val window = (LocalDateTime.MIN, LocalDateTime.MAX)

    val result = analyzer.withRdd (sc.makeRDD(sample),  window)

    println (result)
  }

  private val master = "local[2]"
  private val appName = "weblog-spark"

  lazy val conf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)

  implicit var sc: SparkContext = _

  before {
    sc =  new SparkContext(conf)
  }

  after {
    if (sc != null)
      sc.stop
  }



}


