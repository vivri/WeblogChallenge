package weblog

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.io.Source

/**
  *
  */
class WeblogEntrySpec extends FlatSpec with Matchers with BeforeAndAfter {

  /**
    * Terribly, horribly ad-hoc.
    */
  it should "Correctly parse the demo file" in {
    val lines = Source.fromFile (getClass.getResource("/sample.log").getPath).getLines take(10)

    val condition = lines forall (WeblogEntry.parse(_).isSuccess)

    condition should be (true)

  }
}
