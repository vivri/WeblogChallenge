package weblog

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.io.Source

class WeblogEntrySpec extends FlatSpec with Matchers with BeforeAndAfter {

  /**
    * Very ad-hoc; Instead, I'd create a line generator in a property-based test, and would've given it a decent
    */
  it should "Correctly parse the demo file" in {
    val lines = Source.fromFile (getClass.getResource("/sample.log").getPath).getLines take(100)

    val condition = lines forall (WeblogEntry.parse(_).isSuccess)

    condition should be (true)
  }
}
