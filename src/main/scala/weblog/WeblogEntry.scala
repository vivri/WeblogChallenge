package weblog

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import weblog.WeblogEntry._

import scala.concurrent.duration._
import scala.util.Try


/**
  * Represents a single access log entry
  * See: http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format
  */
case class WeblogEntry (timestamp: LocalDateTime,
                        elb: String,
                        clientAddress:  IPAndPort,
                        backendAddress: IPAndPort,
                        reqProcTime: FiniteDuration,
                        backendProcTime: FiniteDuration,
                        respProcTime: FiniteDuration,
                        elbStatusCode: HTTPStatus,
                        backendStatusCode: HTTPStatus,
                        receivedBytes: Long,
                        sentBytes: Long,
                        request: HTTPRequest,
                        userAgent: UserAgent,
                        sslCipher: String,
                        sslProtocol: String)


object WeblogEntry {
  type IP = String
  type Port = Int
  type IPAndPort = (IP, Port)
  type HTTPMethod = String
  type HTTPReqStr = String
  type HTTPStatus = Int
  type HTTPProtocol = String
  type HTTPRequest = (HTTPMethod, HTTPReqStr, HTTPProtocol)
  type UserAgent = String
  type SSLCipher = String
  type SSLProtocol = String

  val weblogRegex = """^(\S+) (\S+) (\S+?):([0-9]+) (\S+?):([0-9]+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) "([^" ]+) ([^" ]+) ([^" ]+)" "([^"]+)" (\S+) (\S+)$""".r

  /**
    * Attempt to convert a string to a [[WeblogEntry]]
    *
    */
  def parse (raw: String): Try[WeblogEntry] = Try {
    raw match {
      case weblogRegex(tm, el, caip, cap, baip, bap, rqpt, bpt, rspt, esc, bsc, rb, sb, rmeth, rbod, rprot, ua, sc, sp) =>
        new WeblogEntry (
         timestamp = LocalDateTime.parse(tm,DateTimeFormatter.ISO_INSTANT),
          elb = el,
          clientAddress = (caip, cap toInt),
          backendAddress = (baip, bap toInt),
          reqProcTime = rqpt.toInt seconds,
          backendProcTime = bpt.toInt seconds,
          respProcTime = rspt.toInt seconds,
          elbStatusCode = esc toInt,
          backendStatusCode = bsc toInt,
          receivedBytes = rb toLong,
          sentBytes = sb toLong,
          request = (rmeth, rbod, rprot),
          userAgent = ua,
          sslCipher = sc,
          sslProtocol = sp
        )
      case _ => throw new IllegalArgumentException (s"Line doesn't conform to weblog standard: $raw")
    }
  } recover {
    case x => println (x.getMessage); throw x
  }
}