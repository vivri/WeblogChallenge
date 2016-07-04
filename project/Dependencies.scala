import sbt._

object Version {
  val spark        = "0.9.1"
  val hadoop       = "2.4.0"
  val logback      = "1.1.1"
  val mockito      = "1.9.5"
  val scalatest    = "3.0.0-M16-SNAP6"
}

object Library {
  val sparkCore      = "org.apache.spark"  %% "spark-core"      % Version.spark
  val hadoopClient   = "org.apache.hadoop" %  "hadoop-client"   % Version.hadoop
  val logbackClassic = "ch.qos.logback"    %  "logback-classic" % Version.logback
  val mockitoAll     = "org.mockito"       %  "mockito-all"     % Version.mockito
  val scalatest      = "org.scalatest"     %% "scalatest"       % Version.scalatest
}

object Dependencies {

  import Library._

  val sparkHadoop = Seq(
    sparkCore,
    hadoopClient,
    logbackClassic % "test",
    scalatest % "test",
    mockitoAll % "test"
  )
}