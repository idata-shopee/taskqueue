name := "taskqueue"
organization := "io.github.idata-shopee"
version := "0.1.0"
scalaVersion := "2.12.4"

useGpg := true 
parallelExecution in Test := true

publishTo := sonatypePublishTo.value

libraryDependencies ++= Seq(
  // log lib
  "io.github.idata-shopee" %% "klog" % "0.1.0",

  "io.netty" % "netty" % "3.7.0.Final",
  // test suite
  "org.scalatest" %% "scalatest" % "3.0.1" % Test
)
