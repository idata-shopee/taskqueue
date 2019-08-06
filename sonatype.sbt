sonatypeProfileName := "io.github.lock-free"

publishMavenStyle := true

licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

import xerial.sbt.Sonatype._
sonatypeProjectHosting := Some(GitHubHosting("lock-free", "taskqueue", "os.lock.free@gmail.com"))

homepage := Some(url("https://github.com/lock-free/taskqueue"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/lock-free/taskqueue"),
    "scm:git@github.com:lock-free/taskqueue.git"
  )
)

developers := List(
  Developer(id="ddchen", name="ddchen", email="chenjunyuwork@gmail.com", url=url("http://lovekino.github.io/"))
)
