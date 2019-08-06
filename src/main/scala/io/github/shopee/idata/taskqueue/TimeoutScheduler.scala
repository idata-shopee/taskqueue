package io.github.free.lock.taskqueue

import org.jboss.netty.util.{ HashedWheelTimer, Timeout, TimerTask }
import java.util.concurrent.TimeUnit
import scala.concurrent.Promise
import java.util.concurrent.TimeoutException
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration._

object TimeoutScheduler {
  val timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS)

  private def scheduleTimeout(promise: Promise[_], after: Duration) =
    timer.newTimeout(
      new TimerTask {
        def run(timeout: Timeout) {
          promise.failure(
            new TimeoutException("Operation timed out after " + after.toMillis + " millis")
          )
        }
      },
      after.toNanos,
      TimeUnit.NANOSECONDS
    )

  // after: milliseconds
  def sleep(after: Int): Future[Unit] = {

    val promise = Promise[Unit]()

    timer.newTimeout(
      new TimerTask {
        def run(timeout: Timeout) {
          promise.success()
        }
      },
      Duration(after, MILLISECONDS).toNanos,
      TimeUnit.NANOSECONDS
    )

    promise.future
  }

  def withTimeout[T](fut: Future[T], after: Duration)(implicit ec: ExecutionContext) = {
    val prom        = Promise[T]()
    val timeout     = scheduleTimeout(prom, after)
    val combinedFut = Future.firstCompletedOf(List(fut, prom.future))
    fut onComplete { case result => timeout.cancel() }
    combinedFut
  }
}
