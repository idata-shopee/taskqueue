package io.github.free.lock.taskqueue

import java.util.concurrent.Executors
import scala.concurrent.{ Await, duration }
import java.util.concurrent.{ ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ ExecutionContext, Future }
import duration._

class TaskQueueTest extends org.scalatest.FunSuite {
  val es = Executors.newFixedThreadPool(30)
  val ec = ExecutionContext.fromExecutor(es)

  test("base") {
    val taskQueue = new TaskQueue(10, ExecutionContext.global);
    val task1 = taskQueue.asTask(() => {
      Future { 1 }
    }, 1000 * 1)

    val result = Await.result(task1, Duration.Inf)
    assert(result == 1)
  }

  test("base: 2") {
    val taskQueue = new TaskQueue(10, ExecutionContext.global);
    val task1 = taskQueue.asTask(() => {
      Future { 1 }
    }, 1000 * 1)
    val task2 = taskQueue.asTask(() => {
      Future { 2 }
    }, 1000 * 1)

    val result = Await.result(Future.sequence(List(task1, task2)), Duration.Inf)
    assert(result == List(1, 2))
  }

  test("wait") {
    val taskQueue = new TaskQueue(1, ExecutionContext.global);
    val task1 = taskQueue.asTask(() => {
      Future { 1 }
    }, 1000 * 1)
    val task2 = taskQueue.asTask(() => {
      Future { 2 }
    }, 1000 * 1)

    val result = Await.result(Future.sequence(List(task1, task2)), Duration.Inf)
    assert(result == List(1, 2))
  }

  test("wait async") {
    val taskQueue = new TaskQueue(1, ExecutionContext.global);
    val task1 = taskQueue.asTask(() => {
      Future {
        Thread.sleep(100)
        1
      }
    }, 1000 * 1)
    val task2 = taskQueue.asTask(() => {
      Future {
        Thread.sleep(500)
        2
      }
    }, 1000 * 1)

    val result = Await.result(Future.sequence(List(task1, task2)), Duration.Inf)
    assert(result == List(1, 2))
  }

  test("wait error") {
    val taskQueue = new TaskQueue(1, ExecutionContext.global);
    val task1 = taskQueue.asTask(() => {
      Future {
        throw new Error("123")
      }
    }, 1000 * 1)
    val task2 = taskQueue.asTask(() => {
      Future {
        Thread.sleep(500)
        2
      }
    }, 1000 * 1)

    var flag = false

    val result = Await.result(task1 recover {
      case e: Exception => {
        flag = true
      }
    }, Duration.Inf)

    assert(flag)
  }

  test("mess") {
    val taskQueue = new TaskQueue(5, ExecutionContext.global)
    var tasks     = List[Future[Any]]()

    for (i <- 1 to 1000) {
      tasks = tasks :+ taskQueue.asTask(() => {
        Future {
          Thread.sleep(10)
          1
        }
      }, 10000)
    }

    val result = Await.result(Future.sequence(tasks), Duration.Inf)
    assert(result(0) == 1)
  }

  test("timeout") {
    val taskQueue = new TaskQueue(5, ec)

    val tasks = (1 to 7) map (_ => {
      taskQueue.asTask(() => {
        Future {
          Thread.sleep(1000)
          1
        }
      }, 10) recover {
        case e: Exception => {}
      }
    })

    val task = taskQueue.asTask(() => {
      Future {
        12
      }
    })

    val result = Await.result(task, Duration.Inf)
    assert(result == 12)
    Await.result(Future.sequence(tasks), Duration.Inf)
  }

  test("timeout2") {
    val taskQueue = new TaskQueue(2, ec)

    val tasks = (1 to 3) map { i =>
      taskQueue.asTask(() => {
        Future {
          Thread.sleep(5000)
          1
        }
      }, 1000, s"task-$i") recover {
        case e: Exception => {}
      }
    }

    Thread.sleep(500)
    assert(taskQueue.getRunningTaskInfos().length == 2)
    assert(taskQueue.getWaitingTaskInfos().length == 1)
    assert(taskQueue.getTimeoutRunningTaskInfos().length == 0)

    Thread.sleep(1500)
    assert(taskQueue.getRunningTaskInfos().length == 0)
    assert(taskQueue.getWaitingTaskInfos().length == 0)

    Thread.sleep(5000)
    assert(taskQueue.getWaitingTaskInfos().length == 0)
    assert(taskQueue.getRunningTaskInfos().length == 0)
    assert(taskQueue.getTimeoutRunningTaskInfos().length == 0)
    Await.result(Future.sequence(tasks), Duration.Inf)
  }

  test("timeout3") {
    val taskQueue = new TaskQueue(7, ec)

    val tasks = (1 to 20) map { i =>
      taskQueue.asTask(() => {
        Future {
          Thread.sleep(5000)
          1
        }(ec)
      }, 1000, s"task-$i") recover {
        case e: Exception => {}
      }
    }

    Thread.sleep(500)
    assert(taskQueue.getRunningTaskInfos().length == 7)

    Thread.sleep(1500)
    assert(taskQueue.getRunningTaskInfos().length == 0)

    Await.result(Future.sequence(tasks), Duration.Inf)
  }
}
