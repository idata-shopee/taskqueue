package io.github.shopee.idata.taskqueue

import io.github.shopee.idata.klog.KLog
import java.util.UUID.randomUUID
import java.util.concurrent.TimeoutException
import scala.collection.mutable.SynchronizedQueue
import scala.concurrent.{ ExecutionContext, Future, Promise }
import java.util.concurrent.ConcurrentHashMap
import scala.collection._
import scala.concurrent.duration._
import scala.collection.convert.decorateAsScala._
import scala.annotation.tailrec

/**
  * Task queue
  */
/**
  *
  * @param status running
  */
object Task {
  val STATUS_WAITING: Int         = 0
  val STATUS_RUNNING: Int         = 1
  val STATUS_RESOLVED: Int        = 2
  val STATUS_TIMEOUT_RUNNING: Int = 3
  val STATUS_ERRORED: Int         = 4
}

case class Task(id: String, // unique task id
                name: String, // task name
                fun: () => Future[Any], // task function, only run once
                createTime: Long,
                expireTime: Long,
                promise: Promise[TaskResult], // trace task state
                var startRunTime: Long = 0,
                var status: Int = Task.STATUS_WAITING,
                description: String = "") {

  /**
    * update task info in a sync way
    */
  def onTaskSync[T](fn: () => T): T = synchronized {
    fn()
  }

  def run()(implicit ec: ExecutionContext): Future[Any] =
    (try {
      fun() map { result =>
        promise trySuccess TaskResult(0, result)
      } recover {
        case e: Exception => promise trySuccess TaskResult(500, e)
      }
    } catch {
      case e: Exception => {
        Future { promise trySuccess TaskResult(500, e) }
      }
    }) map { ret =>
      // change the task status
      onTaskSync(() => {
        status = Task.STATUS_RESOLVED
      })
      ret
    } recover {
      case ex: Exception => {
        onTaskSync(() => {
          status = Task.STATUS_ERRORED
        })
        throw ex
      }
    }

  def getTaskInfo() =
    TaskInfo(
      id,
      name = name,
      createTime = createTime,
      expireTime = expireTime,
      startRunTime = startRunTime,
      status = status,
      description = description
    )

  def getTaskDes() = s"${name}_${id}_${description}"
}

case class TaskInfo(id: String,
                    name: String,
                    createTime: Long,
                    expireTime: Long,
                    startRunTime: Long,
                    status: Int,
                    description: String)

case class TaskResult(errType: Int, data: Any)

/**
  * a general way to control concurrency
  */
class TaskQueue(concurrentLimit: Int, executor: ExecutionContext) {
  implicit val ec = executor

  private val waitingTaskQ        = new SynchronizedQueue[Task]()
  private val runningTasks        = new ConcurrentHashMap[String, Task]().asScala
  private val timeoutRunningTasks = new ConcurrentHashMap[String, Task]().asScala

  /**
    * add a new task and wait for the result of the task
    */
  def asTask(taskFun: () => Future[Any],
             expireTime: Long = 1000 * 60 * 5, // default 5 minutes
             taskName: String = "",
             description: String = ""): Future[Any] = {
    val p  = Promise[TaskResult]
    val id = randomUUID().toString
    // create a new task
    val task = Task(id,
                    name = taskName,
                    fun = taskFun,
                    createTime = System.currentTimeMillis(),
                    expireTime = expireTime,
                    promise = p,
                    description = description)
    // push new task to queue
    enqueueTask(task)

    // timeout
    TimeoutScheduler.withTimeout(p.future, expireTime / 1000 seconds) recover {
      case e: TimeoutException =>
        timeoutHandler(task)
        throw e
    } map { data =>
      handleTaskResult(task, data)
    }
  }

  private def timeoutHandler(task: Task) = synchronized {
    task.onTaskSync(() => {
      if (task.status == Task.STATUS_WAITING) {
        task.status = Task.STATUS_ERRORED // timeout already
      } else {
        // change the state at first
        if (task.status == Task.STATUS_RUNNING) {
          task.status = Task.STATUS_TIMEOUT_RUNNING
          timeoutRunningTasks(task.id) = task
        }
      }

      runningTasks.remove(task.id)
      KLog.info("task-timeout", s"${getBrief(task)}")
    })

    tryTask()
  }

  private def enqueueTask(_task: Task) = {
    // push task to waiting Q
    waitingTaskQ.enqueue(_task)
    KLog.info("task-enqueue", s"${getBrief(_task)}")
    // try to run a task
    tryTask()
  }

  private def tryTask(): Unit =
    getTaskToRun() match {
      case Some(task) => {
        task.onTaskSync(() => {
          if (task.status == Task.STATUS_RUNNING) {
            KLog.info("task-run", s"${getBrief(task)}")
            // run task
            task.run() map { _ =>
              runningTasks.remove(task.id)
              timeoutRunningTasks.remove(task.id)

              var currentTime = System.currentTimeMillis()
              KLog.info(
                "task-done",
                s"${getBrief(task)}, waiting time: ${task.startRunTime - task.createTime}, runing time: ${currentTime - task.startRunTime}ms."
              )

              tryTask()
            } recover {
              case ex: Exception => {
                runningTasks.remove(task.id)
                timeoutRunningTasks.remove(task.id)

                KLog.logErr(s"task-done-with-error-${getBrief(task)}", ex)
                tryTask()
                throw ex
              }
            }
          } else {
            KLog.info("task-run-outofdate", s"${getBrief(task)}")
            tryTask()
          }
        })
      }

      case None => {}
    }

  private def getTaskToRun() = synchronized {
    if (runningTasks.size < concurrentLimit && waitingTaskQ.size > 0) {
      val task = waitingTaskQ.dequeue()
      task.onTaskSync(() => {
        if (task.status == Task.STATUS_WAITING) { // could be timeout task, just ignore
          // add to running tasks
          runningTasks(task.id) = task
          task.status = Task.STATUS_RUNNING // change the status
          task.startRunTime = System.currentTimeMillis()
        }
      })
      Some(task)
    } else {
      None
    }
  }

  private def handleTaskResult(task: Task, taskResult: TaskResult) =
    if (taskResult.errType == 0) {
      val currentTime = System.currentTimeMillis()
      KLog.info(
        "task-finish",
        s"${getBrief(task)}, waiting time: ${task.startRunTime - task.createTime}, runing time: ${currentTime - task.startRunTime}ms."
      )
      taskResult.data
    } else {
      KLog.logErr(s"task-error-${getBrief(task)}", taskResult.data.asInstanceOf[Exception])
      throw taskResult.data.asInstanceOf[Exception]
    }

  def getBrief(task: Task): String =
    s"(${runningTasks.size},${waitingTaskQ.size},${timeoutRunningTasks.size}) ${task.getTaskDes()}"

  def getTaskQueueInfo() =
    Map[String, List[TaskInfo]](
      "waitingTaskQueue"    -> getWaitingTaskInfos(),
      "runningTaskQueue"    -> getRunningTaskInfos(),
      "timeoutRunningTasks" -> getTimeoutRunningTaskInfos()
    )

  def getRunningTaskInfos() =
    runningTasks.toList.map((item) => {
      val task = item._2
      task.getTaskInfo()
    })

  def getTimeoutRunningTaskInfos() =
    timeoutRunningTasks.toList.map((item) => {
      val task = item._2
      task.getTaskInfo()
    })

  def getWaitingTaskInfos() =
    waitingTaskQ.toList.map((task) => {
      task.getTaskInfo()
    })
}
