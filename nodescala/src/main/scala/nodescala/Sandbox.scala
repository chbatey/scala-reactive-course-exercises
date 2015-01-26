package nodescala

import java.util.concurrent.TimeUnit

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

import scala.concurrent.ExecutionContext.Implicits.global

object Sandbox extends App {
  val working = Future.run() { ct =>
    Future {
      while (ct.nonCancelled) {
        Thread.sleep(100)
        println("working")
      }
      println("done")
    }
  }
  val timeout = Future.delay(Duration(5, TimeUnit.SECONDS))

  timeout onSuccess {
    case _ => working.unsubscribe()
  }

  Await.ready(timeout, Duration(100, TimeUnit.SECONDS))

  Thread.sleep(1000)

}
