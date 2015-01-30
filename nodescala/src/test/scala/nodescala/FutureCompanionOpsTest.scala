package nodescala

import java.util.concurrent.{TimeoutException, TimeUnit}

import dispatch.FutureEither
import org.scalatest.FunSuite
import org.scalatest.matchers.{ShouldMatchers, ClassicMatchers, Matchers}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import scala.concurrent.ExecutionContext.Implicits.global

class FutureCompanionOpsTest extends FunSuite with ShouldMatchers{
  test("Always") {
    val future = Future.always("Chris")

    future.isCompleted should equal(true)
    Await.result(future, Duration(1, TimeUnit.SECONDS)) should equal("Chris")
  }

  test("Never") {
    val future = Future.never

    future.isCompleted should equal(false)
    intercept[TimeoutException] {
      Await.result(future, Duration(1, TimeUnit.SECONDS))
    }
  }

  test("All only success if all futures succeed") {
    val future1 = Future.always("Chris")
    val future2 = Future.always("Batey")

    val all = Future.all(List(future1, future2))

    Await.result(all, Duration(1, TimeUnit.SECONDS)) should equal(List("Chris", "Batey"))
  }

  test("All fails if one is a failure") {
    val future1 = Future.always("Chris")
    val exception: Exception = new Exception
    val future2 = Future.failed(exception)

    val all: Future[List[String]] = Future.all(List(future1, future2))

    Await.result(all.failed, Duration(1, TimeUnit.SECONDS)) should equal(exception)
  }

  test("Any, success wins the race") {
    val success = Future.always("Chris")
    val never = Future.never

    val any: Future[String] = Future.any(List(success, never))

    Await.result(any, Duration(1, TimeUnit.SECONDS)) should equal("Chris")
  }

  test("Any, failure wins the race") {
    val exception: Exception = new Exception
    val failure = Future.failed(exception)
    val never = Future.never

    val any: Future[String] = Future.any(List(failure, never))

    Await.result(any.failed, Duration(1, TimeUnit.SECONDS)) should equal(exception)
  }

  test("Any, no one wins the race") {
    val never = Future.never

    val any: Future[String] = Future.any(List( never))

    intercept[TimeoutException] {
      Await.result(any, Duration(1, TimeUnit.SECONDS))
    }
  }
}
