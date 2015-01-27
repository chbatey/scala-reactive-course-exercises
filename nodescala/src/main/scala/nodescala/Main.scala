package nodescala

import nodescala.NodeScala
import nodescala.NodeScala.Listener.Default

import scala.language.postfixOps
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import scala.util.Success

object Main {

  def main(args: Array[String]) {
    // TO IMPLEMENT
    // 1. instantiate the server at 8191, relative path "/test",
    //    and have the response return headers of the request
    val myServer: NodeScala.Default = new NodeScala.Default(8191)
    val myServerSubscription: Subscription =  myServer.start("/test"){ request =>
      List("Hello").iterator
    }

    // TO IMPLEMENT
    // 2. create a future that expects some user input `x`
    //    and continues with a `"You entered... " + x` message
    val userInterrupted: Future[String] = Future.userInput("Press Enter to stop the server")
    userInterrupted.onComplete {
      case _ =>
        println("Removing as user requested")
        myServerSubscription.unsubscribe()
    }
//    userInterrupted.continueWith( future => future.)

    // TO IMPLEMENT
    // 3. create a future that completes after 20 seconds
    //    and continues with a `"Server timeout!"` message
    val timeOut: Future[String] = Future.delay(Duration(20, SECONDS)).continueWith(_ => "Server timeout!")
    timeOut.onComplete( {
      case Success(msg) => println(s"Timeout: $msg")
    })

    // TO IMPLEMENT
    // 4. create a future that completes when either 10 seconds elapse
    //    or the user enters some text and presses ENTER
    val delayedFuture = Future.delay(Duration(10, SECONDS))
    val terminationRequested: Future[String] = Future.any(List(timeOut, userInterrupted))

    // TO IMPLEMENT
    // 5. unsubscribe from the server
    terminationRequested onSuccess {
      case msg => {
        println("Removing subsription")
        myServerSubscription.unsubscribe()
      }
    }

  }

}