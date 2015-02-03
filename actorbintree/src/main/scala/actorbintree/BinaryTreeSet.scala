/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import akka.event.LoggingReceive
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor with ActorLogging {
  import BinaryTreeSet._
  import BinaryTreeNode._

  var number = 0

  def createRoot: ActorRef = {
    number = number + 1
    context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true), s"RootNode-$number")
  }

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = LoggingReceive  {
    normal
  }

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = LoggingReceive {
    case c: Operation => {
      root ! c
    }
    case GC =>
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))


  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = LoggingReceive {
    case c: Operation => pendingQueue = pendingQueue :+ c
    case GC => log.debug("ignoring GC request as already GCing")
    case CopyFinished =>
      log.debug("Garbage: Finished GC, playing events received in the mean timne")
      root = newRoot
      for (op <- pendingQueue) {
        root ! op
      }
      context.become(normal)
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean = false) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor with ActorLogging {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = LoggingReceive {
    case c @ Contains(requestor, id, requestedValue) =>
      // woohoo found it
      if (!removed && elem == requestedValue) {
        log.debug(s"Contains True {}", this)
        requestor ! ContainsResult(id, true)
      } else {
        val side = if (requestedValue < elem) Left else Right
        subtrees.get(side) match {
          case Some(child) => child ! c
          case None => requestor ! ContainsResult(id, false)
        }
      }
    case i @ Insert(requestor, id, insertedValue) =>
      if (insertedValue == elem) {
        removed = false
        requestor ! OperationFinished(id)
      } else {
        val side = if (insertedValue < elem) Left else Right
        log.debug(s"Inserting value $insertedValue into side $side")
        subtrees.get(side) match {
          case Some(child) =>
            log.debug(s"Already have a $side, forwarding")
            child ! i
          case None =>
            log.debug(s"Inserting $insertedValue to my $side map before is $subtrees")
            val newNode = context.actorOf(BinaryTreeNode.props(insertedValue), s"Node=$insertedValue")
            subtrees += (side -> newNode)
            requestor ! OperationFinished(id)
        }
      }
    case r @ Remove(requestor, id, elemToRemove) =>
      if (elem == elemToRemove) {
        removed = true
        requestor ! OperationFinished(id)
      } else {
        val side = if (elemToRemove < elem) Left else Right
        subtrees.get(side) match {
          case Some(child) => child ! r
          case None => requestor ! OperationFinished(id)
        }
      }
    case copy @ CopyTo(treeToCopyTo) =>
      val values = subtrees.values.toSet
      values.foreach(_ ! copy)
      if (!removed) {
        log.debug(s"Garbage: Copying self requires insert and children {}", values)
        treeToCopyTo ! Insert(self, -1, elem)
        context.become(copying(values, false))
      } else if (values.size == 0) {
        log.debug(s"Garbage: No children and we're deleted {} {}", removed, values)
        // nothing to do
        finish()
      } else {
        // wait for our children to be done
        log.debug(s"Garbage: Have children and we're deleted {} {}", removed, values)
        context.become(copying(values, true))
      }


  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case CopyFinished =>
      if (expected.size == 1 && insertConfirmed) {
        finish()
      } else {
        context.become(copying(expected.tail, insertConfirmed))
      }
    case OperationFinished(_) =>
      if (expected.size == 0) {
        finish()
      } else {
        context.become(copying(expected, true))
      }
  }

  def finish(): Unit = {
    context.parent ! CopyFinished
    context.stop(self)
  }

}
