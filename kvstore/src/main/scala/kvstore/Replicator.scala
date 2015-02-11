package kvstore

import akka.actor.{ActorLogging, Props, Actor, ActorRef}
import scala.concurrent.duration._
import scala.language.postfixOps

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  case class SnapshotCheck(seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor with ActorLogging {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }
  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case r @ Replicate(key, value, id) =>
      val seq: Long = nextSeq
      context.system.scheduler.scheduleOnce(200 milliseconds, self, SnapshotCheck(seq))
      acks += seq -> (replica, r)
      val snapshot: Snapshot = Snapshot(key, value, seq)
      log.info(s"Sending Snapshot to replica $snapshot")
      replica ! snapshot
    case ack @ SnapshotAck(key, seq) =>
      log.info(s"Received $ack sending replicated")
      acks -= seq
      context.parent ! Replicated(key, seq)
    case SnapshotCheck(seq) =>
      acks.get(seq) match {
        case Some(pair) =>
          log.debug(s"Message for seq ${seq} has not been acked, resending.")
          context.system.scheduler.scheduleOnce(100 milliseconds, self, SnapshotCheck(seq))
          pair._1 ! Snapshot(pair._2.key, pair._2.valueOption, seq)
        case None => log.debug(s"Message for seq ${seq} has already been acked")
      }
  }

}
