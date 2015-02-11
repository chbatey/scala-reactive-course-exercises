package kvstore

import akka.actor.{ActorLogging, Actor, ActorRef, Props}
import kvstore.Arbiter._
import kvstore.Replica._
import kvstore.Replicator._
import kvstore.Persistence._
import scala.concurrent.duration._
import scala.language.postfixOps

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case class CheckPersist(persist: Persist, originalTime: Long = System.currentTimeMillis()) {
    def age = System.currentTimeMillis() - originalTime
  }

  case class CheckReplicated(replicate: Replicate, originalTime: Long = System.currentTimeMillis()) {
    def age = System.currentTimeMillis() - originalTime
  }

  case class ReplicationWait(client: ActorRef, replicators: Set[ActorRef])


  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging {


  import context.dispatcher
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  val Timeout: Long = 1000

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  val persister: ActorRef = context.actorOf(persistenceProps)

  arbiter ! Join

  def receive = {
    case JoinedPrimary   =>
      log.info("Becoming primary")
      context.become(leader(Map.empty[Long, ActorRef], Map.empty[Long, ReplicationWait]))
    case JoinedSecondary =>
      log.info("becoming secondary")
      context.become(replica(0, Map.empty[Long, ActorRef]))
  }


  /* TODO Behavior for  the leader role. */
  def leader(awaitingPersist: Map[Long, ActorRef], awaitingReplicate: Map[Long, ReplicationWait]): Receive = {
    case Replicas(replicas) =>
      log.info(s"Adding $replicas")
      replicas.foreach( replica => {
        val replicator = context.actorOf(Props(classOf[Replicator], replica))
        secondaries += replica -> replicator
        replicators += replicator
      })
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
    case Insert(key, value, id) =>
      kv += key -> value
      val replicate = Replicate(key, Some(value), id)
      replicators.foreach(replica => {
        log.info(s"Forwarding snapshot to replicator $replicate")
        replica ! replicate
      })
      context.system.scheduler.scheduleOnce(100 milliseconds, self, CheckReplicated(replicate))

      persistAndSchedule(Persist(key, Some(value), id))

      context.become(leader(awaitingPersist + (id -> sender), awaitingReplicate + (id -> ReplicationWait(sender, replicators))))
    case Remove(key, id) =>
      kv -= key
      persistAndSchedule(Persist(key, None, id))

      context.become(leader(awaitingPersist.+(id -> sender), awaitingReplicate))
    case Persisted(key, id) =>
      log.info(s"Operation persisted sending ack $id")
      if (!awaitingReplicate.get(id).isDefined)  {
        awaitingPersist(id) ! OperationAck(id)
      }
    case checkPersist @ CheckPersist(persist, _) =>
      log.info(s"Received check persist $persist with age ${checkPersist.age}")
      awaitingPersist.get(persist.id ) match {
        case None => log.debug("Persistence already suceeded")
        case Some(client) =>
          if (checkPersist.age > Timeout) {
            client ! OperationFailed(persist.id)
          } else {
            persistAndSchedule(persist, Some(checkPersist))
          }
      }
    case checkReplicated @ CheckReplicated(replicate, _) =>
      if (checkReplicated.age > Timeout) {
        awaitingReplicate(replicate.id).client ! OperationFailed(replicate.id)
      }
  }

  private def persistAndSchedule(persist: Persist, checkPersist: Option[CheckPersist] = None) : Unit = {
    context.system.scheduler.scheduleOnce(100 milliseconds, self, checkPersist.getOrElse(CheckPersist(persist)))
    persister ! persist
  }

  /* TODO Behavior for the replica role. */
  def replica(expectedSeq: Long, awaitingPersist: Map[Long, ActorRef]): Receive = {
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
    case snapshot @ Snapshot(key, value, seq) =>
      if (seq < expectedSeq) {
        sender ! SnapshotAck(key, seq)
      } else if (seq > expectedSeq) {
        log.info("Ignoring snapshot as it is from the future!")
        // ignore
      } else {
        if (value.isDefined) {
          kv += key -> value.get
        } else {
          kv -= key
        }
        log.info("Sending persist message")
        persistAndSchedule(Persist(key, value, seq))
        val newMap = awaitingPersist + (seq -> sender)
        context.become(replica(expectedSeq + 1, newMap))
      }
    case Persisted(key, id) =>
      awaitingPersist(id) ! SnapshotAck(key, id)
      val newMap = awaitingPersist - id
      context.become(replica(expectedSeq, newMap))
    case checkPersist @ CheckPersist(persist, _) =>
      log.info(s"Received check persist $persist")
      awaitingPersist.get(persist.id  ) match {
        case None => log.debug("Persistence already suceeded")
        case Some(_) =>
          persistAndSchedule(persist)
      }

  }

}
