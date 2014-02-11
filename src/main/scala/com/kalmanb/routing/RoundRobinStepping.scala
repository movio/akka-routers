package com.kalmanb.routing

import akka.actor._
import akka.dispatch.Dispatchers
import akka.routing._
import java.util.concurrent.atomic.AtomicLong
import scala.annotation.tailrec

case class RoundRobinStepping(routeeRefs: Iterable[ActorRef] = Seq.empty,
                               val lowWaterMark: Int = 10,
                               val routerDispatcher: String = Dispatchers.DefaultDispatcherId,
                               val supervisorStrategy: SupervisorStrategy = RoundRobinBalancing.defaultSupervisorStrategy)
    extends RouterConfig with SmallestMailboxLike {
  import scala.annotation.tailrec
  import scala.annotation.tailrec

  def nrOfInstances: Int = 0 // not used
  def routees: Iterable[String] = Seq.empty // not used

  val checkCount = lowWaterMark * routeeRefs.size

  override def createRoute(routeeProvider: RouteeProvider): Route = {
    routeeProvider.registerRoutees(routeeRefs)

    val next = new AtomicLong(0)
    val count = new AtomicLong(0)

    @tailrec
    def getNext(count: Int = 0): ActorRef = {
      val currentRoutees = routeeProvider.routees
      if (currentRoutees.size == 0 || count > currentRoutees.size)
        routeeProvider.context.system.deadLetters
      else {
        val proposed = currentRoutees((next.getAndIncrement % currentRoutees.size).asInstanceOf[Int])

        if (!proposed.isTerminated
          && !isSuspended(proposed)
          && queueLength(proposed) <= lowWaterMark)
          proposed
        else
          getNext(count + 1)
      }
    }

    // Unfortunate the numberOfMessages returns 0 if unknown
    def queueLength(target: ActorRef) =
      if (hasMessages(target)) {
        val num = numberOfMessages(target)
        println("ssssssssss " + num)
        if (num > 0) num else Long.MaxValue
      } else 0L

    {
      case (sender, message) ⇒
        message match {
          case Broadcast(msg) ⇒ toAll(sender, routeeProvider.routees)
          case msg            ⇒ List(Destination(sender, getNext()))
        }
    }
  }
}

object RoundRobinBalancing {
  val defaultSupervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _ ⇒ SupervisorStrategy.Escalate
  }
}
