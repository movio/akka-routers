package com.kalmanb.routing

import akka.actor._
import akka.dispatch.Dispatchers
import akka.routing._
import java.util.concurrent.atomic.AtomicLong

case class RoundRobinBalancing(routeeRefs: Iterable[ActorRef] = Seq.empty,
                               val routerDispatcher: String = Dispatchers.DefaultDispatcherId,
                               val supervisorStrategy: SupervisorStrategy = RoundRobinBalancing.defaultSupervisorStrategy)
    extends RouterConfig with SmallestMailboxLike {

  def nrOfInstances: Int = 0 // not used

  def routees: Iterable[String] = Seq.empty // not used

  override def createRoute(routeeProvider: RouteeProvider): Route = {
    routeeProvider.registerRoutees(routeeRefs)

    val next = new AtomicLong(0)

    def getNext(): ActorRef = {
      val currentRoutees = routeeProvider.routees
      if (currentRoutees.isEmpty) routeeProvider.context.system.deadLetters
      else currentRoutees((next.getAndIncrement % currentRoutees.size).asInstanceOf[Int])
    }

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
