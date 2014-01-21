package com.kalmanb.routing

import akka.actor._
import akka.dispatch.Dispatchers
import akka.routing._
import java.util.concurrent.atomic.AtomicLong

case class RoundRobinBalancing(routees: Iterable[ActorRef] = Nil,
          val routerDispatcher: String = Dispatchers.DefaultDispatcherId,
          val supervisorStrategy: SupervisorStrategy = RoundRobinBalancing.defaultSupervisorStrategy) extends RouterConfig {


  def createRoute(routeeProvider: RouteeProvider): Route = {
    routeeProvider.registerRoutees(routees)

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

object RoundRobinBalancing  {
  val defaultSupervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _ ⇒ SupervisorStrategy.Escalate
  }
}
