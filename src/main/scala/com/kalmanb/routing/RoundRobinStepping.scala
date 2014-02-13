package com.kalmanb.routing

import akka.actor._
import akka.dispatch.Dispatchers
import akka.routing._
import java.util.concurrent.atomic.AtomicLong
import scala.annotation.tailrec

case class RoundRobinStepping(routeeRefs: Iterable[ActorRef] = Seq.empty,
                              val stepSize: Int = 10,
                              val routerDispatcher: String = Dispatchers.DefaultDispatcherId,
                              val supervisorStrategy: SupervisorStrategy = RoundRobinStepping.defaultSupervisorStrategy)
    extends RouterConfig with SmallestMailboxLike {
  import RoundRobinStepping._

  def nrOfInstances: Int = 0 // not used
  def routees: Iterable[String] = Seq.empty // not used

  override def createRoute(routeeProvider: RouteeProvider): Route = {
    routeeProvider.registerRoutees(routeeRefs)

    val currentStep = new AtomicLong(0)
    val count = new AtomicLong(0)

    // Unfortunate the numberOfMessages returns 0 if unknown
    def queueLength(target: ActorRef) = {
      if (hasMessages(target)) {
        val num = numberOfMessages(target)
        if (num > 0) num else Long.MaxValue
      } else 0L
    }

    def nextActor = getNextActor(routeeRefs, routeeProvider.context.system.deadLetters, queueLength, isSuspended, currentStep, count, stepSize)

    {
      case (sender, message) ⇒
        message match {
          case Broadcast(msg) ⇒ toAll(sender, routeeProvider.routees)
          case msg            ⇒ List(Destination(sender, nextActor))
        }
    }
  }
}

object RoundRobinStepping {
  case class Next(lastStep: Int, lastCount: Int, nextActor: ActorRef)

  val defaultSupervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _ ⇒ SupervisorStrategy.Escalate
  }

  /**
   *  We manipulate currentStep and count here as this can be happening on
   *  multiple threads.
   */
  def getNextActor(routees: Iterable[ActorRef],
                   defaultRoutee: ActorRef,
                   queueLength: ActorRef ⇒ Long,
                   isSuspended: ActorRef ⇒ Boolean,
                   currentStep: AtomicLong,
                   count: AtomicLong,
                   stepSize: Int = 10): ActorRef = {
    val currentRoutees = routees.toIndexedSeq

    @tailrec
    def next(accumulator: Int = 0): ActorRef = {
      if (accumulator >= currentRoutees.size) {
        currentStep.getAndIncrement
        next(0)
      } else {
        val proposed = currentRoutees((count.getAndIncrement % currentRoutees.size).asInstanceOf[Int])

        val length = queueLength(proposed)
        val thisStep = (length / stepSize)

        if (proposed.isTerminated || isSuspended(proposed))
          next(accumulator + 1)
        else if (thisStep == currentStep.get) {
          proposed
        } else if (thisStep < currentStep.get) {
          currentStep.set(thisStep)
          proposed
        } else {
          next(accumulator + 1)
        }
      }
    }

    if (currentRoutees.size == 0)
      defaultRoutee
    else
      next()
  }
}

