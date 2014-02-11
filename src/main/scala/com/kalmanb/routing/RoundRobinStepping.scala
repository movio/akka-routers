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
    def queueLength = (target: ActorRef) ⇒ {
      if (hasMessages(target)) {
        val num = numberOfMessages(target)
        if (num > 0) num else Long.MaxValue
      } else 0L
    }

    def nextActor = getNextActor(routeeRefs, routeeProvider.context.system.deadLetters, queueLength, currentStep, count)

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
                   currentStep: AtomicLong,
                   count: AtomicLong,
                   stepSize: Int = 10): ActorRef = {
    val currentRoutees = routees.toIndexedSeq

    @tailrec
    def next(accumulator: Int = 0): ActorRef = {
      if (accumulator >= currentRoutees.size) {
        println("aaaaaaaaaaaa")
        currentStep.getAndIncrement
        next(0)
      } else {
        val proposed = currentRoutees((count.getAndIncrement % currentRoutees.size).asInstanceOf[Int])

        val length = queueLength(proposed)
        val thisStep = (length % stepSize).asInstanceOf[Int]
        println("aaa "+thisStep )
        println("len "+length  )
        println("ss "+stepSize   )


        //if (!proposed.isTerminated  // FIXME test
        //&& !isSuspended(proposed) // FIXME test
        if (thisStep == currentStep.get)
          proposed
        else if (thisStep < currentStep.get) {
          println("aa")
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

