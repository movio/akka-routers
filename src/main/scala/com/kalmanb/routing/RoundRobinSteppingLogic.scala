package com.kalmanb.routing

import java.util.concurrent.atomic.AtomicLong

import scala.annotation.tailrec
import scala.collection.immutable

import akka.routing.NoRoutee
import akka.routing.Routee
import akka.routing.SmallestMailboxRoutingLogic

class RoundRobinSteppingLogic(stepSize: Int = 10) extends SmallestMailboxRoutingLogic {

  val currentStep = new AtomicLong(0)
  val next = new AtomicLong(0)

  override def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee = {

    @tailrec
    def selectNext(accumulator: Int = 0): Routee = {
      if (accumulator >= routees.size) {
        currentStep.getAndIncrement
        selectNext()
      } else {
        val proposed = routees((next.getAndIncrement % routees.size).asInstanceOf[Int])

        val length = queueLength(proposed)
        val thisStep = (length / stepSize)

        if (isTerminated(proposed) || isSuspended(proposed))
          selectNext(accumulator + 1)
        else if (thisStep == currentStep.get) {
          proposed
        } else if (thisStep < currentStep.get) {
          currentStep.set(thisStep)
          proposed
        } else {
          selectNext(accumulator + 1)
        }
      }
    }

    if (routees.size == 0)
      NoRoutee
    else
      selectNext()
  }

  // Unfortunately numberOfMessages returns 0 for remote actors.
  //
  // Can erroneously report that a local actor's queue length is
  // Int.MaxValue if the queue has one message which the actor
  // starts processing between the hasMessages and
  // numberOfMessages calls
  def queueLength(target: Routee): Int =
    if (hasMessages(target)) {
      val num = numberOfMessages(target)
      // Assume remote actors have full queues
      if (num > 0) num else Int.MaxValue
    } else 0
}


