package com.kalmanb.routing

import scala.collection.immutable

import akka.actor.Actor
import akka.actor.Props
import akka.routing.Routee
import akka.routing.Router

object RoundRobinSteppingRouterActor {
  def props(routees: immutable.IndexedSeq[Routee]): Props =
    Props(new RoundRobinSteppingRouterActor(routees))

  def props(routees: immutable.IndexedSeq[Routee], stepSize: Int): Props =
    Props(new RoundRobinSteppingRouterActor(routees, stepSize))
}
class RoundRobinSteppingRouterActor(routees: immutable.IndexedSeq[Routee], stepSize: Int = 10)
  extends Actor {

  val router = Router(new RoundRobinSteppingLogic(stepSize), routees)

  def receive = {
    case m: Any => router.route(m, sender())
  }
}
