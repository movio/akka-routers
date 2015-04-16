package com.kalmanb.routing

import scala.collection.immutable.List
import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import com.kalmanb.test.TestSpec

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.DeadLetter
import akka.actor.PoisonPill
import akka.actor.Props
import akka.pattern.ask
import akka.routing.ActorRefRoutee
import akka.routing.Broadcast
import akka.routing.Routee
import akka.testkit.TestActorRef
import akka.testkit.TestProbe
import akka.util.Timeout

class RoundRobinSteppingTest extends TestSpec {
  implicit val system = ActorSystem("test")
  import system.dispatcher
  implicit val timeout = Timeout(1 second)

  describe("RoundRobinStepping router") {
    it("should route messages to a single actor") {
      val actor1 = TestProbe()
      val routeeRefs = Seq(actor1.ref)
      val routees = routeeRefs map ActorRefRoutee
      val router = system.actorOf(RoundRobinSteppingRouterActor.props(routees.toIndexedSeq))

      router ! "one"
      router ! "two"

      actor1.expectMsg(500 millis, "one")
      actor1.expectMsg(500 millis, "two")
    }

    it("should round robin messages when not under load") {
      val actor1 = TestProbe()
      val actor2 = TestProbe()
      val routeeRefs = Seq(actor1.ref, actor2.ref)
      val routees = routeeRefs map ActorRefRoutee
      val router = system.actorOf(RoundRobinSteppingRouterActor.props(routees.toIndexedSeq))

      router ! "one"
      router ! "two"
      router ! "three"
      router ! "four"

      actor1.expectMsg(500 millis, "one")
      actor2.expectMsg(500 millis, "two")
      actor1.expectMsg(500 millis, "three")
      actor2.expectMsg(500 millis, "four")
    }

    it("should send Broadcast messages to all routeeRefs") {
      val routeeProbes = (1 to 5) map (_ ⇒ TestProbe())
      val routeeRefs = routeeProbes map (_.ref)
      val routees = routeeRefs map ActorRefRoutee
      val router = system.actorOf(RoundRobinSteppingRouterActor.props(routees.toIndexedSeq))

      router ! Broadcast("one")

      routeeProbes.foreach(_.expectMsg(500 millis, "one"))
    }

    it("should send messages to dead letter if no routeeRefs available") {
      val routeeRefs = List.empty
      val routees = routeeRefs map ActorRefRoutee
      val router = system.actorOf(RoundRobinSteppingRouterActor.props(routees.toIndexedSeq))

      val listener = TestProbe()
      system.eventStream.subscribe(listener.ref, classOf[DeadLetter])

      router ! "one"
      listener.expectMsgType[DeadLetter]
    }

  }

  describe("stepping") {

    val routeeProbes = (1 to 3) map (_ ⇒ TestProbe())
    val routeeRefs = routeeProbes map (_.ref)
    val routees = routeeRefs map ActorRefRoutee

    class Fixture(queueLengthFunc: Routee => Int) {
      val target = new RoundRobinSteppingLogic(stepSize = 3) {
        override def queueLength(target: Routee) = queueLengthFunc(target)
      }

      val currentStep = target.currentStep
      val next = target.next

      def test = target.select(None, routees)

    }

    describe("no stepping") {
      it("should be round robin before stepping") {
        def lengthFunc = (routee: Routee) ⇒ 2

        new Fixture(lengthFunc) {
          test should be(routees(0))
          test should be(routees(1))
          test should be(routees(2))
          test should be(routees(0))

          currentStep.get should be(0)
          next.get should be(4)
        }
      }
    }
    describe("stepping up") {
      it("if the first actor is above the current step it should be skipped") {
        // First actor will be 4
        // All others 2
        def lengthFunc = (routee: Routee) ⇒
          if (routee == routees(0)) 4
          else 2

        new Fixture(lengthFunc) {
          test should be(routees(1))
          test should be(routees(2))
          test should be(routees(1))
          test should be(routees(2))

          currentStep.get should be(0)
          // Should be 4 plus 2 skipped
          next.get should be(6)
        }
      }
      it("if all actors above current step it should move up one step") {
        def lengthFunc = (routee: Routee) ⇒ 4

        new Fixture(lengthFunc) {
          test should be(routees(0))
          test should be(routees(1))
          test should be(routees(2))
          test should be(routees(0))

          currentStep.get should be(1)
          // Will check 1-3 then move up to deliver the 4 on step 1
          next.get should be(7)
        }
      }
      it("if all actors above current step it should move up to the lowest step") {
        def lengthFunc = (routee: Routee) ⇒
          if (routee == routees(0)) 6 // on step 2
          else 10 // on step 3

        new Fixture(lengthFunc) {
          test should be(routees(0))
          test should be(routees(0))
          test should be(routees(0))

          currentStep.get should be(2)
          // Will check 1-3 x 2 then one per 3
          next.get should be(13)
        }
      }
    }

    describe("stepping down") {
      it("should step down if a lower step is found") {
        def lengthFunc = (routee: Routee) ⇒
          if (routee == routees(0)) 10 // step 3
          else if (routee == routees(1)) 4 // step 2
          else 2 // step 1

        new Fixture(lengthFunc) {
          currentStep.set(3)
          test should be(routees(0))
          currentStep.get should be(3)

          test should be(routees(1))
          currentStep.get should be(1)

          test should be(routees(2))
          currentStep.get should be(0)
        }
      }
    }
  }

  describe("stepping integration") {
    it(s"should round robin until mailbox a mailbox size exceeds stepSize") {
      val routeeRefs = (1 to 3).map(_ ⇒ TestActorRef(new Tester()))
      val routees = routeeRefs map ActorRefRoutee
      val router = system.actorOf(RoundRobinSteppingRouterActor.props(routees.toIndexedSeq))

      (1 to 12) map (i ⇒ router ! i)

      Thread sleep 100

      routeeRefs foreach { r ⇒
        r.underlyingActor.messagesReveived.size should be(4)
      }
      val firstRouteeMessages = routeeRefs(0).underlyingActor.messagesReveived
      firstRouteeMessages(0) should be(1)
      firstRouteeMessages(1) should be(4)
      firstRouteeMessages(2) should be(7)
      firstRouteeMessages(3) should be(10)
    }

    // TODO: Fix this test. The split of messages is a race condition, so there isn't an
    //       "correct" number that each should get. The current numbers assume that
    //       the slow actor doesn't process any messages before all messages finish routing,
    //       but this is often not the case in reality.
    ignore("when an routee is slow it should not take traffic once over stepSize") {
      implicit val timeout = Timeout(1 second)
      val fast = system.actorOf(Props(new Tester()))
      val slow = system.actorOf(Props(new Tester(100)))
      val routeeRefs = Seq(fast, slow)
      val routees = routeeRefs map ActorRefRoutee
      val router = system.actorOf(RoundRobinSteppingRouterActor.props(routees = routees.toIndexedSeq, stepSize = 2))
      (1 to 20) map (i ⇒ router ! i)

      val fastFuture = fast ? 'GetMessages
      val fastResult = Await.result(fastFuture, 1 second)
      fastResult.asInstanceOf[List[Int]].size should be(17)

      val slowFuture = slow ? 'GetMessages
      val slowResult = Await.result(slowFuture, 1 second)
      slowResult.asInstanceOf[List[Int]].size should be(3)
    }

    it("should not route messages to terminated actors") {
      val terminated = system.actorOf(Props(new Tester(100)))
      terminated ! PoisonPill
      Thread sleep 100
      val normal = system.actorOf(Props(new Tester()))
      val routeeRefs = Seq(terminated, normal)
      val routees = routeeRefs map ActorRefRoutee
      val router = system.actorOf(RoundRobinSteppingRouterActor.props(routees = routees.toIndexedSeq, stepSize = 2))
      (1 to 2) map (i ⇒ router ! i)

      val normalFuture = normal ? 'GetMessages
      val normalResult = Await.result(normalFuture, 1 second)
      normalResult.asInstanceOf[List[Int]].size should be(2)
    }

    ignore("should not route messages to suspended actors") {
      // Have no idea how to test this??
    }
  }
}

class Tester(sleepMillis: Int = 0) extends Actor {
  var messagesReveived = List.empty[Int]
  def receive = {
    case e: Int ⇒
      messagesReveived = messagesReveived :+ e
      Thread sleep sleepMillis
    case 'GetMessages ⇒ sender ! messagesReveived
    case _ ⇒ throw new Exception("--- bad ---")
  }
}


