package com.kalmanb.routing

import com.kalmanb.test.TestSpec
import akka.actor._
import akka.testkit._
import akka.routing._
import scala.concurrent.duration._
import scala.collection.immutable._
import java.util.concurrent.atomic.AtomicLong

class RoundRobinSteppingTest extends TestSpec {
  implicit val system = ActorSystem("test")

  //describe("RoundRobinStepping router") {
    //it("should route messages to a single actor") {
      //val actor1 = TestProbe()
      //val routeeRefs = Seq(actor1.ref)
      //val router = system.actorOf(Props().withRouter(RoundRobinStepping(routeeRefs = routeeRefs)))

      //router ! "one"
      //router ! "two"

      //actor1.expectMsg(500 millis, "one")
      //actor1.expectMsg(500 millis, "two")
    //}

    //it("should round robin messages when not under load") {
      //val actor1 = TestProbe()
      //val actor2 = TestProbe()
      //val routeeRefs = Seq(actor1.ref, actor2.ref)
      //val router = system.actorOf(Props().withRouter(RoundRobinStepping(routeeRefs = routeeRefs)))

      //router ! "one"
      //router ! "two"
      //router ! "three"
      //router ! "four"

      //actor1.expectMsg(500 millis, "one")
      //actor2.expectMsg(500 millis, "two")
      //actor1.expectMsg(500 millis, "three")
      //actor2.expectMsg(500 millis, "four")
    //}

    //it("should send Broadcast messages to all routeeRefs") {
      //val routeeRefs = (1 to 5) map (_ ⇒ TestProbe())
      //val router = system.actorOf(Props().withRouter(RoundRobinStepping(routeeRefs = routeeRefs map (_.ref))))

      //router ! Broadcast("one")

      //routeeRefs.foreach(_.expectMsg(500 millis, "one"))
    //}

    //it("should send messages to dead letter if no routeeRefs available") {
      //val routeeRefs = List.empty
      //val router = system.actorOf(Props().withRouter(RoundRobinStepping(routeeRefs = routeeRefs)))

      //val listener = TestProbe()
      //system.eventStream.subscribe(listener.ref, classOf[DeadLetter])

      //router ! "one"
      //listener.expectMsgType[DeadLetter]
    //}

    //it("should not route messages to terminated actors") {
      //fail("")
    //}

    //it("should not route messages to suspended actors") {
      //fail("")
    //}
  //}

  describe("stepping") {
      val routees = ((1 to 3) map (_ ⇒ TestProbe())).toSeq
      val deadLetter = TestProbe()
      def queueLength = (actor: ActorRef) ⇒ 2L
      val currentStep = new AtomicLong(0)
      val count = new AtomicLong(0)

      def test(length: ActorRef => Long) = {
        RoundRobinStepping.getNextActor(routees map (_.ref), deadLetter.ref,
          length,
          currentStep,
          count,
          3)
      }
    it("should be round robin before stepping"){
      currentStep.set(0)
      count.set(0)
      def length = (actor: ActorRef) ⇒ 2L
      test(length) should be(routees(0).ref)
      test(length) should be(routees(1).ref)
      test(length) should be(routees(2).ref)
      test(length) should be(routees(0).ref)
      currentStep.get should be(0)
      count.get should be(4)
    }
    //it("if the first actor is above the current step it should be skipped"){
      //currentStep.set(0)
      //count.set(0)
      //// First actor will be 4
      //// All others 2
      //def length = (actor: ActorRef) ⇒ 
        //if(actor == routees(0).ref) 4L
        //else 2L
      //test(length) should be(routees(1).ref)
      //test(length) should be(routees(2).ref)
      //test(length) should be(routees(1).ref)
      //test(length) should be(routees(2).ref)
      //currentStep.get should be(0)
      //// Should be 4 plus 2 skipped
      //count.get should be(6)
    //}
    //it("if all actors above current step it should move up one step") {
      //currentStep.set(0)
      //count.set(0)
      //def length = (actor: ActorRef) ⇒ 4L
      //test(length) should be(routees(0).ref)
      //test(length) should be(routees(1).ref)
      //test(length) should be(routees(2).ref)
      //test(length) should be(routees(0).ref)
      //currentStep.get should be(1)
      //count.get should be(4)
    //}
  //}

  //describe("stepping integration") {
    //it(s"should round robin until mailbox a mailbox size exceeds stepSize") {
      //val routeeRefs = (1 to 3).map(_ ⇒ TestActorRef(new Tester()))
      //val router = system.actorOf(Props().withRouter(RoundRobinStepping(routeeRefs = routeeRefs)))

      //(1 to 12) map (i ⇒ router ! i)

      //Thread sleep 100

      //routeeRefs foreach { r ⇒
        //r.underlyingActor.messagesReveived.size should be(4)
      //}
      //val firstRouteeMessages = routeeRefs(0).underlyingActor.messagesReveived
      //firstRouteeMessages(0) should be(1)
      //firstRouteeMessages(1) should be(4)
      //firstRouteeMessages(2) should be(7)
      //firstRouteeMessages(3) should be(10)
    //}

    //it("should move up to step 1 if all routees have more than stepSize in there queues") {

    //}

    //it(s"when an routee is slow it should not take traffic once over stepSize") {
    //val fast = TestActorRef(new Tester())
    //val slow = TestActorRef(new Tester(100))
    //val routees = Seq(fast, slow)
    //val router = system.actorOf(Props().withRouter(RoundRobinStepping(routeeRefs = routees, stepSize = 2)))
    //(1 to 20) map (i ⇒ router ! i)
    //fast.underlyingActor.messagesReveived.size should be(18)
    //slow.underlyingActor.messagesReveived.size should be(2)
    //}
  }
}

class Tester(sleepMillis: Int = 0) extends Actor {
  var messagesReveived = List.empty[Int]
  def receive = {
    case e: Int ⇒
      messagesReveived = messagesReveived :+ e
      Thread sleep sleepMillis
    case _ ⇒ throw new Exception("--- bad ---")
  }
}

