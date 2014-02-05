package com.kalmanb.routing

import com.kalmanb.test.TestSpec
import akka.actor._
import akka.testkit._
import akka.routing._
import scala.concurrent.duration._
import scala.collection.immutable._

class RoundRobinBalancingTest extends TestSpec {
  implicit val system = ActorSystem("test")

  describe("RoundRobinBalancing router") {
    it("should route messages to a single actor") {
      val actor1 = TestProbe()
      val routeeRefs = Seq(actor1.ref)
      val router = system.actorOf(Props().withRouter(RoundRobinBalancing(routeeRefs = routeeRefs)))

      router ! "one"
      router ! "two"

      actor1.expectMsg(500 millis, "one")
      actor1.expectMsg(500 millis, "two")
    }

    it("should round robin messages when not under load") {
      val actor1 = TestProbe()
      val actor2 = TestProbe()
      val routeeRefs = Seq(actor1.ref, actor2.ref)
      val router = system.actorOf(Props().withRouter(RoundRobinBalancing(routeeRefs = routeeRefs)))

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
      val routeeRefs = (1 to 5) map (_ => TestProbe())
      val router = system.actorOf(Props().withRouter(RoundRobinBalancing(routeeRefs = routeeRefs map (_.ref))))

      router ! Broadcast("one")

      routeeRefs.foreach(_.expectMsg(500 millis, "one"))
    }

    it("should send messages to dead letter if no routeeRefs available") {
      val routeeRefs = List.empty
      val router = system.actorOf(Props().withRouter(RoundRobinBalancing(routeeRefs = routeeRefs)))

      val listener = TestProbe()
      system.eventStream.subscribe(listener.ref, classOf[DeadLetter])
      
      router ! "one"
      listener.expectMsgType[DeadLetter]
    }
  }

  describe("below watermark") {
    it(s"should round robin until mailbox a mailbox size exceeds lowWaterMark") {
      val routeeRefs = (1 to 3).map (_ => TestActorRef(new Tester(4)))
      val router = system.actorOf(Props().withRouter(RoundRobinBalancing(routeeRefs = routeeRefs)))

      (1 to 12) map (i => router ! i)

      Thread sleep 100 

      routeeRefs foreach {r =>
        r.underlyingActor.messagesReveived.size should be (4)
      }
      val firstRouteeMessages = routeeRefs(0).underlyingActor.messagesReveived
      firstRouteeMessages(0) should be (1)
      firstRouteeMessages(1) should be (4)
      firstRouteeMessages(2) should be (7)
      firstRouteeMessages(3) should be (10)
    }

    it(s"should only check mailbox sizes ... TBC ") {
      //fail("TBC")
    }
  }
}

class Tester(maxQueue: Int = Int.MaxValue) extends Actor {
  var messagesReveived = List.empty[Int]
  def receive = {
    case e:Int => 
      messagesReveived = messagesReveived :+ e
    case _ => throw new Exception("--- bad ---")
  }
}

