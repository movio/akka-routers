package com.kalmanb.routing

import com.kalmanb.test.TestSpec
import akka.actor._
import akka.testkit._
import akka.routing._
import scala.concurrent.duration._

class RoundRobinBalancingTest extends TestSpec {
  implicit val system = ActorSystem("test")

  describe("RoundRobinBalancing router") {
    it("should route messages to a single actor") {
      val actor1 = TestProbe()
      val routees = Seq(actor1.ref)
      val router = system.actorOf(Props().withRouter(RoundRobinBalancing(routees = routees)))

      router ! "one"
      router ! "two"

      actor1.expectMsg(500 millis, "one")
      actor1.expectMsg(500 millis, "two")
    }

    it("should round robin messages when not under load") {
      val actor1 = TestProbe()
      val actor2 = TestProbe()
      val routees = Seq(actor1.ref, actor2.ref)
      val router = system.actorOf(Props().withRouter(RoundRobinBalancing(routees = routees)))

      router ! "one"
      router ! "two"
      router ! "three"
      router ! "four"

      actor1.expectMsg(500 millis, "one")
      actor2.expectMsg(500 millis, "two")
      actor1.expectMsg(500 millis, "three")
      actor2.expectMsg(500 millis, "four")
    }

    it("should send Broadcast messages to all routees") {
      val actor1 = TestProbe()
      val actor2 = TestProbe()
      val routees = Seq(actor1.ref, actor2.ref)
      val router = system.actorOf(Props().withRouter(RoundRobinBalancing(routees = routees)))

      router ! Broadcast("one")

      actor1.expectMsg(500 millis, "one")
      actor2.expectMsg(500 millis, "one")
    }

    it("should send messages to dead letter if no routees available") {
      val routees = List.empty
      val router = system.actorOf(Props().withRouter(RoundRobinBalancing(routees = routees)))
      //fail("not sure how to test")
    }
  }

  describe("overloading slow routees") {
    it(s"should round robin until mailbox a mailbox size exceeds lowWaterMark") {
      //fail("TBC")
    }

    it(s"should only check mailbox sizes ... TBC ") {
      //fail("TBC")
    }
  }
}

