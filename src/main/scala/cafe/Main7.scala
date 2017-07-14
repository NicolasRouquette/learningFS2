package cafe

import java.lang.{Integer, System, Thread}

import fs2._
import fs2.async._

import scala.collection.immutable.Seq
import scala.{App, Int, Long, StringContext, Unit}
import scala.Predef.String

object Main7 extends App {

  implicit val S = fs2.Strategy.fromFixedDaemonPool(8, threadName = "worker")

  val norders: Long = Integer.parseInt(args(0)).toLong
  val hotQueueLength: Int = Integer.parseInt(args(1))
  val coldQueueLength: Int = Integer.parseInt(args(2))

  val c = Cafe()
  System.out.println(s"Cafe ${c.name} opened for business")

  // An asynchronous task that asks for a single order in
  // case getOrder() does always not return quickly.
  val askForOrder
  : Task[Order]
  = Task {
    val o = c.getOrder()
    System.out.println(s"""${Thread.currentThread.getName} asking for $o""")
    o
  }

  // A process that repeatedly ask for an order
  val askForOrders
  : Stream[Task, Order]
  = Stream.repeatEval(askForOrder)

  // The process that asks for N orders. Normally the ordering
  // process would end when the Cafe closes. This process
  // enters a Halt(Cause.End) state automatically after the take() reaches
  // its limit.
  val orders
  : Stream[Task, Order]
  = askForOrders.take(norders)

  // Splitting the order means extracting out the drinks.
  val drinksOnly
  : Pipe[Task, Order, Seq[Item]]
  = { in =>
    in.map { o =>
      o.items
    }
  }

  val hotDrinkQueue
  : Task[mutable.Queue[Task, Item]]
  = boundedQueue[Task, Item](maxSize = hotQueueLength)

  val coldDrinkQueue
  : Task[mutable.Queue[Task, Item]]
  = boundedQueue[Task, Item](maxSize = coldQueueLength)

  // Based on "iced", map to left or right. Use \/ (disjunction).
  // Each drink order is broken out individually.
  def coldOrHotIndividualDrink
  (hq: mutable.Queue[Task, Item],
   cq: mutable.Queue[Task, Item])
  : Pipe[Task, Item, Unit]
  = { in =>
    in.flatMap { item =>
      Stream.eval((if (item.iced) cq else hq).enqueue1(item))
    }
  }

  def processHotDrinks(barista: Barista)
  : Pipe[Task, Item, Drink]
  = {
    _.map(barista.prepareHotDrink)
  }

  def processColdDrinks(barista: Barista)
  : Pipe[Task, Item, Drink]
  = {
    _.map(barista.prepareColdDrink)
  }

  def hotBaristaWorker
  (q: mutable.Queue[Task, Item],
   barista: Barista)
  : Stream[Task, Drink]
  = q.size.discrete.flatMap { _ =>
    q.dequeue.map { item =>
      barista.prepareHotDrink(item)
    }
  }

  def coldBaristaWorker
  (q: mutable.Queue[Task, Item],
   barista: Barista)
  : Stream[Task, Drink]
  = q.size.discrete.flatMap { _ =>
    q.dequeue.map { item =>
      barista.prepareColdDrink(item)
    }
  }

  val joinDrinkQueue
  : Task[mutable.Queue[Task, Drink]]
  = boundedQueue[Task, Drink](maxSize = 3)

  val jdq = Stream.eval(joinDrinkQueue)

  def joinBaristaWorker
  (s: Stream[Task, Drink],
   j: mutable.Queue[Task, Drink])
  : Stream[Task, Unit]
  = s.to(j.enqueue)

  val printer
  : Pipe[Task, Drink, String]
  = { in =>
    in.map {
      _.toString
    }
  }

  def items
  (hq: mutable.Queue[Task, Item],
   cq: mutable.Queue[Task, Item])
  : Stream[Task, Unit]
  = orders
    .through(drinksOnly)
    .flatMap(Stream.emits)
    .through(coldOrHotIndividualDrink(hq, cq))

  def waiter
  (j: mutable.Queue[Task, Drink])
  : Stream[Task, Unit]
  = j.size.discrete.flatMap { _ =>
    j.dequeue.map { drink =>
      System.out.println(drink)
      ()
    }
  }

  val b1 = Barista("Jack")
  val b2 = Barista("Peter")
  val b3 = Barista("Jane")
  val b4 = Barista("Susan")

  val hq = hotDrinkQueue.unsafeRun()
  val cq = coldDrinkQueue.unsafeRun()
  val j = joinDrinkQueue.unsafeRun()

  val h1 = hotBaristaWorker(hq, b1)
  val h2 = hotBaristaWorker(hq, b2)
  val h3 = hotBaristaWorker(hq, b3)
  val c1 = coldBaristaWorker(cq, b4)

  val i: Stream[Task, Unit] = items(hq, cq)

  val j1: Stream[Task, Unit] = joinBaristaWorker(h1, j)
  val j2: Stream[Task, Unit] = joinBaristaWorker(h2, j)
  val j3: Stream[Task, Unit] = joinBaristaWorker(h3, j)
  val j4: Stream[Task, Unit] = joinBaristaWorker(c1, j)

  val w: Stream[Task, Unit] = waiter(j)

  i.merge(j1).merge(j2).merge(j3).merge(j4).merge(w).drain.run.unsafeRun()
}
