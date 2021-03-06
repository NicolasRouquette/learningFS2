package cafe

import java.lang.{Integer, System, Thread}

import fs2._

import scalaz._
import scala.collection.immutable.Seq
import scala.concurrent.duration.DurationInt
import scala.{App, Int, Long, None, Option, StringContext, Unit}
import scala.Predef.String

object Main6f extends App {
  implicit val S: Strategy = Strategy.fromFixedDaemonPool(8, threadName = "worker")

  val norders: Long = if (args.length == 2) Integer.parseInt(args(0)).toLong else 20L
  val qsize: Int = if (args.length == 2) Integer.parseInt(args(1)) else 5

  val c = Cafe()
  System.out.println(s"Cafe ${c.name} opened for business")

  // An asynchronous task that asks for a single order in
  // case getOrder() does always not return quickly.
  val askForOrder
  : Task[Order]
  = Task { c.getOrder() }

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
  = _.map(_.items)

  // Based on "iced", map to left or right. Use \/ (disjunction).
  // Each drink order is broken out individually.
  val coldOrHotIndividualDrink
  : Pipe[Task, Item, \/[Item, Item]]
  = _.map { item =>
    if (item.iced)
      -\/(item)
    else
      \/-(item)
  }

  def createDrinks(barista: Barista)
  : Pipe[Task, \/[Item, Item], Drink]
  = _.map {
    case -\/(item) =>
      barista.prepareColdDrink(item)
    case \/-(item) =>
      barista.prepareHotDrink(item)
  }

  val joinDrinkQueue
  : Task[async.mutable.Queue[Task, Option[Drink]]]
  = async.boundedQueue[Task, Option[Drink]](maxSize = qsize)

  val jdq = Stream.eval(joinDrinkQueue).flatMap {
    q: async.mutable.Queue[Task, Option[Drink]] =>

      implicit val sched: Scheduler = Scheduler.fromFixedDaemonPool(corePoolSize = 1)

      val drinks
      : Stream[Task, Option[Drink]]
      = orders
        .through(drinksOnly)
        .flatMap(Stream.emits)
        .through(coldOrHotIndividualDrink)
        .through(createDrinks(Barista("Jack")))
        .noneTerminate              // 1st terminating None
        .append(Stream.emit(None))  // 2nd terminating None

      def marker(str: String) = Stream.eval(Task.delay(System.out.println(Thread.currentThread.getName + " " + str)))

      val waiter1
      : Stream[Task, Unit]
      = q.dequeue.unNoneTerminate.flatMap { d =>
        marker("W1 Serving" + d) ++ time.sleep_[Task](30.millis) ++ marker("W1 Served" + d)
      }

      val waiter2
      : Stream[Task, Unit]
      = q.dequeue.unNoneTerminate.flatMap { d =>
        marker("W2 Serving" + d) ++ time.sleep_[Task](300.millis) ++ marker("W2 Served" + d)
      }

      // Run and collect the results for print out.
      val orderingResult
      : Stream[Task, Unit]
      = drinks.through(q.enqueue) merge waiter1 merge waiter2

      orderingResult
  }

  jdq.run.unsafeRun()

}
