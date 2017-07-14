package cafe

import java.lang.{Integer, Thread, System}

import fs2._

import scalaz._
import scala.collection.immutable.Seq
import scala.{App, Int, Long, StringContext, Unit}
import scala.Predef.String

object Main6c extends App {

  implicit val S = fs2.Strategy.fromFixedDaemonPool(8, threadName = "worker")

  val norders: Long = Integer.parseInt(args(0)).toLong
  val qsize: Int = Integer.parseInt(args(1))

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
  = { in =>
    in.map { o =>
      o.items
    }
  }

  // Based on "iced", map to left or right. Use \/ (disjunction).
  // Each drink order is broken out individually.
  val coldOrHotIndividualDrink
  : Pipe[Task, Item, \/[Item, Item]]
  = { in =>
    in.map { item =>
      if (item.iced) -\/(item)
      else \/-(item)
    }
  }

  def createDrinks(barista: Barista)
  : Pipe[Task, \/[Item, Item], Drink]
  = { in => in.map {
    case -\/(item) =>
      barista.prepareColdDrink(item)
    case \/-(item) =>
      barista.prepareHotDrink(item)
  }}

  val printer
  : Pipe[Task, Drink, String]
  = { in =>
    in.map { _.toString }
  }

  val joinDrinkQueue
  : Task[async.mutable.Queue[Task, Drink]]
  = async.boundedQueue[Task, Drink](maxSize = qsize)

  val available
  : Task[async.mutable.Semaphore[Task]]
  = async.semaphore[Task](0L)

  val jdq = Stream.eval(joinDrinkQueue).flatMap {
    q: async.mutable.Queue[Task, Drink] =>

      Stream.eval(available).flatMap {
        as: async.mutable.Semaphore[Task] =>

          val items
          : Stream[Task, Unit]
          = orders
            .through(drinksOnly)
            .flatMap(Stream.emits)
            .through(coldOrHotIndividualDrink)
            .through(createDrinks(Barista("Jack")))
            //.through { in => q.enqueue(in) }
            .flatMap { d =>
            Stream.eval(q.enqueue1(d) flatMap { _ => as.incrementBy(1L) })
            }

          val waiter
          : Stream[Task, Unit]
          = Stream.repeatEval(as.decrement).flatMap { _ =>
            q.dequeue.map { d =>
              System.out.println(s"Serving $d...")
              Thread.sleep(300L)
              System.out.println(s"Served $d")
              ()
            }
          }

          // Run and collect the results for print out.
          val orderingResult: Stream[Task, Unit] = items mergeHaltL waiter

          orderingResult
      }
  }

  jdq.run.unsafeRun()

}
