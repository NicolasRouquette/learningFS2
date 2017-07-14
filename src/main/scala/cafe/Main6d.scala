package cafe

import java.lang.{Integer, Thread, System}

import fs2._

import scalaz._
import scala.collection.immutable.Seq
import scala.{App, Int, Long, None, Option, Some, StringContext, Unit}
import scala.Predef.String

object Main6d extends App {

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
  : Stream[Task, Option[Order]]
  = askForOrders.take(norders).noneTerminate

  // Splitting the order means extracting out the drinks.
  val drinksOnly
  : Pipe[Task, Option[Order], Seq[Option[Item]]]
  = { in =>
    in.map {
      case Some(o) =>
        o.items.map(Some(_))
      case None =>
        Seq(None)
    }
  }

  // Based on "iced", map to left or right. Use \/ (disjunction).
  // Each drink order is broken out individually.
  val coldOrHotIndividualDrink
  : Pipe[Task, Option[Item], Option[\/[Item, Item]]]
  = { in =>
    in.map {
      case Some(item) =>
        Some(
          if (item.iced)
            -\/(item)
          else
            \/-(item))
      case None =>
        None
    }
  }

  def createDrinks(barista: Barista)
  : Pipe[Task, Option[\/[Item, Item]], Option[Drink]]
  = { in => in.map {
    case Some(-\/(item)) =>
      Some(barista.prepareColdDrink(item))
    case Some(\/-(item)) =>
      Some(barista.prepareHotDrink(item))
    case None =>
      None
  }}

  val printer
  : Pipe[Task, Drink, String]
  = { in =>
    in.map { _.toString }
  }

  val joinDrinkQueue
  : Task[async.mutable.Queue[Task, Option[Drink]]]
  = async.boundedQueue[Task, Option[Drink]](maxSize = qsize)

  val jdq = Stream.eval(joinDrinkQueue).flatMap {
    q: async.mutable.Queue[Task, Option[Drink]] =>

      val items
      : Stream[Task, Unit]
      = orders
        .through(drinksOnly)
        .flatMap(Stream.emits)
        .through(coldOrHotIndividualDrink)
        .through(createDrinks(Barista("Jack")))
        //.through { in => q.enqueue(in) }
        .flatMap { d => Stream.eval(q.enqueue1(d)) }

      val waiter
      : Stream[Task, Unit]
      = q.dequeue.unNoneTerminate.map { d =>
          System.out.println(s"Serving $d...")
          Thread.sleep(300L)
          System.out.println(s"Served $d")
          ()
      }

      // Run and collect the results for print out.
      val orderingResult: Stream[Task, Unit] = items merge waiter

      orderingResult
  }

  jdq.run.unsafeRun()

}
