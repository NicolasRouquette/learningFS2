package cafe

import fs2._

import scala.collection.immutable.Seq
import scalaz._

object Main4 extends App {

  implicit val S = fs2.Strategy.fromFixedDaemonPool(8, threadName = "worker")

  val norders: Long = Integer.parseInt(args(0)).toLong

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
  : Stream[Task, Seq[Item]]
  = orders.map(_.items)

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

  val printer
  : Pipe[Task, \/[Item,Item], String]
  = { in =>
    in.map {
      case -\/(i) =>
        s"Left: $i"
      case \/-(i) =>
        s"Right: $i"
    }
  }

  val items
  : Stream[Task, String]
  = drinksOnly.flatMap(Stream.emits).through(coldOrHotIndividualDrink).flatMap(ii => printer(Stream.emit(ii)))

  val waiter
  : Sink[Task, String]
  = { in =>
    in.map { message =>
      System.out.println(message)
      ()
    }
  }

  // Run and collect the results for print out.
  val orderingResult = items to waiter
  orderingResult.run.unsafeRun()

}
