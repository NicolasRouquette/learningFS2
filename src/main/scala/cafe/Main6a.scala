package cafe

import fs2._

import scala.collection.immutable.Seq
import scalaz._

object Main6a extends App {

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

  val drinks
  : Stream[Task, Drink]
  = orders
    .through(drinksOnly)
    .flatMap(Stream.emits)
    .through(coldOrHotIndividualDrink)
    .through(createDrinks(Barista("Jack")))

  val waiter
  : Sink[Task, Drink]
  = _.map { d =>
    System.out.println(s"Serving $d...")
    Thread.sleep(300L)
    System.out.println(s"Served $d")
    ()
  }

  // Run and collect the results for print out.
  val orderingResult
  : Stream[Task, Unit]
  = drinks to waiter

  orderingResult.run.unsafeRun()

}
