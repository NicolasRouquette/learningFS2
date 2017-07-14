package cafe

import java.lang.{Integer, System}

import fs2._

import scala.{App, Long, StringContext}

object Main2 extends App {

  implicit val S = fs2.Strategy.fromFixedDaemonPool(8, threadName = "worker")

  val norders: Long = Integer.parseInt(args(0)).toLong

  val c = Cafe()
  System.out.println(s"Cafe ${c.name} opened for business")

  // An asynchronous task that asks for a single order in
  // case getOrder() does always not return quickly.
  val askForOrder = Task {
    c.getOrder()
  }

  // A process that repeatedly ask for an order
  val askForOrders = Stream.repeatEval(askForOrder)

  // The process that asks for N orders. Normally the ordering
  // process would end when the Cafe closes. This process
  // enters a Halt(Cause.End) state automatically after the take() reaches
  // its limit.
  val orders = askForOrders.take(norders)

  // Splitting the order means extracting out the drinks.
  val drinksOnly = orders.map(_.items)

  // Run and collect the results for print out.
  val orderingResult = drinksOnly.runLog.unsafeRun()
  orderingResult.foreach(order => System.out.println(s"order: $order"))

}
