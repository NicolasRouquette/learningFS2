package cafe

import java.lang.{Integer, System, Thread}

import fs2._

import scalaz._
import scala.collection.immutable.Seq
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.{App, Boolean, Int, Long, None, Some, StringContext, Unit}
import scala.Predef.String

object Main6g extends App {
  implicit val S: Strategy = Strategy.fromFixedDaemonPool(8, threadName = "worker")

  implicit val sched: Scheduler = Scheduler.fromFixedDaemonPool(corePoolSize = 1)

  val norders: Long = if (args.length == 2) Integer.parseInt(args(0)).toLong else 10L
  val qsize: Int = if (args.length == 2) Integer.parseInt(args(1)) else 3

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
  : Task[async.mutable.Queue[Task, Drink]]
  = async.boundedQueue[Task, Drink](maxSize = qsize)

  // number of remaining drinks to be served
  // Incremented when a drink is prepared
  // Decremented when a drink is served or when there are no more drinks to serve
  // If a customer asks to redo a drink, the semaphore is unchanged but the drink is enqueued again.
  val remainingToServe
  : Task[async.mutable.Semaphore[Task]]
  = async.semaphore[Task](1L)

  // All drinks are served when the semaphore `remainingToServe` reaches zero
  val allDrinksServed
  : Task[async.mutable.Signal[Task, Boolean]]
  = async.mutable.Signal(false)

  val jdq = Stream.eval(joinDrinkQueue).flatMap {
    q: async.mutable.Queue[Task, Drink] =>

      Stream.eval(remainingToServe).flatMap {
        as: async.mutable.Semaphore[Task] =>

          Stream.eval(allDrinksServed).flatMap {
            finished: async.mutable.Signal[Task, Boolean] =>

              val drinks
              : Stream[Task, Drink]
              = orders
                .through(drinksOnly)
                .flatMap(Stream.emits)
                .through(coldOrHotIndividualDrink)
                .through(createDrinks(Barista("Jack")))

              def marker(str: String)
              : Stream[Task, Unit]
              = Stream.eval(Task.delay(System.out.println(Thread.currentThread.getName + " " + str)))

              def serveOrRedo(name: String, delay: FiniteDuration)(d: Drink)
              : Stream[Task, Unit]
              = if (d.redo)
                Stream.eval {
                  for {
                    _ <- Task.delay(System.out.println(s"${Thread.currentThread.getName} $name Redo: $d"))
                    _ <- q.enqueue1(d.copy(redo = false))
                  } yield ()
                }
              else {
                marker(s"$name Serving $d") ++
                  time.sleep_[Task](delay) ++
                  marker(s"$name Served $d") ++
                  Stream.eval {
                    for {
                      _ <- as.decrement
                      n <- as.available
                      _ <- finished.set(n == 0)
                      _ <- Task delay System.out.println(s"${Thread.currentThread.getName} $name Remaining = ${if (n == 0) "Finished!" else n}")
                    } yield ()
                  }
              }

              val waiter1
              : Stream[Task, Unit]
              = q.dequeue.flatMap(serveOrRedo("W1", 30.millis))

              val waiter2
              : Stream[Task, Unit]
              = q.dequeue.flatMap(serveOrRedo("W2", 300.millis))

              // Run and collect the results for print out.
              val orderingResult
              : Stream[Task, Unit]
              = drinks
                .noneTerminate
                .flatMap {
                  case Some(d) =>
                    Stream.eval {
                      for {
                        _ <- Task delay System.out.println(s"${Thread.currentThread.getName} Queueing: $d")
                        _ <- as.increment
                        _ <- q.enqueue1(d)
                      } yield ()
                    }
                  case None =>
                    Stream.eval {
                      for {
                        _ <- Task delay System.out.println(s"${Thread.currentThread.getName} No more drinks!")
                        _ <- as.decrement
                      } yield ()
                    }
                } merge waiter1 merge waiter2

              orderingResult.interruptWhen(finished.discrete.repeat)
          }
      }
  }

  jdq.run.unsafeRun()

}
