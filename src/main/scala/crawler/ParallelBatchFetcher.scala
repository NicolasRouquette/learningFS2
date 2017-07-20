package crawler

import java.lang.{System, Thread}
import java.nio.charset.StandardCharsets

import fs2.{Chunk, Scheduler, Strategy, Stream, Task, async}
import io.circe.Json
import org.http4s.{Headers, Method, Request, Uri}
import org.http4s.client.Client

import scala.collection.immutable.{Seq,Vector}
import scala.concurrent.duration.FiniteDuration
import scala.{Boolean, Int, StringContext, Unit}
import scala.Predef.{String, intWrapper}

object ParallelBatchFetcher {

  /**
    * Fetch a starting set of root IDs and for a batch of IDs, fetch the corresponding elements
    * and continue with the IDs of the element dependencies until all referenced IDs have been fetched.
    *
    * @param fetchRootIDsURI The URI to GET the starting set of root IDs
    * @param fetchIDBatchURI The URI to POST a query to fetch elements for a given batch of IDs
    * @param batchSize The maximum size of a batch of IDs or 0 for fetching all at once
    * @param batchDelay A small delay to wait before repeatedly checking whether the queue is empty
    * @param h Headers (content, authorization, ...)
    * @param httpClient http4s client for executing GET and POST requests
    * @param json2rootIDs Scan the Json result of the fetchRootIDsURI query for root IDs
    * @param json2elementMapDependencies Scan the Json result of the fetchIDBatchURI query for elements and dependencies
    * @param S fs2 strategy
    * @tparam ID type
    * @tparam Element type
    * @return Error or the map of all elements fetched
    */
  def fetchRecursively[ID, Element]
  (fetchRootIDsURI: Uri,
   fetchIDBatchURI: Uri,
   nbWorkers: Int,
   batchSize: Int,
   batchDelay: FiniteDuration,
   maxSize: Int = 10000000,
   h: Headers,
   httpClient: Client,
   json2rootIDs: (Json) => Seq[ID],
   json2elementMapDependencies: Json => (Seq[Element], Seq[ID]))
  (implicit S: Strategy, sched: Scheduler)
  : Task[Vector[Element]] = {

    import org.http4s.circe.jsonDecoder

    // Queue of fetched Elements.
    val fechedElements
    : Task[async.mutable.Queue[Task, Element]]
    = async.boundedQueue[Task, Element](maxSize = maxSize)

    // Queue of IDs to be fetched
    val idQueue
    : Task[async.mutable.Queue[Task, ID]]
    = async.boundedQueue[Task, ID](maxSize = maxSize)

    // Count of IDs that have been requested and that should eventually result in corresponding Elements
    val remainingToReceive
    : Task[async.mutable.Semaphore[Task]]
    = async.semaphore[Task](0L)

    // true when there there are no IDs to be fetched and all requested IDs have been received.
    val allIDsFetched
    : Task[async.mutable.Signal[Task, Boolean]]
    = async.mutable.Signal(false)

    // Show progress and check if the work is done.
    def updateStats
    (es: async.mutable.Queue[Task, Element],
     as: async.mutable.Semaphore[Task],
     q: async.mutable.Queue[Task, ID],
     finished: async.mutable.Signal[Task, Boolean],
     prefix: String, suffix: String)
    : Task[Unit]
    = for {
      n <- as.available
      qs <- q.size.get
      nes <- es.size.get
      done = n == 0 && qs == 0
      _ =
      System.out.println(
        f"${Thread.currentThread.getName} $prefix (elements: $nes%7d, queue: $qs%7d, remaining: $n%7d)${if (done) "=> Finished!" else " "}$suffix")
      _ <- finished.set(done)
    } yield ()

    // Request elements for a batch of IDs and scan for new IDs to fetch
    def fetchElements
    (es: async.mutable.Queue[Task, Element],
     as: async.mutable.Semaphore[Task],
     q: async.mutable.Queue[Task, ID],
     finished: async.mutable.Signal[Task, Boolean],
     ids: Seq[ID])
    : Task[Unit]
    = if (ids.isEmpty)
      Task(())
    else
      for {
        _ <- as.incrementBy(ids.size.toLong)
        _ <- updateStats(es, as, q, finished, "Batch:  ", s" => Fetch ${ids.size} IDs...")

        json <- httpClient
          .expect[Json](
          Request(Method.POST,
            fetchIDBatchURI,
            headers = h,
            body =
              Stream.chunk(Chunk.bytes(ids.toList.mkString(",").getBytes(StandardCharsets.UTF_8)))))

        (els: Seq[Element], more: Seq[ID]) = json2elementMapDependencies(json)
        _ <- updateStats(es, as, q, finished, "Result: ", s" => Got ${els.size} elements and ${more.size} new IDs")
        _ <- Stream.emits(more).to(q.enqueue).drain.run
        _ <- Stream.emits(els).to(es.enqueue).drain.run
        _ <- as.decrementBy(ids.size.toLong)
        _ <- updateStats(es, as, q, finished, "Next:   ", "")
      } yield ()

    def createWorkerPool
    (es: async.mutable.Queue[Task, Element],
     as: async.mutable.Semaphore[Task],
     q: async.mutable.Queue[Task, ID],
     finished: async.mutable.Signal[Task, Boolean])
    : Stream[Task, Unit]
    = {
      val ws
      = for {
        i <- 1 to nbWorkers
        worker = Stream.repeatEval {
          for {
            qs <- q.size.get
            ids <- if (qs > 0) q.dequeueBatch1(batchSize).map(_.toVector) else Task.schedule(Seq.empty[ID], batchDelay)
            _ <- fetchElements(es, as, q, finished, ids)
          } yield ()
        }
      } yield worker

      ws.reduce(_ merge _)
    }

    val result
    : Stream[Task, Element]
    = Stream.eval(fechedElements).flatMap { es =>

      val fetchAll
      : Stream[Task, Unit]
      = for {
        q <- Stream.eval(idQueue)
        as <- Stream.eval(remainingToReceive)
        finished <- Stream.eval(allIDsFetched)
        _ <- {
          val workers: Stream[Task, Unit] = createWorkerPool(es, as, q, finished)
          val roots: Stream[Task, Unit] = Stream.eval {
            httpClient.expect[Json](Request(Method.GET, fetchRootIDsURI, headers = h)).flatMap { json =>
              val ids = json2rootIDs(json)
              for {
                _ <- Stream.emits(ids).to(q.enqueue).drain.run
                _ <- updateStats(es, as, q, finished, "Roots:  ", "")
              } yield ()
            }
          }
          (roots merge workers.drain).interruptWhen(finished)
        }
      } yield ()

      fetchAll.drain >> es.dequeue
    }

    result.runLog
  }

}
