package crawler

import java.lang.{System,Thread}
import java.nio.charset.StandardCharsets

import fs2.{Chunk, Pipe, Scheduler, Stream, Task, async}
import io.circe.Json
import org.http4s.{Headers, Method, Request, Uri}
import org.http4s.client.Client

import scala.collection.immutable.Seq
import scala.{Boolean, Either, Int, Right, StringContext, Unit}
import scala.Predef.intWrapper

object ParallelBatchFetcher {

  /**
    * Fetch a starting set of root IDs and for a batch of IDs, fetch the corresponding elements
    * and continue with the IDs of the element dependencies until all referenced IDs have been fetched.
    *
    * @param fetchRootIDsURI The URI to GET the starting set of root IDs
    * @param fetchIDBatchURI The URI to POST a query to fetch elements for a given batch of IDs
    * @param batchSize The maximum size of a batch of IDs or 0 for fetching all at once
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
   maxSize: Int = 100000,
   h: Headers,
   httpClient: Client,
   json2rootIDs: (Json) => Seq[ID],
   json2elementMapDependencies: Json => (Seq[Element], Seq[ID]))
  (implicit S: fs2.Strategy)
  : Either[java.lang.Throwable, Seq[Element]] = {

    import org.http4s.circe.jsonDecoder

    val resultQueue
    : Task[async.mutable.Queue[Task, Element]]
    = async.unboundedQueue[Task, Element]

    val idQueue
    : Task[async.mutable.Queue[Task, ID]]
    = async.boundedQueue[Task, ID](maxSize = maxSize)

    val remainingToFetch
    : Task[async.mutable.Semaphore[Task]]
    = async.semaphore[Task](1L)

    val allIDsFetched
    : Task[async.mutable.Signal[Task, Boolean]]
    = async.mutable.Signal(false)

    val rq
    : Stream[Task, Element]
    = Stream.eval(resultQueue).flatMap {
      elements: async.mutable.Queue[Task, Element] =>
        val fetchAll
        : Stream[Task, Unit]
        = Stream.eval(idQueue).flatMap {
          q: async.mutable.Queue[Task, ID] =>

            Stream.eval(remainingToFetch).flatMap {
              as: async.mutable.Semaphore[Task] =>

                Stream.eval(allIDsFetched).flatMap {
                  finished: async.mutable.Signal[Task, Boolean] =>

                    implicit val sched: Scheduler = Scheduler.fromFixedDaemonPool(corePoolSize = 1)

                    val enqueueIDs
                    : Pipe[Task, ID, Unit]
                    = _.flatMap { id =>
                      Stream.eval {
                        for {
                          _ <- as.increment
                          _ <- q.enqueue1(id)
                        } yield ()
                      }
                    }

                    val enqueueElement
                    : Pipe[Task, Element, Unit]
                    = _.flatMap { element =>
                      Stream.eval {
                        for {
                          _ <- elements.enqueue1(element)
                          _ <- as.decrement
                          n <- as.available
                          _ <- finished.set(n == 0)
                        } yield ()
                      }
                    }

                    def extractElementsAndIDs
                    (json: Json)
                    : Stream[Task, Unit]
                    = {
                      val (inc, more) = json2elementMapDependencies(json)

                      val elementsAdded
                      : Stream[Task, Unit]
                      = Stream.emits(inc) through enqueueElement

                      val idsAdded
                      : Stream[Task, Unit]
                      = Stream.emits(more) through enqueueIDs

                      val info
                      : Stream[Task, Unit]
                      = Stream.eval {
                        for {
                          n <- as.available
                          s <- elements.size.get
                          _ <- Task delay System.out.println(
                            f"${Thread.currentThread.getName} (elements: $s%7d) => Remaining = ${if (n == 0) "Finished!" else n} with ${inc.size} elements and ${more.size} new IDs")
                        } yield ()
                      }

                      elementsAdded.drain merge idsAdded.drain merge info.drain
                    }

                    val workers
                    = for {
                      i <- 1 to nbWorkers
                      worker = Stream.repeatEval {
                        for {
                          ids <- q.dequeueBatch1(batchSize)
                          json <- httpClient
                            .expect[Json](
                            Request(Method.POST,
                              fetchIDBatchURI,
                              headers = h,
                              body =
                                Stream.chunk(Chunk.bytes(ids.toList.mkString(",").getBytes(StandardCharsets.UTF_8)))))
                          _ <- extractElementsAndIDs(json).run
                        } yield ()
                      }
                    } yield worker

                    val rootIDs
                    : Stream[Task, Unit]
                    = for {
                      json <- Stream.eval(httpClient.expect[Json](Request(Method.GET, fetchRootIDsURI, headers = h)))
                      ids = json2rootIDs(json)
                      _ <- Stream.emits(ids) through enqueueIDs
                    } yield ()

                    (rootIDs.drain merge workers.reduce(_ merge _).drain).interruptWhen(finished)
                }
            }
        }

        fetchAll >> elements.dequeue
    }

    Right(rq.runLog.unsafeRun())
  }

}

