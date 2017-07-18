package jira

import java.lang.System

import io.circe.Json
import org.http4s._
import org.http4s.client.Client
import org.http4s.client.blaze.{BlazeClientConfig, PooledHttp1Client}
import org.http4s.headers.{Accept, Authorization, `Content-Type`,  `Set-Cookie`}
import _root_.util.Env

import scala.concurrent.duration.Duration
import scala.{App, None, Option, Some, StringContext}
import scala.Predef.identity

object MyIssues extends App {

  implicit val S = fs2.Strategy.fromFixedDaemonPool(8, threadName = "worker")
  implicit val c = System.console()

  val server = Env.lookupEnvOrPrompt("JIRA server")
  val user = Env.lookupEnvOrPrompt("username")
  val pass = Env.lookupEnvOrPromptPassword("password")
  val issueID = Env.lookupEnvOrPrompt("JIRA Issue #")

  val h = Headers(`Content-Type`(MediaType.`text/plain`)) ++ Headers(Authorization(BasicCredentials(user, pass)))

  val httpClient: Client = PooledHttp1Client(config = BlazeClientConfig.insecure.copy(idleTimeout = Duration.Inf))

  val jiraAuth: Uri = Uri.fromString(s"https://$server/rest/auth/1").fold(throw _, identity)
  val jiraApi: Uri = Uri.fromString(s"https://$server/rest/api/2").fold(throw _, identity)

  import org.http4s.circe.jsonDecoder

  val acceptApplicationJson = Headers(Accept(MediaType.`application/json`))

  val hfilter = (h: Headers) => h.filter { h =>
    h.name == `Set-Cookie`.name
  }

  val issueInfo
  : Option[Json]
  = httpClient.fetch(Request(Method.POST, jiraAuth / "session", headers = h)) { resp: Response =>
    val sessionHeaders = resp.headers ++  acceptApplicationJson
    System.out.println(s"Status:\n${resp.status}")
    System.out.println(s"Headers:\n${sessionHeaders.mkString("\n")}")
    httpClient.expect[Json](
      Request(
        Method.GET,
        jiraApi / "issue" / issueID +? ("fields", "*all"),
        headers = sessionHeaders))
      .attemptFold(
        (t: java.lang.Throwable) => {
          System.err.println(s"Error: $t")
          t.printStackTrace(System.err)
          None
        },
        (json: Json) =>
          Some(json)
      )
  }.unsafeRun()

  System.out.println(s"Issue\n$issueInfo")
}
