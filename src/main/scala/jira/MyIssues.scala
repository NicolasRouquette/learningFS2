package jira

import java.lang.System
import java.nio.charset.StandardCharsets

import util.Env
import io.circe.Json
import org.http4s._
import org.http4s.client.{Client}
import org.http4s.client.blaze.{BlazeClientConfig, PooledHttp1Client}
import org.http4s.headers.{Accept, Authorization, `Content-Type`, `Set-Cookie`}
import fs2.{Chunk, Stream}

import scala.concurrent.duration.Duration
import scala.{App, StringContext}
import scala.Predef.identity

object MyIssues extends App {

  implicit val S = fs2.Strategy.fromFixedDaemonPool(8, threadName = "worker")
  implicit val c = System.console()

  val server = Env.lookupEnvOrPrompt("JIRA server")
  val user = Env.lookupEnvOrPrompt("username")
  val pass = Env.lookupEnvOrPromptPassword("password")
  val issueID = Env.lookupEnvOrPrompt("JIRA Issue #")

  val acceptApplicationJson = Headers(Accept(MediaType.`application/json`))

  val contentTypeJson = Headers(`Content-Type`(MediaType.`application/json`, Charset.`UTF-8`))

  val h = Headers(Authorization(BasicCredentials(user, pass)))

  val httpClient: Client = PooledHttp1Client(config = BlazeClientConfig.insecure.copy(idleTimeout = Duration.Inf))

  val jiraAuth: Uri = Uri.fromString(s"https://$server/rest/auth/1").fold(throw _, identity)
  val jiraApi: Uri = Uri.fromString(s"https://$server/rest/api/2").fold(throw _, identity)

  import org.http4s.circe.jsonDecoder

  import io.circe.syntax._
  import io.circe.generic.auto._

  val credentials: Json = jira.Credentials(username = user, password = pass).asJson

  val issueInfo
  : Json
  = httpClient.fetch(
    Request(
      Method.POST,
      jiraAuth / "session",
      headers = h ++ contentTypeJson,
      body = Stream.chunk(Chunk.bytes(credentials.toString.getBytes(StandardCharsets.UTF_8))))) { resp =>

    val cookies = resp.headers.filter(_.name == `Set-Cookie`.name).map { h =>
      headers.Cookie(headers.`Set-Cookie`.parse(h.value).fold(throw _, identity).cookie)
    }

    httpClient.expect[Json](
      Request(
        Method.GET,
        jiraApi / "issue" / issueID +? ("fields", "*all"),
        headers = cookies))

  }.unsafeRun()

  System.out.println(s"Issue\n$issueInfo")
}
