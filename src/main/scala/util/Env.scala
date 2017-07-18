package util

import java.io.Console
import java.lang.System
import scala.{None,Option}
import scala.Predef.{charArrayOps,String}

object Env {

  def lookupEnvOrPrompt
  (prop: String, prompt: Option[String]=None)
  (implicit c: Console)
  : String
  = Option.apply(System.getenv(prop))
    .orElse(Option.apply(System.getProperty(prop)))
    .getOrElse {
      System.out.println()
      System.out.print(prompt.getOrElse(prop))
      val r = c.readLine()
      System.out.println()
      r
    }

  def lookupEnvOrPromptPassword
  (prop: String, prompt: Option[String]=None)
  (implicit c: Console)
  : String
  = Option.apply(System.getenv(prop))
    .orElse(Option.apply(System.getProperty(prop)))
    .getOrElse {
      System.out.println()
      System.out.print(prompt.getOrElse(prop))
      val p = c.readPassword().mkString
      System.out.println()
      p
    }
}
