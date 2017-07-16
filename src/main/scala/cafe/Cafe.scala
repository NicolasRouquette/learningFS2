package cafe

import java.lang.{System,Thread}
import java.util.concurrent.{ atomic => juca }

import scala.collection.immutable.{Map,Seq}
import scala.Predef.{intWrapper,ArrowAssoc,String}
import scala.{Boolean,Int,StringContext}

sealed trait DrinkType
case object Espresso extends DrinkType
case object Latte extends DrinkType
case object Cappuccino extends DrinkType
case object Mocha extends DrinkType

object DrinkType {
  private val indices = Map(0 -> Espresso, 1 -> Latte, 2 -> Cappuccino, 3 -> Mocha)
  private val random = scala.util.Random
  def randomDrink() = indices(random.nextInt(3))
}

case class Drink(
                  onumber: Int,
                  drinkType: DrinkType,
                  shots: Int,
                  iced: Boolean,
                  redo: Boolean)

case class Item(
                 onumber: Int,
                 drinkType: DrinkType,
                 shots: Int = 1,
                 iced: Boolean = false,
                 redo: Boolean = false)

case class Order(
                  number: Int,
                  items: Seq[Item])

case class Cafe(
                 name: String = "MyBucks") {
  import DrinkType._

  private val orders = new juca.AtomicInteger(0)
  private val random = scala.util.Random

  def getOrder(): Order = {
    val orderNum = orders.addAndGet(1)
    val nbItems = 1 + random.nextInt(5)
    val items = for {
      _ <- 0 to nbItems
      i = Item(orderNum, randomDrink(), random.nextInt(3), random.nextBoolean(), random.nextBoolean())
    } yield i

    Order(orderNum, items)
  }

}

case class Barista(name: String) {
  val hotDrinkCounter = new juca.AtomicInteger(0)
  val coldDrinkCounter = new juca.AtomicInteger(0)

  val hotDrinkDelay = 200L
  val coldDrinkDelay = 10L

  def prepareHotDrink(item: Item): Drink = {
    Thread.sleep(hotDrinkDelay)
    System.out.println(s"""$name: ${Thread.currentThread.getName} prepared hot drink #${hotDrinkCounter.getAndIncrement} for order #${item.onumber}: $item""")
    Drink(item.onumber, item.drinkType, item.shots, item.iced, item.redo)
  }
  def prepareColdDrink(item: Item): Drink = {
    Thread.sleep(coldDrinkDelay)
    System.out.println(s"""$name: ${Thread.currentThread.getName} prepared cold drink #${hotDrinkCounter.getAndIncrement} for order #${item.onumber}: $item""")
    Drink(item.onumber, item.drinkType, item.shots, item.iced, item.redo)
  }
}