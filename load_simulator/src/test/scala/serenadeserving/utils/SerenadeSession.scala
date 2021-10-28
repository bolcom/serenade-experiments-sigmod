package serenadeserving.utils

import scala.collection.mutable.ListBuffer

class SerenadeSession(sessionId: String, itemId: Long) {
  val productids= new ListBuffer[Long]()
  productids += itemId.toLong

  def getSessionId(): String = {
    sessionId
  }

  def addItemId(itemId:Long): Unit = {
    productids += itemId
  }
}
