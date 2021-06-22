package serenadeserving.utils

import scala.io.Source

class RealtimeFeeder(filename: String, skipHeader: Boolean) extends Iterator[Map[String, Any]] {

  private var hasNextLine: Boolean = true

  println("RealtimeFeeder start loading: " + filename)
  private val src = Source.fromFile(filename).getLines
  if (skipHeader) {
    println("RealtimeFeeder skipping header")
    src.take(1).next
  }
  // parse first record
  val line = src.next()
  val Array(sessionId, itemId, time) = line.split("\t")
  private var evolvingSession = new SerenadeSession(sessionId=sessionId, itemId=itemId.toLong)

  override def hasNext(): Boolean = {
    hasNextLine
  }

  override def next(): Map[String, Any] = {
    while (src.hasNext) {
      val line = src.next()
      val Array(sessionId, itemId, time) = line.split("\t")
      if (sessionId != evolvingSession.getSessionId()) {
        val result = createResult(evolvingSession)

        evolvingSession = new SerenadeSession(sessionId=sessionId, itemId=itemId.toLong)
        return result
      } else {
        evolvingSession.productids += itemId.toLong
      }
    }
    hasNextLine = false
    createResult(evolvingSession)
  }

  private def createResult(session: SerenadeSession): Map[String, Any] = {
    val result = collection.mutable.Map[String, Any]()
    result += ("customer_id" -> session.getSessionId())
    result += ("product_ids" -> session.productids.toSeq)
    result.toMap
  }
}