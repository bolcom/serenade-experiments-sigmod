package serenadeserving

import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._

class SerenadeSimilation extends Simulation {
  val baseUrl = "http://127.0.0.1:8080" // http://127.0.0.1:8080/recommend/144/9200000033418652/

  val timeSortedFile = "../datasets/2021-04-05/bolcom-clicks-100m_test.txt"
  var realtime_feeder = tsv(timeSortedFile).circular

  val httpConfiguration = http.baseUrl(baseUrl)
    .acceptHeader("application/json")
    .contentTypeHeader("application/json")
    .shareConnections
    .warmUp(baseUrl)

  val scn = scenario("Load test serenade http endpoint")
    .feed(realtime_feeder)
    .exec(
      http("get serenade recommendation")
        .get("/v1/recommend")
        .queryParam("session_id", "${SessionId}")
        .queryParam("item_id", "${ItemId}")
        .queryParam("user_consent", "true")
    )

  setUp(
    scn.inject(
      nothingFor(1.seconds),
      constantUsersPerSec(100) during (3.minutes)
    )
  ).protocols(httpConfiguration)
}
