package com.examples

import org.scalatest.flatspec.AnyFlatSpec
import com.databricks.connect.DatabricksSession

class NycTaxiSpec extends AnyFlatSpec {
  // Use serverless for unit tests
  val spark = DatabricksSession
    .builder()
    .host("https://e2-demo-field-eng.cloud.databricks.com")
    .clusterId("0911-142606-ftn61hl2") // personal compute that I made
//    .serverless()   // This works really well!
    .getOrCreate()

  "totalTrips" should "return fewer trips per zip than the total trips" in {
    val taxis = new NycTaxi(spark)

    val totalTrips = taxis.countTrips()
    assert(totalTrips > 0)

    val tripsPerZip = taxis.countTrips(Option("10003"))
    assert(tripsPerZip > 0)
    assert(tripsPerZip < totalTrips)
  }

  "trips" should "return the correct number of trips" in {
    val taxis = new NycTaxi(spark)
    val trips = taxis.trips(20)
    assert(trips.count() == 20)
  }
}
