package com.timeout

import java.time.{Clock, ZonedDateTime}
import java.util.Date

import org.scalatest.{FreeSpec, Matchers}

class KinesisSourceTest extends FreeSpec with Matchers {
  val now = ZonedDateTime.parse("2017-01-01T07:00:00Z")
  implicit val clock = Clock.fixed(now.toInstant, now.getZone)
  val stream = "test"
  val shards = List(
      Shard("1234", List.empty),
      Shard("2345", List.empty)
    )

  "Kinesis Source" - {

    "Should generate one AT_TIMESTAMP iterator request per shard" in {
      val requests = KinesisSource.shardIteratorRequests(now, shards, stream)
      requests.map(_._2.getShardId).toSet shouldEqual Set("1234", "2345")
      requests.map(_._2.getShardIteratorType).toSet shouldEqual Set("AT_TIMESTAMP")
      requests.map(_._2.getTimestamp).toSet shouldEqual Set(Date.from(clock.instant))
    }

    "Should cap the timestamp of shard iterator requests to the minimum of (now, since)" in {
      val requests = KinesisSource.shardIteratorRequests(now.plusDays(1), shards, stream)
      requests.map(_._2.getTimestamp).toSet shouldEqual Set(Date.from(clock.instant))
    }
  }
}
