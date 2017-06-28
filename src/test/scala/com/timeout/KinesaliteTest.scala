package com.timeout

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import akka.Done
import akka.stream.scaladsl.{Sink, Source}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry
import com.amazonaws.services.kinesis.{AmazonKinesisAsync, AmazonKinesisAsyncClientBuilder}
import org.scalatest.{BeforeAndAfterEach, Suite}

import scala.concurrent.Future
import scala.sys.process._

trait KinesaliteTest extends AkkaStreamsTest with BeforeAndAfterEach { self: Suite =>

  protected val streamName = "test-stream"
  private val kinesalitePort = 5737
  private var kinesalite: Process = _
  var kinesis: AmazonKinesisAsync = _

  override def beforeAll = {
    val output = new ByteArrayOutputStream
    assume(("kinesalite --help" #> output).! == 0, "Kinesalite is installed")
    kinesalite = s"kinesalite --port $kinesalitePort".run

    val endpoint = s"http://localhost:$kinesalitePort"
    kinesis = AmazonKinesisAsyncClientBuilder.standard
      .withEndpointConfiguration(
        new EndpointConfiguration(endpoint, "eu-west-1")
      ).build
  }

  override def beforeEach = {
    kinesis.createStream(streamName, 2)
    Thread.sleep(550) // takes 500ms
  }

  override def afterEach = {
    super.afterEach()
    kinesis.deleteStream(streamName)
    Thread.sleep(550)
  }

  override def afterAll() = {
    super.afterAll()
    kinesalite.destroy
  }

  protected def pushToStream(records: List[(Int, String)]): Future[Done] = {
    implicit val tprr = ToPutRecordsRequest.instance[(Int, String)] { case (shard, data) =>
      new PutRecordsRequestEntry()
        .withData(ByteBuffer.wrap(data.getBytes))
        .withPartitionKey(shard.toString)
    }
    Source(records)
      .via(KinesisGraphStage.withClient[(Int, String)](kinesis, streamName))
      .runWith(Sink.ignore)
  }
}
