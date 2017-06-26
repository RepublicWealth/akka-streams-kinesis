package com.timeout

import java.nio.ByteBuffer
import java.time.{Clock, ZonedDateTime}
import java.util.Date

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStageLogic.EagerTerminateOutput
import akka.stream.stage._
import akka.stream.{Attributes, Outlet, SourceShape}
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.model.{Shard => _, _}
import com.amazonaws.services.kinesis.{AmazonKinesisAsync, AmazonKinesisAsyncClientBuilder}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

object KinesisSource {


  private [timeout] val region: Regions =
    Option(Regions.getCurrentRegion)
      .map(r => Regions.fromName(r.getName))
      .getOrElse(Regions.EU_WEST_1)

  private[timeout] lazy val kinesis: AmazonKinesisAsync =
    AmazonKinesisAsyncClientBuilder.standard.withRegion(region).build


  private [timeout] case class ShardIterator(
    shard: Shard,
    iterator: String,
    reissue: GetShardIteratorRequest
  )

  /**
    * Given a response from kinesis and the current shard iterator, prepare a new iterator.
    * The new iterator needs to have a reissue command that can reproduce it if it expires.
    * To do this we either use an AFTER_SEQUENCE_NUMBER request if we have records, or
    * just use the last reissue if we don't (which I can't see ever happening)
    */
  private [timeout] def nextIterator(
    s: ShardIterator,
    g: GetRecordsResult
  )(implicit c: Clock): ShardIterator = {
    val reissue = g.getRecords.asScala.lastOption.fold(s.reissue) { lastRecord =>
      s.reissue
        .withShardIteratorType("AFTER_SEQUENCE_NUMBER")
        .withStartingSequenceNumber(lastRecord.getSequenceNumber)
    }
    ShardIterator(s.shard, g.getNextShardIterator, reissue)
  }

  /**
    * This creates a source that reads records from AWS Kinesis.
    * It is serialisation format agnostic so emits a stream of ByteBuffers
    */
  def apply(
    stream: String,
    since: ZonedDateTime
  )(
    implicit
    ec: ExecutionContext,
    clock: Clock = Clock.systemUTC
  ): Source[ByteBuffer, NotUsed] =
    Source.fromGraph(new KinesisSource(stream, since))

  /**
    * Construct shard iterator requests
    * based on a stream description
    */
  private[timeout] def shardIteratorRequests(
    since: ZonedDateTime,
    shards: List[Shard],
    stream: String
  )(
    implicit
    clock: Clock
  ): List[(Shard, GetShardIteratorRequest)] =
    shards.map { shard =>
      val now = clock.instant
      val readFrom = if (since.toInstant.isBefore(now)) since.toInstant else now
      shard -> new GetShardIteratorRequest()
        .withShardIteratorType("AT_TIMESTAMP")
        .withTimestamp(Date.from(readFrom))
        .withStreamName(stream)
        .withShardId(shard.id)
  }
}

/**
  * A source for kinesis records
  */
private[timeout] class KinesisSource(
  streamName: String,
  since: ZonedDateTime
)(
  implicit
  e: ExecutionContext,
  clock: Clock
) extends GraphStage[SourceShape[ByteBuffer]] {

  import KinesisSource._
  val outlet = Outlet[ByteBuffer]("Kinesis Records")
  override def shape = SourceShape[ByteBuffer](outlet)

  override def createLogic(attrs: Attributes) = new GraphStageLogic(shape) with StageLogging {
    setHandler(outlet, EagerTerminateOutput)

    /**
      * Adapt Amazon's 2 argument AsyncHandler based functions to execute a block on completion,
      * using Akka Streams' threadsafe getAsyncCallback function
      *
      * In most case the request argument is the same as the request the AsyncHandler gives you
      * but describeStreamAsync lets you pass in a string stream name, so we need a different type
      */
    private def run[A, Req <: AmazonWebServiceRequest, Resp](
      requestArgument: A
    )(
      amazonAsyncFunction: (A, AsyncHandler[Req, Resp]) => Any
    )(
      whenDone: Try[Resp] => Unit
    ) = {
      val callback = getAsyncCallback[Try[Resp]](whenDone)
      val handler = new AsyncHandler[Req, Resp] {
        override def onError(exception: Exception) = callback.invoke(Failure(exception))
        override def onSuccess(request: Req, result: Resp) = callback.invoke(Success(result))
      }
      amazonAsyncFunction(requestArgument, handler)
    }

    private def beginReadingFromShards(shards: List[Shard]): Unit =
      shardIteratorRequests(since, shards, streamName).foreach { case (shard, request) =>
        run(request)(kinesis.getShardIteratorAsync) { iteratorResult =>
          Option(iteratorResult.get.getShardIterator).fold {
            log.warning(s"$streamName: No iterator for $shard")
          } { it =>
            log.debug(s"$streamName: Beginning to read from ${shard.id}")
            getRecords(ShardIterator(shard, it, request))
          }
        }
      }

    /**
      * bootstrap everything by getting initial shard iterators
      * Any errors here are essentially unrecoverable so we explode, hence the .gets
      */
    override def preStart() = {
      run(streamName)(kinesis.describeStreamAsync) { stream =>
        val awsShards = stream.get.getStreamDescription.getShards.asScala.toList
        val shards = Shard.fromAws(awsShards)
        log.debug(s"Found shards: $shards")
        beginReadingFromShards(shards)
      }
    }

    /**
      * Get records from Kinesis, then call handleResult
      * to deal with errors or emitting the results
      */
    //noinspection AccessorLikeMethodIsUnit
    private def getRecords(it: ShardIterator): Unit = {
      val request = new GetRecordsRequest().withShardIterator(it.iterator)
      run(request)(kinesis.getRecordsAsync)(handleResult(it))
    }

    /**
      * Given a result from getRecords, emit it
      * then call getRecords again when we're finished
      */
    private def emitThenGetRecords(currentIterator: ShardIterator, result: GetRecordsResult): Unit = {
      emitMultiple[ByteBuffer](outlet, result.getRecords.asScala.map(_.getData).toList, { () =>
        Option(result.getNextShardIterator).fold {
          log.debug(s"${currentIterator.shard.id} has been closed")
          beginReadingFromShards(currentIterator.shard.children)
        } { _ =>
          getRecords(nextIterator(currentIterator, result))
        }
      })
    }

    /**
      * Given a shard iterator, reissue it
      * then call getRecords with the new iterator
      */
    private def reissueThenGetRecords(iterator: ShardIterator): Unit = {
      log.debug(s"$streamName - reissuing shard iterator")
      run(iterator.reissue)(kinesis.getShardIteratorAsync) { r =>
        getRecords(iterator.copy(iterator = r.get.getShardIterator))
      }
    }

    /**
      * Handle the results of a Kinesis GetRecords call by dispatching
      * to the above functions dependent on what happened.
      */
    private def handleResult(iterator: ShardIterator)(res: Try[GetRecordsResult]) = res match {
      case Success(recordsResult) =>
        emitThenGetRecords(iterator, recordsResult)
      case Failure(_: ExpiredIteratorException) =>
        reissueThenGetRecords(iterator)
      case Failure(error) =>
        log.error(error.getMessage)
        getRecords(iterator)
    }
  }
}
