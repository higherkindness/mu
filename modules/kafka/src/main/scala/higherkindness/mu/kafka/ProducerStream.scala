/*
 * Copyright 2017-2020 47 Degrees, LLC. <http://www.47deg.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package higherkindness.mu.kafka

import cats.Functor
import cats.effect._
import fs2._
import cats.effect.std.Queue
import fs2.kafka._
import higherkindness.mu.format.Serialiser
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object ProducerStream {
  def pipe[F[_]: Async, A](
      topic: String,
      settings: fs2.kafka.ProducerSettings[F, String, Array[Byte]]
  )(implicit
      encoder: Serialiser[A]
  ): fs2.Stream[F, Option[A]] => fs2.Stream[F, ByteArrayProducerResult] =
    as =>
      for {
        implicit0(logger: Logger[F]) <- fs2.Stream.eval(Slf4jLogger.create[F])
        s                            <- apply(fs2.kafka.KafkaProducer.pipe(settings))(topic, as.unNoneTerminate)
      } yield s

  def apply[F[_]: Async, A](
      topic: String,
      queue: Queue[F, Option[A]],
      settings: fs2.kafka.ProducerSettings[F, String, Array[Byte]]
  )(implicit
      encoder: Serialiser[A]
  ): Stream[F, ByteArrayProducerResult] =
    for {
      implicit0(logger: Logger[F]) <- fs2.Stream.eval(Slf4jLogger.create[F])
      s <- apply(fs2.kafka.KafkaProducer.pipe(settings))(
        topic,
        Stream.fromQueueNoneTerminated(queue)
      )
    } yield s

  private[kafka] def apply[F[_]: Functor: Logger, A](
      publishToKafka: PublishToKafka[F]
  )(topic: String, stream: Stream[F, A])(implicit
      serialiser: Serialiser[A]
  ): Stream[F, ByteArrayProducerResult] =
    stream
      .evalTap(a => Logger[F].info(s"Dequeued $a"))
      .map(a =>
        ProducerRecords
          .one(
            ProducerRecord(topic, "dummy-key", serialiser.serialise(a))
          ) // TODO key generation and propagation
      )
      .through(publishToKafka)
      .evalTap(result =>
        Logger[F].info(
          result.records.head
            .fold("Error: ProducerResult contained empty records.")(a => s"Published $a")
        )
      )
}
