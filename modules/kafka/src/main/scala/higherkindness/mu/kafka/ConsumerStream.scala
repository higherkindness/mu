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

import cats.effect.{ConcurrentEffect, Sync}
import fs2.Stream
import fs2.kafka.{ConsumerSettings, KafkaConsumer}
import higherkindness.mu.format.Deserialiser
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.effect.Temporal

object ConsumerStream {

  def apply[F[_]: Sync, A](topic: String, settings: ConsumerSettings[F, String, Array[Byte]])(
      implicit
      concurrentEffect: ConcurrentEffect[F],
      timer: Temporal[F],
      decoder: Deserialiser[A]
  ): Stream[F, A] =
    for {
      implicit0(logger: Logger[F]) <- fs2.Stream.eval(Slf4jLogger.create[F])
      s                            <- apply(fs2.kafka.KafkaConsumer.stream(settings))(topic)
    } yield s

  private[kafka] def apply[F[_], A](
      kafkaConsumerStream: Stream[F, KafkaConsumer[F, String, Array[Byte]]]
  )(topic: String)(implicit
      concurrentEffect: ConcurrentEffect[F],
      decoder: Deserialiser[A],
      logger: Logger[F]
  ): Stream[F, A] =
    kafkaConsumerStream
      .evalTap(_.subscribeTo(topic))
      .flatMap(
        _.stream
          .flatMap { message =>
            val a = decoder.deserialise(message.record.value)
            for {
              _ <- fs2.Stream.eval(logger.info(a.toString))
            } yield a
          }
      )

}
