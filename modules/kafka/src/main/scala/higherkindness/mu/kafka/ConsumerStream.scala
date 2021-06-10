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

import cats.Monad
import cats.effect.Async
import fs2.Stream
import fs2.kafka.{ConsumerSettings, KafkaConsumer}
import higherkindness.mu.format.Deserialiser
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object ConsumerStream {

  def apply[F[_]: Async, A](topic: String, settings: ConsumerSettings[F, String, Array[Byte]])(
      implicit decoder: Deserialiser[A]
  ): Stream[F, A] =
    for {
      implicit0(logger: Logger[F]) <- fs2.Stream.eval(Slf4jLogger.create[F])
      s                            <- apply(fs2.kafka.KafkaConsumer.stream(settings))(topic)
    } yield s

  private[kafka] def apply[F[_]: Monad, A](
      kafkaConsumerStream: Stream[F, KafkaConsumer[F, String, Array[Byte]]]
  )(topic: String)(implicit
      decoder: Deserialiser[A],
      logger: Logger[F]
  ): Stream[F, A] =
    kafkaConsumerStream
      .evalTap(_.subscribeTo(topic))
      .flatMap(
        _.stream
          .map(message => decoder.deserialise(message.record.value))
          .evalTap(a => logger.info(a.toString))
      )

}
