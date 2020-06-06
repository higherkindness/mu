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

package higherkindness.mu.kafka.it.example

import cats.effect.{ContextShift, IO, Timer}
import com.typesafe.scalalogging.LazyLogging
import fs2.{Pipe, Stream}
import higherkindness.mu.kafka
import higherkindness.mu.kafka.config.KafkaBrokers
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.{Futures, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.Seconds
import org.scalatest.{time, OneInstancePerTest}

import scala.concurrent.ExecutionContext.global
import scala.concurrent.Promise
import scala.util.Try

class MuKafkaServiceSpec
    extends AnyFlatSpec
    with Matchers
    with Futures
    with ScalaFutures
    with LazyLogging
    with OneInstancePerTest
    with EmbeddedKafka {

  behavior of "mu Kafka consumer And producer."

  // dependencies for mu kafka consumer & producer
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)
  import higherkindness.mu.format.AvroWithSchema._

  // kafka config
  import IntegrationTestConfig.kafka._

  it should "produce and consume UserAdded" in {
    val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

    withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
      implicit val actualBrokers: KafkaBrokers = brokers.copy(list =
        brokers.list.map(broker =>
          broker.copy(port = actualConfig.kafkaPort)
        )
      )

      // messages
      val userAddedMessage: UserAdded = UserAdded("n")
      val userAddedMessageStream: Stream[IO, Option[UserAdded]] =
        Stream(Option(userAddedMessage), None)

      // message processing logic - used here to make the message available for assertion via promise
      val (consumed, putConsumeMessageIntoFuture): (
          Promise[UserAdded],
          Pipe[IO, UserAdded, UserAdded]
      ) = {
        val consumed: Promise[UserAdded] = Promise()

        val processor: Pipe[IO, UserAdded, UserAdded] = _.map { userAdded =>
          logger.info(s"Processing $userAdded")
          if (consumed.isCompleted)
            logger.warn(s"UserAdded message was received more than once")
          else
            consumed.complete(Try(UserAdded("n")))
          userAdded
        }
        (consumed, processor)
      }

      kafka
        .producer(topic, userAddedMessageStream)
        .unsafeRunAsyncAndForget()

      kafka
        .consumer(topic, consumerGroup, putConsumeMessageIntoFuture)
        .unsafeRunAsyncAndForget()

      whenReady(consumed.future, Timeout(time.Span(5, Seconds)))(userAdded =>
        userAdded shouldBe userAddedMessage
      )
    }
  }
}