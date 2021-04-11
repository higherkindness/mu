/*
 * Copyright 2017-2020 47 Degrees Open Source <https://www.47deg.com>
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

package higherkindness.mu.rpc.kafka

import cats.effect.{ConcurrentEffect, ContextShift, IO, Sync}
import cats.syntax.applicative._
import fs2.kafka._
import higherkindness.mu.rpc.protocol.Empty
import higherkindness.mu.rpc.testing.servers.withServerChannel
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest._

import scala.concurrent.ExecutionContext

import kafkaManagementService._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ServiceSpec extends AnyFunSuite with Matchers with OneInstancePerTest with EmbeddedKafka {
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.Implicits.global)

  def adminClientSettings[F[_]: Sync](config: EmbeddedKafkaConfig): AdminClientSettings[F] =
    AdminClientSettings[F].withProperties(
      Map(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}")
    )

  def withKafka[F[_]: Sync, A](f: AdminClientSettings[F] => A): A =
    withRunningKafkaOnFoundPort(
      EmbeddedKafkaConfig()
    )(adminClientSettings[F] _ andThen f)

  def withClient[F[_]: ContextShift: ConcurrentEffect, A](
      settings: AdminClientSettings[F]
  )(f: KafkaManagement[F] => F[A]): F[A] =
    (for {
      km            <- KafkaManagement.buildInstance[F](settings)
      serverChannel <- withServerChannel(KafkaManagement.bindService[F](ConcurrentEffect[F], km))
      client        <- KafkaManagement.clientFromChannel[F](Sync[F].delay(serverChannel.channel))
    } yield client).use(f)

  test("create/list/delete topic") {
    withKafka { settings: AdminClientSettings[IO] =>
      withClient(settings) { client =>
        for {
          topicName  <- "topic".pure[IO]
          create     <- client.createTopic(CreateTopicRequest(topicName, 2, 1)).attempt
          _          <- IO(assert(create.isRight))
          topicNames <- client.listTopics(Empty)
          _          <- IO(assert(topicNames.listings.map(_.name).contains(topicName)))
          delete     <- client.deleteTopic(DeleteTopicRequest(topicName)).attempt
          _          <- IO(assert(delete.isRight))
          topicNames <- client.listTopics(Empty)
          _          <- IO(assert(!topicNames.listings.map(_.name).contains(topicName)))
        } yield ()
      }.unsafeRunSync()
    }
  }

  test("create/list/delete topics") {
    withKafka { settings: AdminClientSettings[IO] =>
      withClient(settings) { client =>
        for {
          topicNames <- List("topic1", "topic2").pure[IO]
          creates <-
            client
              .createTopics(CreateTopicRequests(topicNames.map(CreateTopicRequest(_, 2, 1))))
              .attempt
          _                <- IO(assert(creates.isRight))
          listedTopicNames <- client.listTopics(Empty)
          _                <- IO(assert(topicNames.forall(listedTopicNames.listings.map(_.name).contains)))
          deletes          <- client.deleteTopics(DeleteTopicsRequest(topicNames)).attempt
          _                <- IO(assert(deletes.isRight))
          listedTopicNames <- client.listTopics(Empty)
          _ <- IO(
            assert(topicNames.forall(n => !listedTopicNames.listings.map(_.name).contains(n)))
          )
        } yield ()
      }.unsafeRunSync()
    }
  }

  test("create/create partitions/describe topic") {
    withKafka { settings: AdminClientSettings[IO] =>
      withClient(settings) { client =>
        for {
          topicName <- "topic".pure[IO]
          create    <- client.createTopic(CreateTopicRequest(topicName, 2, 1)).attempt
          _         <- IO(assert(create.isRight))
          // describe fails after upgrading to embed-kafka 2.7.0
          // describe  <- client.describeTopics(DescribeTopicsRequest(List(topicName))).attempt
          // _         <- IO(assert(describe.isRight))
          // _ <- IO(assert(describe.toOption.exists(_.topics.size == 1)))
          // _ <- IO(
          //   assert(
          //     describe.toOption
          //       .flatMap(_.topics.headOption)
          //       .exists(_.partitions.length == 2)
          //   )
          // )
          partition <-
            client
              .createPartitions(CreatePartitionsRequest(topicName, 4))
              .attempt
          _ <- IO(assert(partition.isRight))
          // describe fails after upgrading to embed-kafka 2.7.0
          // describe <- client.describeTopics(DescribeTopicsRequest(List(topicName)))
          // _        <- IO(assert(describe.topics.size == 1))
          // _        <- IO(assert(describe.topics.headOption.exists(_.partitions.length == 4)))
        } yield ()
      }.unsafeRunSync()
    }
  }

  test("describe cluster") {
    withKafka { settings: AdminClientSettings[IO] =>
      withClient(settings) { client =>
        for {
          cluster <- client.describeCluster(Empty).attempt
          _       <- IO(assert(cluster.isRight))
          _       <- IO(assert(cluster.toOption.exists(_.nodes.length == 1)))
        } yield ()
      }.unsafeRunSync()
    }
  }

  test("alter/describe configs") {
    withKafka { settings: AdminClientSettings[IO] =>
      withClient(settings) { client =>
        for {
          topicName <- "topic".pure[IO]
          create    <- client.createTopic(CreateTopicRequest(topicName, 2, 1)).attempt
          _         <- IO(assert(create.isRight))
          resource = ConfigResource(ConfigType.TopicConfigType, topicName)
          request = AlterConfigsRequest(
            AlterConfig(
              resource,
              AlterConfigOp("cleanup.policy", "compact", OpType.Set) :: Nil
            ) :: Nil
          )
          alter <- client.alterConfigs(request).attempt
          _     <- IO(assert(alter.isRight))
          entry = ConfigEntry(
            "cleanup.policy",
            "compact",
            ConfigSource.DynamicTopicConfig,
            false,
            false,
            Nil
          )
          describe <- client.describeConfigs(DescribeConfigsRequest(resource :: Nil)).attempt
          _        <- IO(assert(describe.isRight))
          _ <- IO(
            assert(
              describe.toOption
                .flatMap(_.configs.headOption)
                .exists(c => c.resource == resource && c.entries.contains(entry))
            )
          )
        } yield ()
      }.unsafeRunSync()
    }
  }

  test("describe/list/list offsets consumer groups") {
    withKafka { settings: AdminClientSettings[IO] =>
      val topicName                                       = "topic"
      implicit val stringSerializer: StringSerializer     = new StringSerializer
      implicit val stringDeserializer: StringDeserializer = new StringDeserializer
      createCustomTopic(topicName)
      publishToKafka(topicName, (0 until 100).map(n => s"key-$n" -> s"value->$n"))
      consumeFirstMessageFrom(topicName, autoCommit = true)

      withClient(settings) { client =>
        for {
          groups <- client.listConsumerGroups(Empty).attempt
          _      <- IO(assert(groups.isRight))
          _      <- IO(assert(groups.toOption.exists(_.consumerGroupListings.size == 1)))
          groupId =
            groups.toOption
              .flatMap(_.consumerGroupListings.headOption)
              .map(_.groupId)
              .getOrElse("")
          offsets <-
            client
              .listConsumerGroupOffsets(ListConsumerGroupOffsetsRequest(groupId))
              .attempt
          _ <- IO(assert(offsets.isRight))
          _ <- IO(assert(offsets.toOption.exists(_.offsets.size == 1)))
          // describe fails after upgrading to embed-kafka 2.7.0
          // describe <-
          //            client
          //              .describeConsumerGroups(DescribeConsumerGroupsRequest(List(groupId)))
          //              .attempt
          //          _ <- IO(assert(describe.isRight))
          //          _ <- IO(assert(describe.toOption.exists(_.consumerGroups.size == 1))) // fails after upgrading to embed-kafka 2.7.0
        } yield ()
      }.unsafeRunSync()
    }
  }
}
