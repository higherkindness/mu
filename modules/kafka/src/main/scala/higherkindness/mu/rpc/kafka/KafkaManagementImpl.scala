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

import cats.effect.Concurrent
import cats.implicits._
import fs2.kafka._
import higherkindness.mu.rpc.protocol.Empty
import org.apache.kafka.clients.admin.{NewPartitions, NewTopic}

import kafkaManagementService._

class KafkaManagementImpl[F[_]: ContextShift: Concurrent] private[kafka] (
    adminClient: KafkaAdminClient[F]
) extends KafkaManagement[F] {

  override def alterConfigs(acr: AlterConfigsRequest): F[Unit] =
    for {
      configs <-
        acr.configs
          .map(c => ConfigResource.toJava(c.resource) -> c.ops.map(AlterConfigOp.toJava))
          .toMap
          .pure[F]
      alterConfigs <- adminClient.alterConfigs(configs)
    } yield alterConfigs

  override def createPartitions(cpr: CreatePartitionsRequest): F[Unit] =
    adminClient.createPartitions(Map(cpr.name -> NewPartitions.increaseTo(cpr.numPartitions)))

  override def createTopic(ctr: CreateTopicRequest): F[Unit] =
    adminClient.createTopic(new NewTopic(ctr.name, ctr.numPartitions, ctr.replicationFactor))

  override def createTopics(ctrs: CreateTopicRequests): F[Unit] =
    for {
      newTopics <-
        ctrs.createTopicRequests
          .map(ctr => new NewTopic(ctr.name, ctr.numPartitions, ctr.replicationFactor))
          .pure[F]
      _ <- adminClient.createTopics(newTopics)
    } yield ()

  override def deleteTopic(t: DeleteTopicRequest): F[Unit] = adminClient.deleteTopic(t.name)

  override def deleteTopics(ts: DeleteTopicsRequest): F[Unit] = adminClient.deleteTopics(ts.names)

  override def describeCluster(r: Empty.type): F[Cluster] = {
    val dc = adminClient.describeCluster
    (dc.clusterId, dc.controller, dc.nodes).mapN { (id, c, ns) =>
      Cluster(ns.map(Node.fromJava).toList, Node.fromJava(c), id)
    }
  }

  override def describeConfigs(dcr: DescribeConfigsRequest): F[Configs] =
    for {
      kConfigs <- adminClient.describeConfigs(dcr.resources.map(ConfigResource.toJava))
      configs = kConfigs.map { case (cr, ces) =>
        Config(ConfigResource.fromJava(cr), ces.map(ConfigEntry.fromJava))
      }.toList
    } yield Configs(configs)

  override def describeConsumerGroups(dcgr: DescribeConsumerGroupsRequest): F[ConsumerGroups] =
    for {
      kGroups <- adminClient.describeConsumerGroups(dcgr.groupIds)
      groups = kGroups.map { case (gid, cgd) =>
        ConsumerGroup(gid, ConsumerGroupDescription.fromJava(cgd))
      }.toList
    } yield ConsumerGroups(groups)

  override def describeTopics(dtr: DescribeTopicsRequest): F[Topics] =
    for {
      kTopics <- adminClient.describeTopics(dtr.names)
      topics = kTopics.map { case (_, desc) => TopicDescription.fromJava(desc) }.toList
    } yield Topics(topics)

  override def listConsumerGroupOffsets(
      lcgor: ListConsumerGroupOffsetsRequest
  ): F[ConsumerGroupOffsets] =
    for {
      kOffsets <- adminClient.listConsumerGroupOffsets(lcgor.groupId).partitionsToOffsetAndMetadata
      offsets = kOffsets.map { case (topic, offset) =>
        Offset(TopicPartition.fromJava(topic), OffsetAndMetadata.fromJava(offset))
      }.toList
    } yield ConsumerGroupOffsets(offsets)

  override def listConsumerGroups(r: Empty.type): F[ConsumerGroupListings] =
    for {
      kListings <- adminClient.listConsumerGroups.listings
      listings = kListings.map(ConsumerGroupListing.fromJava)
    } yield ConsumerGroupListings(listings)

  override def listTopics(r: Empty.type): F[TopicListings] =
    for {
      kListings <- adminClient.listTopics.includeInternal.listings
      listings = kListings.map(TopicListing.fromJava)
    } yield TopicListings(listings)
}
