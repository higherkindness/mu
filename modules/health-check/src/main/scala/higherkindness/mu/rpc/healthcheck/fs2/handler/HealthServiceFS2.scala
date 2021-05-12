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

package higherkindness.mu.rpc.healthcheck.fs2.handler

import cats.effect.{Concurrent, Sync}
import higherkindness.mu.rpc.healthcheck.ordering._
import cats.implicits._
import fs2.Stream
import fs2.concurrent.Topic
import higherkindness.mu.rpc.healthcheck.fs2.serviceFS2.HealthCheckServiceFS2
import higherkindness.mu.rpc.healthcheck.unary.handler._
import cats.effect.Ref

object HealthServiceFS2 {

  def buildInstance[F[_]: Concurrent]: F[HealthCheckServiceFS2[F]] = {

    val checkRef: F[Ref[F, Map[String, ServerStatus]]] =
      Ref.of[F, Map[String, ServerStatus]](Map.empty[String, ServerStatus])

    val watchTopic: F[Topic[F, HealthStatus]] = Topic(
      HealthStatus(new HealthCheck("FirstStatus"), ServerStatus("UNKNOWN"))
    )

    for {
      c <- checkRef
      t <- watchTopic
    } yield new HealthCheckServiceFS2Impl[F](c, t)
  }
}
class HealthCheckServiceFS2Impl[F[_]: Sync](
    checkStatus: Ref[F, Map[String, ServerStatus]],
    watchTopic: Topic[F, HealthStatus]
) extends AbstractHealthService[F](checkStatus)
    with HealthCheckServiceFS2[F] {

  def setStatus(newStatus: HealthStatus): F[Unit] =
    checkStatus
      .update(_ + (newStatus.hc.nameService -> newStatus.status)) <*
      Stream.eval(watchTopic.publish1(newStatus)).compile.drain

  def watch(service: HealthCheck): F[Stream[F, HealthStatus]] =
    watchTopic.subscribe(100).filter(_.hc === service).pure[F]
}
