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

import cats.Applicative
import cats.effect.Ref
import cats.effect.kernel.Concurrent
import cats.syntax.all._
import fs2.Stream
import fs2.concurrent.SignallingRef
import higherkindness.mu.rpc.healthcheck.fs2.serviceFS2.HealthCheckServiceFS2
import higherkindness.mu.rpc.healthcheck.ordering._
import higherkindness.mu.rpc.healthcheck.unary.handler._

object HealthServiceFS2 {

  def buildInstance[F[_]: Concurrent]: F[HealthCheckServiceFS2[F]] = {

    val checkRef: F[Ref[F, Map[String, ServerStatus]]] =
      Ref.of[F, Map[String, ServerStatus]](Map.empty[String, ServerStatus])

    for {
      c <- checkRef
      t <- SignallingRef[F, HealthStatus](
        HealthStatus(new HealthCheck("FirstStatus"), ServerStatus("UNKNOWN"))
      )
    } yield new HealthCheckServiceFS2Impl[F](c, t)
  }
}

class HealthCheckServiceFS2Impl[F[_]: Applicative](
    checkStatus: Ref[F, Map[String, ServerStatus]],
    watchTopic: SignallingRef[F, HealthStatus]
) extends AbstractHealthService[F](checkStatus)
    with HealthCheckServiceFS2[F] {

  def setStatus(newStatus: HealthStatus): F[Unit] =
    checkStatus.update(_ + (newStatus.hc.nameService -> newStatus.status)) <*
      watchTopic.set(newStatus)

  def watch(service: HealthCheck): F[Stream[F, HealthStatus]] =
    watchTopic.discrete.debug().filter(_.hc === service).pure[F]
}
