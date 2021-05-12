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

package higherkindness.mu.rpc.healthcheck.unary.handler

import cats.Functor
import cats.effect.Sync
import cats.implicits._
import higherkindness.mu.rpc.healthcheck.unary.serviceUnary.HealthCheckServiceUnary
import higherkindness.mu.rpc.protocol.Empty
import cats.effect.Ref

object HealthService {

  def buildInstance[F[_]: Sync]: F[HealthCheckServiceUnary[F]] =
    Ref
      .of[F, Map[String, ServerStatus]](Map.empty[String, ServerStatus])
      .map(new HealthCheckServiceUnaryImpl[F](_))

}

abstract class AbstractHealthService[F[_]: Functor](
    checkStatus: Ref[F, Map[String, ServerStatus]]
) {

  def check(service: HealthCheck): F[ServerStatus] =
    checkStatus.modify(m => (m, m.getOrElse(service.nameService, ServerStatus("UNKNOWN"))))

  def clearStatus(service: HealthCheck): F[Unit] =
    checkStatus.update(_ - service.nameService)

  def checkAll(empty: Empty.type): F[AllStatus] = {
    val _ = empty // just to suppress compiler warning about unused parameter
    checkStatus.get
      .map(_.map(p => HealthStatus(new HealthCheck(p._1), p._2)).toList)
      .map(AllStatus)
  }

  def cleanAll(empty: Empty.type): F[Unit] = {
    val _ = empty // just to suppress compiler warning about unused parameter
    checkStatus.set(Map.empty[String, ServerStatus])
  }

}

class HealthCheckServiceUnaryImpl[F[_]: Functor](checkStatus: Ref[F, Map[String, ServerStatus]])
    extends AbstractHealthService[F](checkStatus)
    with HealthCheckServiceUnary[F] {

  def setStatus(newStatus: HealthStatus): F[Unit] =
    checkStatus
      .update(_ + (newStatus.hc.nameService -> newStatus.status))

}
