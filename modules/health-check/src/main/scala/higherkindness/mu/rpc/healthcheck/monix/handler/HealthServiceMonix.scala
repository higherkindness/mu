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

package higherkindness.mu.rpc.healthcheck.monix.handler

import cats.effect.Sync
import cats.implicits._
import higherkindness.mu.rpc.healthcheck.monix.serviceMonix.HealthCheckServiceMonix
import higherkindness.mu.rpc.healthcheck.unary.handler._
import higherkindness.mu.rpc.healthcheck.ordering._
import monix.execution.Scheduler
import monix.reactive.{MulticastStrategy, Observable, Observer, Pipe}
import cats.effect.Ref

object HealthServiceMonix {

  def buildInstance[F[_]: Sync](implicit s: Scheduler): F[HealthCheckServiceMonix[F]] = {

    val checkRef: F[Ref[F, Map[String, ServerStatus]]] =
      Ref.of[F, Map[String, ServerStatus]](Map.empty[String, ServerStatus])

    val (observer, observable): (Observer.Sync[HealthStatus], Observable[HealthStatus]) =
      Pipe(
        MulticastStrategy
          .behavior(HealthStatus(new HealthCheck("FirstStatus"), ServerStatus("UNKNOWN")))
      ).concurrent(s)

    checkRef.map(c => new HealthCheckServiceMonixImpl[F](c, observer, observable))
  }
}

class HealthCheckServiceMonixImpl[F[_]: Sync](
    checkStatus: Ref[F, Map[String, ServerStatus]],
    observer: Observer.Sync[HealthStatus],
    observable: Observable[HealthStatus]
) extends AbstractHealthService[F](checkStatus)
    with HealthCheckServiceMonix[F] {

  override def setStatus(newStatus: HealthStatus): F[Unit] =
    checkStatus
      .update(_ + (newStatus.hc.nameService -> newStatus.status)) <*
      Sync[F].delay(observer.onNext(newStatus))

  override def watch(service: HealthCheck): F[Observable[HealthStatus]] =
    observable.filter(_.hc === service).pure[F]

}
