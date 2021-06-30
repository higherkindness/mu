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

package higherkindness.mu.rpc.channel.cache

import cats.effect.{Async, Clock, Ref, Resource}
import cats.effect.implicits._
import cats.syntax.all._
import fs2.Stream
import org.log4s.{getLogger, Logger}

import scala.concurrent.duration.{Duration, FiniteDuration}

trait ClientCache[Client[_[_]], F[_]] {

  def getClient: F[Client[F]]

}

object ClientCache {

  private val logger: Logger = getLogger

  def fromResource[Client[_[_]], F[_]: Async, H](
      getKey: F[H],
      createClient: H => Resource[F, Client[F]],
      tryToRemoveUnusedEvery: FiniteDuration,
      removeUnusedAfter: FiniteDuration
  ): Stream[F, ClientCache[Client, F]] =
    impl(getKey, (h: H) => createClient(h).allocated, tryToRemoveUnusedEvery, removeUnusedAfter)

  def impl[Client[_[_]], F[_], H](
      getKey: F[H],
      createClient: H => F[(Client[F], F[Unit])],
      tryToRemoveUnusedEvery: FiniteDuration,
      removeUnusedAfter: FiniteDuration
  )(implicit F: Async[F]): Stream[F, ClientCache[Client, F]] = {

    type UnixMillis = Duration
    final case class ClientMeta(client: Client[F], close: F[Unit], lastAccessed: UnixMillis)
    type State = (Map[H, ClientMeta], UnixMillis)

    val nowUnix: F[UnixMillis] =
      Clock[F].realTime.widen[Duration]

    def create(ref: Ref[F, State]): ClientCache[Client, F] =
      new ClientCache[Client, F] {
        val getClient: F[Client[F]] = for {
          key      <- getKey
          now      <- nowUnix
          (map, _) <- ref.get
          client <-
            map
              .get(key)
              .fold {
                createClient(key).flatMap { case (client, close) =>
                  F.delay(logger.info(s"Created new RPC client for $key")) *>
                    ref
                      .update(_.leftMap(_ + (key -> ClientMeta(client, close, now))))
                      .as(client)
                }
              }(clientMeta =>
                F.delay(logger.debug(s"Reuse existing RPC client for $key")) *>
                  ref
                    .update(_.leftMap(_.updated(key, clientMeta.copy(lastAccessed = now))))
                    .as(clientMeta.client)
              )

          (_, lastClean) <- ref.get
          _ <- F.whenA(lastClean < (now - tryToRemoveUnusedEvery))(
            cleanup(ref, _.lastAccessed < (now - removeUnusedAfter)).start
          )
        } yield client
      }

    def cleanup(ref: Ref[F, State], canBeRemoved: ClientMeta => Boolean): F[Unit] =
      for {
        now <- nowUnix
        change <- ref.modify { case (map, _) =>
          val (remove, keep) = map.partition { case (_, clientMeta) => canBeRemoved(clientMeta) }
          ((keep, now), remove)
        }
        noLongerUsed = change.values.toList
        _ <- noLongerUsed.traverse_(_.close)
        _ <- F.delay(logger.info(s"Removed ${noLongerUsed.length} RPC clients from cache."))
      } yield ()

    val refState: F[Ref[F, State]] =
      nowUnix.tupleLeft(Map.empty[H, ClientMeta]).flatMap(Ref.of[F, State])

    Stream.bracket(refState)(cleanup(_, _ => true)).map(create)
  }

}
