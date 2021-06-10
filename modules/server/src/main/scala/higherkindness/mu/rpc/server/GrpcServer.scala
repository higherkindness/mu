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

package higherkindness.mu.rpc
package server

import cats.~>
import cats.effect.{Async, Resource, Sync}
import cats.syntax.flatMap._
import cats.syntax.functor._
import io.grpc.{Server, ServerBuilder, ServerServiceDefinition}
import io.grpc.netty.NettyServerBuilder
import scala.concurrent.duration.TimeUnit

trait GrpcServer[F[_]] { self =>

  def start: F[Unit]

  def getPort: F[Int]

  def getServices: F[List[ServerServiceDefinition]]

  def getImmutableServices: F[List[ServerServiceDefinition]]

  def getMutableServices: F[List[ServerServiceDefinition]]

  def shutdown: F[Unit]

  def shutdownNow: F[Unit]

  def isShutdown: F[Boolean]

  def isTerminated: F[Boolean]

  def awaitTerminationTimeout(timeout: Long, unit: TimeUnit): F[Boolean]

  def awaitTermination: F[Unit]

  def mapK[G[_]](fk: F ~> G): GrpcServer[G] =
    new GrpcServer[G] {
      def start: G[Unit] = fk(self.start)

      def getPort: G[Int] = fk(self.getPort)

      def getServices: G[List[ServerServiceDefinition]] = fk(self.getServices)

      def getImmutableServices: G[List[ServerServiceDefinition]] = fk(self.getImmutableServices)

      def getMutableServices: G[List[ServerServiceDefinition]] = fk(self.getMutableServices)

      def shutdown: G[Unit] = fk(self.shutdown)

      def shutdownNow: G[Unit] = fk(self.shutdownNow)

      def isShutdown: G[Boolean] = fk(self.isShutdown)

      def isTerminated: G[Boolean] = fk(self.isTerminated)

      def awaitTerminationTimeout(timeout: Long, unit: TimeUnit): G[Boolean] =
        fk(self.awaitTerminationTimeout(timeout, unit))

      def awaitTermination: G[Unit] = fk(self.awaitTermination)
    }
}

object GrpcServer {

  def defaultServer[F[_]: Async](
      port: Int,
      configList: List[GrpcConfig]
  ): Resource[F, GrpcServer[F]] =
    Resource.eval(default[F](port, configList)).flatMap(s => serverResource(s).as(s))

  /**
   * Build a Resource that starts the given [[GrpcServer]] before use,
   * and shuts it down afterwards.
   */
  def serverResource[F[_]](S: GrpcServer[F])(implicit F: Async[F]): Resource[F, Unit] =
    Resource.make(S.start)(_ => S.shutdown >> S.awaitTermination)

  /**
   * Start the given server and keep it running forever.
   */
  def server[F[_]](S: GrpcServer[F])(implicit F: Async[F]): F[Unit] =
    serverResource[F](S).use(_ => F.never[Unit])

  /**
   * Build a [[GrpcServer]] that uses the default network transport layer.
   *
   * The transport layer will be Netty, unless you have written your own
   * `io.grpc.ServerProvider` implementation and added it to the classpath.
   */
  def default[F[_]](port: Int, configList: List[GrpcConfig])(implicit
      F: Sync[F]
  ): F[GrpcServer[F]] =
    buildServer[F](ServerBuilder.forPort(port), configList).map(fromServer[F])

  /**
   * Build a [[GrpcServer]] that uses the Netty network transport layer.
   */
  def netty[F[_]](port: Int, configList: List[GrpcConfig])(implicit F: Sync[F]): F[GrpcServer[F]] =
    netty(ChannelForPort(port), configList)

  /**
   * Build a [[GrpcServer]] that uses the Netty network transport layer.
   */
  def netty[F[_]](channelFor: ChannelFor, configList: List[GrpcConfig])(implicit
      F: Sync[F]
  ): F[GrpcServer[F]] =
    for {
      builder <- F.delay(nettyBuilder(channelFor))
      server  <- F.delay(buildNettyServer(builder, configList))
    } yield fromServer[F](server)

  /**
   * Helper to convert an `io.grpc.Server` into a [[GrpcServer]].
   */
  def fromServer[F[_]: Sync](server: Server): GrpcServer[F] =
    handlers.GrpcServerHandler[F].mapK(λ[GrpcServerOps[F, *] ~> F](_.run(server)))

  private[this] def buildServer[F[_]: Sync](
      bldr: ServerBuilder[SB] forSome { type SB <: ServerBuilder[SB] },
      configList: List[GrpcConfig]
  ): F[Server] = Sync[F].delay {
    configList
      .foldLeft(bldr)((bldr, cfg) => SBuilder(bldr)(cfg))
      .build()
  }

  private[this] def buildNettyServer(
      bldr: NettyServerBuilder,
      configList: List[GrpcConfig]
  ): Server = {
    configList
      .foldLeft(bldr)((bldr, cfg) => (SBuilder(bldr) orElse NettySBuilder(bldr))(cfg))
      .build()
  }

  private[this] def nettyBuilder(initConfig: ChannelFor): NettyServerBuilder =
    initConfig match {
      case ChannelForPort(port)        => NettyServerBuilder.forPort(port)
      case ChannelForSocketAddress(sa) => NettyServerBuilder.forAddress(sa)
      case e =>
        throw new IllegalArgumentException(s"ManagedChannel not supported for $e")
    }

}
