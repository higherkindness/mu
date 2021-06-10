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

import java.util.concurrent.{Executor, TimeUnit}
import cats.effect.Sync
import cats.syntax.functor._
import higherkindness.mu.rpc.common.{RpcBaseTestSuite, SC}
import higherkindness.mu.rpc.server.netty._
import io.grpc._
import io.grpc.internal.GrpcUtil
import io.grpc.netty.{GrpcSslContexts, InternalProtocolNegotiators}
import io.grpc.util.MutableHandlerRegistry
import io.netty.channel.ChannelOption
import io.netty.channel.local.LocalServerChannel
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.handler.ssl.SslContext
import org.scalatest.OneInstancePerTest

import scala.jdk.CollectionConverters._
import scala.concurrent.duration.TimeUnit

trait RpcServerTestSuite extends RpcBaseTestSuite with OneInstancePerTest {

  trait DummyData {

    val serverMock: Server                                  = stub[Server]
    val serverCopyMock: Server                              = stub[Server]
    val timeout: Long                                       = 1L
    val timeoutUnit: TimeUnit                               = TimeUnit.MINUTES
    val b: Boolean                                          = true
    val unit: Unit                                          = ()
    val sd1: ServerServiceDefinition                        = ServerServiceDefinition.builder("s1").build()
    val sd2: ServerServiceDefinition                        = ServerServiceDefinition.builder("s2").build()
    val serviceList: List[ServerServiceDefinition]          = List(sd1, sd2)
    val immutableServiceList: List[ServerServiceDefinition] = List(sd1)
    val mutableServiceList: List[ServerServiceDefinition]   = List(sd2)

    (serverMock.start _: () => Server).when().returns(serverCopyMock)
    (serverMock.getPort _: () => Int).when().returns(SC.port)
    (serverMock.getServices _: () => java.util.List[ServerServiceDefinition])
      .when()
      .returns(serviceList.asJava)
    (serverMock.getImmutableServices _: () => java.util.List[ServerServiceDefinition])
      .when()
      .returns(immutableServiceList.asJava)
    (serverMock.getMutableServices _: () => java.util.List[ServerServiceDefinition])
      .when()
      .returns(mutableServiceList.asJava)
    (serverMock.shutdown _: () => Server).when().returns(serverCopyMock)
    (serverMock.shutdownNow _: () => Server).when().returns(serverCopyMock)
    (serverMock.isShutdown _: () => Boolean).when().returns(b)
    (serverMock.isTerminated _: () => Boolean).when().returns(b)
    (serverMock.awaitTermination(_: Long, _: TimeUnit)).when(timeout, timeoutUnit).returns(b)
    (serverMock.awaitTermination _: () => Unit).when().returns(unit)

    import java.io.File

    import io.grpc.internal.testing.TestUtils

    val cert: File         = TestUtils.loadCert("server1.pem")
    val key: File          = TestUtils.loadCert("server1.key")
    val sslCtx: SslContext = GrpcSslContexts.forServer(cert, key).build()

    val grpcAllConfigList: List[GrpcConfig] = List(
      AddService(sd1),
      DirectExecutor,
      SetExecutor(new Executor() {
        override def execute(r: Runnable): Unit =
          throw new RuntimeException("Test executor")
      }),
      AddBindableService(new BindableService {
        override def bindService(): ServerServiceDefinition = sd1
      }),
      AddTransportFilter(new ServerTransportFilter() {
        val key1: Attributes.Key[String] = Attributes.Key.create("key1")
        val key2: Attributes.Key[String] = Attributes.Key.create("key2")
        override def transportReady(attrs: Attributes): Attributes =
          attrs.toBuilder.set(key1, "foo").set(key2, "bar").build()

        override def transportTerminated(attrs: Attributes): Unit = (): Unit
      }),
      AddStreamTracerFactory(new ServerStreamTracer.Factory() {
        override def newServerStreamTracer(fullMethodName: String, headers: Metadata) =
          throw new UnsupportedOperationException
      }),
      SetFallbackHandlerRegistry(new MutableHandlerRegistry),
      UseTransportSecurity(cert, key),
      SetDecompressorRegistry(DecompressorRegistry.getDefaultInstance),
      SetCompressorRegistry(CompressorRegistry.getDefaultInstance),
      // Netty configurations:
      ChannelType((new LocalServerChannel).getClass),
      WithChildOption[Boolean](ChannelOption.valueOf("ALLOCATOR"), true),
      BossEventLoopGroup(new NioEventLoopGroup(0)),
      WorkerEventLoopGroup(new NioEventLoopGroup(0)),
      SetSslContext(sslCtx),
      SetProtocolNegotiator(InternalProtocolNegotiators.tls(sslCtx)),
      MaxConcurrentCallsPerConnection(1000),
      FlowControlWindow(5),
      MaxMessageSize(GrpcUtil.DEFAULT_MAX_MESSAGE_SIZE),
      MaxHeaderListSize(GrpcUtil.DEFAULT_MAX_HEADER_LIST_SIZE),
      KeepAliveTime(GrpcUtil.DEFAULT_KEEPALIVE_TIMEOUT_NANOS, TimeUnit.NANOSECONDS),
      KeepAliveTimeout(GrpcUtil.DEFAULT_KEEPALIVE_TIMEOUT_NANOS, TimeUnit.NANOSECONDS),
      MaxConnectionIdle(1000, TimeUnit.MILLISECONDS),
      MaxConnectionAge(1000, TimeUnit.MILLISECONDS),
      MaxConnectionAgeGrace(1000, TimeUnit.MILLISECONDS),
      PermitKeepAliveTime(1000, TimeUnit.MILLISECONDS),
      PermitKeepAliveWithoutCalls(true)
    )
  }

  object implicits extends Helpers with DummyData {

    def grpcServerHandlerTests[F[_]](implicit F: Sync[F]): GrpcServer[F] = {
      new GrpcServer[F] {

        def start: F[Unit] = F.pure(serverMock.start).void

        def getPort: F[Int] = F.pure(serverMock.getPort)

        def getServices: F[List[ServerServiceDefinition]] =
          F.pure(serverMock.getServices.asScala.toList)

        def getImmutableServices: F[List[ServerServiceDefinition]] =
          F.pure(serverMock.getImmutableServices.asScala.toList)

        def getMutableServices: F[List[ServerServiceDefinition]] =
          F.pure(serverMock.getMutableServices.asScala.toList)

        def shutdown: F[Unit] = F.pure(serverMock.shutdown()).void

        def shutdownNow: F[Unit] = F.pure(serverMock.shutdownNow()).void

        def isShutdown: F[Boolean] = F.pure(serverMock.isShutdown)

        def isTerminated: F[Boolean] = F.pure(serverMock.isTerminated)

        def awaitTerminationTimeout(timeout: Long, unit: TimeUnit): F[Boolean] =
          F.pure(serverMock.awaitTermination(timeout, unit))

        def awaitTermination: F[Unit] = F.pure(serverMock.awaitTermination())

      }
    }
  }
}
