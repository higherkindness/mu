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
package ssl

import java.io.File
import java.security.cert.X509Certificate

import cats.effect.Sync
import cats.effect.std.Dispatcher
import cats.effect.unsafe.implicits.global
import higherkindness.mu.rpc.common._
import higherkindness.mu.rpc.protocol._
import higherkindness.mu.rpc.server.netty.SetSslContext
import higherkindness.mu.rpc.server.{AddService, GrpcConfig, GrpcServer}
import io.grpc.internal.testing.TestUtils
import io.grpc.netty.GrpcSslContexts
import io.netty.handler.ssl.{ClientAuth, SslContext, SslProvider}

object Utils extends CommonUtils {

  object service {

    @service(Avro)
    trait AvroRPCService[F[_]] {
      def unary(a: A): F[C]
    }

    @service(AvroWithSchema)
    trait AvroWithSchemaRPCService[F[_]] {
      def unaryWithSchema(a: A): F[C]

    }

  }

  object handlers {

    object server {

      import database._
      import service._

      class ServerRPCService[F[_]: Sync]
          extends AvroRPCService[F]
          with AvroWithSchemaRPCService[F] {

        def unary(a: A): F[C] = Sync[F].delay(c1)

        def unaryWithSchema(a: A): F[C] = unary(a)

      }

    }

  }

  trait MuRuntime {

    import service._
    import handlers.server._
    import cats.instances.list._
    import cats.syntax.traverse._

    //////////////////////////////////
    // Server Runtime Configuration //
    //////////////////////////////////

    implicit val muRPCHandler: ServerRPCService[ConcurrentMonad] =
      new ServerRPCService[ConcurrentMonad]

    val serverCertFile: File                         = TestUtils.loadCert("server1.pem")
    val serverPrivateKeyFile: File                   = TestUtils.loadCert("server1.key")
    val serverTrustedCaCerts: Array[X509Certificate] = Array(TestUtils.loadX509Cert("ca.pem"))

    val serverSslContext: SslContext =
      GrpcSslContexts
        .configure(
          GrpcSslContexts.forServer(serverCertFile, serverPrivateKeyFile),
          SslProvider.OPENSSL
        )
        .trustManager(serverTrustedCaCerts: _*)
        .clientAuth(ClientAuth.REQUIRE)
        .build()

    val (dispatcher, _) = Dispatcher[ConcurrentMonad].allocated.unsafeRunSync()

    val grpcConfigs: ConcurrentMonad[List[GrpcConfig]] =
      List(
        AvroRPCService.bindService[ConcurrentMonad](dispatcher),
        AvroWithSchemaRPCService.bindService[ConcurrentMonad](dispatcher)
      ).sequence
        .map(_.map(AddService))
        .map(services => SetSslContext(serverSslContext) :: services)

    implicit val grpcServer: GrpcServer[ConcurrentMonad] =
      grpcConfigs.flatMap(GrpcServer.netty[ConcurrentMonad](SC.port, _)).unsafeRunSync()

    //////////////////////////////////
    // Client Runtime Configuration //
    //////////////////////////////////

    // Create a client.
    val clientCertChainFile: File                    = TestUtils.loadCert("client.pem")
    val clientPrivateKeyFile: File                   = TestUtils.loadCert("client.key")
    val clientTrustedCaCerts: Array[X509Certificate] = Array(TestUtils.loadX509Cert("ca.pem"))

    val clientSslContext: SslContext =
      GrpcSslContexts.forClient
        .keyManager(clientCertChainFile, clientPrivateKeyFile)
        .trustManager(clientTrustedCaCerts: _*)
        .build()

  }

  object implicits extends MuRuntime

}
