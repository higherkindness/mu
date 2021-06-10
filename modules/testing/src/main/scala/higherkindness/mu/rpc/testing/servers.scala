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
package testing

import java.util.UUID

import cats.effect.{Resource, Sync}
import cats.syntax.all._
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.internal.NoopServerCall.NoopServerCallListener
import io.grpc.util.MutableHandlerRegistry
import io.grpc._

object servers {

  def serverCallHandler[Req, Res]: ServerCallHandler[Req, Res] =
    new ServerCallHandler[Req, Res] {
      override def startCall(
          call: ServerCall[Req, Res],
          headers: Metadata
      ): ServerCall.Listener[Req] = new NoopServerCallListener[Req]
    }

  def serverServiceDefinition(
      serviceName: String,
      methodList: List[String]
  ): ServerServiceDefinition = {
    val ssdBuilder = ServerServiceDefinition.builder(serviceName)
    methodList.foreach { methodName =>
      ssdBuilder.addMethod(methods.voidMethod(Some(methodName)), serverCallHandler[Void, Void])
    }
    ssdBuilder.build()
  }

  def withServerChannel[F[_]: Sync](
      service: Resource[F, ServerServiceDefinition],
      clientInterceptor: Option[ClientInterceptor] = None
  ): Resource[F, ServerChannel] =
    withServerChannelList(service.map(List(_)), clientInterceptor)

  def withServerChannelList[F[_]: Sync](
      services: Resource[F, List[ServerServiceDefinition]],
      clientInterceptor: Option[ClientInterceptor] = None
  ): Resource[F, ServerChannel] =
    services.flatMap(ServerChannel.fromList(_, clientInterceptor))

  final case class ServerChannel(server: Server, channel: ManagedChannel)

  object ServerChannel {

    def apply[F[_]: Sync](
        serverServiceDefinition: ServerServiceDefinition,
        clientInterceptor: Option[ClientInterceptor] = None
    ): Resource[F, ServerChannel] =
      ServerChannel.fromList[F](List(serverServiceDefinition), clientInterceptor)

    def fromList[F[_]](
        serverServiceDefinitions: List[ServerServiceDefinition],
        clientInterceptor: Option[ClientInterceptor] = None
    )(implicit F: Sync[F]): Resource[F, ServerChannel] = {

      val setup = for {
        serviceRegistry <- F.delay(new MutableHandlerRegistry)
        serverName      <- F.delay(UUID.randomUUID.toString)

        serverBuilder <- F.delay {
          InProcessServerBuilder
            .forName(serverName)
            .fallbackHandlerRegistry(serviceRegistry)
            .directExecutor()
        }
        channelBuilder <- F.delay(InProcessChannelBuilder.forName(serverName))
        _ <- F.delay(clientInterceptor.foreach { interceptor =>
          channelBuilder.intercept(interceptor)
        })
        _ <- F.delay(serverServiceDefinitions.map(serverBuilder.addService))
      } yield (serverBuilder, channelBuilder)

      Resource.make {
        setup.flatMap { case (sb, cb) =>
          (
            F.delay(sb.build().start()),
            F.delay(cb.directExecutor.build())
          ).mapN(ServerChannel(_, _))
        }
      }(sc => F.delay(sc.server.shutdown()).void)
    }

  }

}
