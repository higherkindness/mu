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
package fs2

import higherkindness.mu.rpc.common._
import higherkindness.mu.rpc.protocol._
import _root_.fs2._
import cats.effect.{Async, IO, Resource, Sync}
import cats.effect.std.Dispatcher
import cats.effect.unsafe.implicits.global
import _root_.io.grpc.{CallOptions, Status}
import cats.syntax.applicative._

object Utils extends CommonUtils {

  object service {

    @service(Protobuf) trait ProtoRPCService[F[_]] {
      def serverStreamingWithError(e: E): F[Stream[F, C]]
      def clientStreaming(oa: Stream[F, A]): F[D]
      def serverStreaming(b: B): F[Stream[F, C]]
    }

    @service(Protobuf, Gzip) trait CompressedProtoRPCService[F[_]] {
      def serverStreamingCompressed(b: B): F[Stream[F, C]]
      def clientStreamingCompressed(oa: Stream[F, A]): F[D]
    }

    @service(Avro) trait AvroRPCService[F[_]] {
      def unary(a: A): F[C]
      def biStreaming(oe: Stream[F, E]): F[Stream[F, E]]
    }

    @service(Avro, Gzip) trait CompressedAvroRPCService[F[_]] {
      def unaryCompressed(a: A): F[C]
      def biStreamingCompressed(oe: Stream[F, E]): F[Stream[F, E]]
    }

    @service(AvroWithSchema) trait AvroWithSchemaRPCService[F[_]] {
      def unaryWithSchema(a: A): F[C]
      def biStreamingWithSchema(oe: Stream[F, E]): F[Stream[F, E]]
    }

    @service(AvroWithSchema, Gzip) trait CompressedAvroWithSchemaRPCService[F[_]] {
      def unaryCompressedWithSchema(a: A): F[C]
      def biStreamingCompressedWithSchema(oe: Stream[F, E]): F[Stream[F, E]]
    }

    // this companion objects are here to make sure @service supports
    // companion objects
    object ProtoRPCService          {}
    object AvroRPCService           {}
    object AvroWithSchemaRPCService {}

  }

  object handlers {

    object server {

      import database._
      import service._

      class ServerRPCService[F[_]: Async]
          extends ProtoRPCService[F]
          with AvroRPCService[F]
          with AvroWithSchemaRPCService[F]
          with CompressedProtoRPCService[F]
          with CompressedAvroRPCService[F]
          with CompressedAvroWithSchemaRPCService[F] {

        def unary(a: A): F[C] = Sync[F].delay(c1)

        def unaryWithSchema(a: A): F[C] = unary(a)

        def unaryCompressed(a: A): F[C] = unary(a)

        def unaryCompressedWithSchema(a: A): F[C] = unaryCompressed(a)

        def serverStreaming(b: B): F[Stream[F, C]] = {
          debug(s"[fs2 - SERVER] b -> $b")
          Stream.fromIterator(cList.iterator, 1).pure[F]
        }

        def serverStreamingWithError(e: E): F[Stream[F, C]] = {
          val stream: Stream[F, C] = e.foo match {
            case "SE" =>
              Stream.raiseError(Status.INVALID_ARGUMENT.withDescription(e.foo).asException)
            case "SRE" =>
              Stream.raiseError(Status.INVALID_ARGUMENT.withDescription(e.foo).asRuntimeException)
            case "RTE" =>
              Stream.raiseError(new IllegalArgumentException(e.foo))
            case _ =>
              sys.error(e.foo)
          }
          stream.pure[F]
        }

        def serverStreamingCompressed(b: B): F[Stream[F, C]] = serverStreaming(b)

        def clientStreaming(oa: Stream[F, A]): F[D] =
          oa.compile.fold(D(0)) { case (current, a) =>
            debug(s"[fs2 - SERVER] Current -> $current / a -> $a")
            D(current.bar + a.x + a.y)
          }

        def clientStreamingCompressed(oa: Stream[F, A]): F[D] = clientStreaming(oa)

        def biStreaming(oe: Stream[F, E]): F[Stream[F, E]] =
          oe.flatMap { e: E =>
            save(e)
            Stream.fromIterator(eList.iterator, 1)
          }.pure[F]

        def biStreamingWithSchema(oe: Stream[F, E]): F[Stream[F, E]] = biStreaming(oe)

        def biStreamingCompressed(oe: Stream[F, E]): F[Stream[F, E]] = biStreaming(oe)

        def biStreamingCompressedWithSchema(oe: Stream[F, E]): F[Stream[F, E]] =
          biStreamingCompressed(oe)

        def save(e: E): E = e // do something else with e?

      }

    }

  }

  trait MuRuntime {

    import service._
    import handlers.server._
    import higherkindness.mu.rpc.server._
    import cats.instances.list._
    import cats.syntax.traverse._

    //////////////////////////////////
    // Server Runtime Configuration //
    //////////////////////////////////

    implicit val muRPCHandler: ServerRPCService[IO] =
      new ServerRPCService[IO]

    val (dispatcher, _) = Dispatcher[ConcurrentMonad].allocated.unsafeRunSync()

    val grpcConfigs: IO[List[GrpcConfig]] = List(
      ProtoRPCService.bindService[IO](dispatcher),
      AvroRPCService.bindService[IO](dispatcher),
      AvroWithSchemaRPCService.bindService[IO](dispatcher),
      CompressedProtoRPCService.bindService[IO](dispatcher),
      CompressedAvroRPCService.bindService[IO](dispatcher),
      CompressedAvroWithSchemaRPCService.bindService[IO](dispatcher)
    ).sequence.map(_.map(AddService))

    implicit val grpcServer: GrpcServer[IO] =
      grpcConfigs.flatMap(createServerConf[IO]).unsafeRunSync()

    //////////////////////////////////
    // Client Runtime Configuration //
    //////////////////////////////////

    val muProtoRPCServiceClient: Resource[IO, ProtoRPCService[IO]] =
      ProtoRPCService.client[IO](createChannelFor)

    val muAvroRPCServiceClient: Resource[IO, AvroRPCService[IO]] =
      AvroRPCService.client[IO](createChannelFor)

    val muAvroWithSchemaRPCServiceClient: Resource[IO, AvroWithSchemaRPCService[IO]] =
      AvroWithSchemaRPCService.client[IO](createChannelFor)

    val muCompressedProtoRPCServiceClient: Resource[IO, CompressedProtoRPCService[IO]] =
      CompressedProtoRPCService.client[IO](
        createChannelFor,
        options = CallOptions.DEFAULT.withCompression("gzip")
      )

    val muCompressedAvroRPCServiceClient: Resource[IO, CompressedAvroRPCService[IO]] =
      CompressedAvroRPCService.client[IO](
        createChannelFor,
        options = CallOptions.DEFAULT.withCompression("gzip")
      )

    val muCompressedAvroWithSchemaRPCServiceClient: Resource[IO, CompressedAvroWithSchemaRPCService[
      IO
    ]] =
      CompressedAvroWithSchemaRPCService.client[IO](
        createChannelFor,
        options = CallOptions.DEFAULT.withCompression("gzip")
      )

  }

  object implicits extends MuRuntime

}
