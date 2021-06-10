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
package protocol

import cats.MonadError
import cats.effect.{Async, IO, Resource}
import cats.syntax.applicative._
import io.grpc.Status
import higherkindness.mu.rpc.common._

object Utils extends CommonUtils {

  object service {
    @service(Avro, Gzip) trait CompressedAvroRPCService[F[_]] {
      def unaryCompressed(a: A): F[C]
      def unaryCompressedWithError(e: E): F[C]
      def emptyAvroCompressed(empty: Empty.type): F[Empty.type]
      def emptyAvroParamCompressed(a: A): F[Empty.type]
      def emptyAvroParamResponseCompressed(empty: Empty.type): F[A]
    }

    @service(Protobuf, Gzip) trait CompressedProtoRPCService[F[_]] {
      import ExternalScope._

      def emptyCompressed(empty: Empty.type): F[Empty.type]
      def emptyParamCompressed(a: A): F[Empty.type]
      def emptyParamResponseCompressed(empty: Empty.type): F[A]
      def scopeCompressed(empty: Empty.type): F[External]
    }

    @service(AvroWithSchema, Gzip) trait CompressedAvroWithSchemaRPCService[F[_]] {
      def unaryCompressedWithSchema(a: A): F[C]
      def emptyAvroWithSchemaCompressed(empty: Empty.type): F[Empty.type]
      def emptyAvroWithSchemaParamCompressed(a: A): F[Empty.type]
      def emptyAvroWithSchemaParamResponseCompressed(empty: Empty.type): F[A]
    }

    @service(Avro) trait AvroRPCService[F[_]] {
      def unary(a: A): F[C]
      def unaryWithError(e: E): F[C]
      def emptyAvro(empty: Empty.type): F[Empty.type]
      def emptyAvroParam(a: A): F[Empty.type]
      def emptyAvroParamResponse(empty: Empty.type): F[A]
    }

    @service(Protobuf) trait ProtoRPCService[F[_]] {
      import ExternalScope._

      def empty(empty: Empty.type): F[Empty.type]
      def emptyParam(a: A): F[Empty.type]
      def emptyParamResponse(empty: Empty.type): F[A]
      def scope(empty: Empty.type): F[External]
    }

    @service(AvroWithSchema) trait AvroWithSchemaRPCService[F[_]] {
      def unaryWithSchema(a: A): F[C]
      def emptyAvroWithSchema(empty: Empty.type): F[Empty.type]
      def emptyAvroWithSchemaParam(a: A): F[Empty.type]
      def emptyAvroWithSchemaParamResponse(empty: Empty.type): F[A]
    }
  }

  object client {

    trait MyRPCClient[F[_]] {
      def empty: F[Empty.type]
      def emptyParam(a: A): F[Empty.type]
      def emptyParamResponse: F[A]
      def emptyAvro: F[Empty.type]
      def emptyAvroWithSchema: F[Empty.type]
      def emptyAvroParam(a: A): F[Empty.type]
      def emptyAvroWithSchemaParam(a: A): F[Empty.type]
      def emptyAvroParamResponse: F[A]
      def emptyAvroWithSchemaParamResponse: F[A]
      def u(x: Int, y: Int): F[C]

    }

  }

  object handlers {

    object server {

      import database._
      import service._
      import higherkindness.mu.rpc.protocol._

      class ServerRPCService[F[_]: Async](implicit M: MonadError[F, Throwable])
          extends ProtoRPCService[F]
          with AvroRPCService[F]
          with AvroWithSchemaRPCService[F]
          with CompressedProtoRPCService[F]
          with CompressedAvroRPCService[F]
          with CompressedAvroWithSchemaRPCService[F] {

        def empty(empty: Empty.type): F[Empty.type] = Empty.pure[F]

        def emptyParam(a: A): F[Empty.type] = Empty.pure[F]

        def emptyParamResponse(empty: Empty.type): F[A] = a4.pure[F]

        def emptyAvro(empty: Empty.type): F[Empty.type] = Empty.pure[F]

        def emptyAvroWithSchema(empty: Empty.type): F[Empty.type] = emptyAvro(empty)

        def emptyAvroParam(a: A): F[Empty.type] = Empty.pure[F]

        def emptyAvroWithSchemaParam(a: A): F[Empty.type] = emptyAvroParam(a)

        def emptyAvroParamResponse(empty: Empty.type): F[A] = a4.pure[F]

        def emptyAvroWithSchemaParamResponse(empty: Empty.type): F[A] =
          emptyAvroParamResponse(empty)

        def unary(a: A): F[C] = c1.pure[F]

        def unaryWithError(e: E): F[C] =
          e.foo match {
            case "SE" =>
              M.raiseError(Status.INVALID_ARGUMENT.withDescription(e.foo).asException)
            case "SRE" =>
              M.raiseError(Status.INVALID_ARGUMENT.withDescription(e.foo).asRuntimeException)
            case "RTE" =>
              M.raiseError(new IllegalArgumentException(e.foo))
            case _ =>
              sys.error(e.foo)
          }

        def unaryWithSchema(a: A): F[C] = unary(a)

        def save(e: E) = e // do something else with e?

        def emptyCompressed(empty: Empty.type): F[Empty.type] = Empty.pure[F]

        def emptyParamCompressed(a: A): F[Empty.type] = emptyParam(a)

        def emptyParamResponseCompressed(empty: Empty.type): F[A] = emptyParamResponse(empty)

        def emptyAvroCompressed(empty: Empty.type): F[Empty.type] = emptyAvro(empty)

        def emptyAvroWithSchemaCompressed(empty: Empty.type): F[Empty.type] =
          emptyAvroCompressed(empty)

        def emptyAvroParamCompressed(a: A): F[Empty.type] = emptyAvroParam(a)

        def emptyAvroWithSchemaParamCompressed(a: A): F[Empty.type] = emptyAvroParamCompressed(a)

        def emptyAvroParamResponseCompressed(empty: Empty.type): F[A] =
          emptyAvroParamResponse(empty)

        def emptyAvroWithSchemaParamResponseCompressed(empty: Empty.type): F[A] =
          emptyAvroParamResponseCompressed(empty)

        def unaryCompressed(a: A): F[C] = unary(a)

        def unaryCompressedWithSchema(a: A): F[C] = unaryCompressed(a)

        def unaryCompressedWithError(e: E): F[C] = unaryWithError(e)

        import ExternalScope._

        def scope(empty: protocol.Empty.type): F[External] = External(e1).pure[F]

        def scopeCompressed(empty: protocol.Empty.type): F[External] = External(e1).pure[F]
      }

    }

    object client {

      import service._
      import higherkindness.mu.rpc.protocol.Utils.client.MyRPCClient
      import higherkindness.mu.rpc.protocol._

      class MuRPCServiceClientHandler[F[_]: Async](
          proto: Resource[F, ProtoRPCService[F]],
          avro: Resource[F, AvroRPCService[F]],
          aws: Resource[F, AvroWithSchemaRPCService[F]]
      ) extends MyRPCClient[F] {

        override def empty: F[Empty.type] =
          proto.use(_.empty(protocol.Empty))

        override def emptyParam(a: A): F[Empty.type] =
          proto.use(_.emptyParam(a))

        override def emptyParamResponse: F[A] =
          proto.use(_.emptyParamResponse(protocol.Empty))

        override def emptyAvro: F[Empty.type] =
          avro.use(_.emptyAvro(protocol.Empty))

        override def emptyAvroWithSchema: F[Empty.type] =
          aws.use(_.emptyAvroWithSchema(protocol.Empty))

        override def emptyAvroParam(a: A): F[Empty.type] =
          avro.use(_.emptyAvroParam(a))

        override def emptyAvroWithSchemaParam(a: A): F[Empty.type] =
          aws.use(_.emptyAvroWithSchemaParam(a))

        override def emptyAvroParamResponse: F[A] =
          avro.use(_.emptyAvroParamResponse(protocol.Empty))

        override def emptyAvroWithSchemaParamResponse: F[A] =
          aws.use(_.emptyAvroWithSchemaParamResponse(protocol.Empty))

        override def u(x: Int, y: Int): F[C] =
          avro.use(_.unary(A(x, y)))

      }

      class MuRPCServiceClientCompressedHandler[F[_]: Async](
          proto: Resource[F, CompressedProtoRPCService[F]],
          avro: Resource[F, CompressedAvroRPCService[F]],
          aws: Resource[F, CompressedAvroWithSchemaRPCService[F]]
      ) extends MyRPCClient[F] {

        override def empty: F[Empty.type] =
          proto.use(_.emptyCompressed(protocol.Empty))

        override def emptyParam(a: A): F[Empty.type] =
          proto.use(_.emptyParamCompressed(a))

        override def emptyParamResponse: F[A] =
          proto.use(_.emptyParamResponseCompressed(protocol.Empty))

        override def emptyAvro: F[Empty.type] =
          avro.use(_.emptyAvroCompressed(protocol.Empty))

        override def emptyAvroWithSchema: F[Empty.type] =
          aws.use(_.emptyAvroWithSchemaCompressed(protocol.Empty))

        override def emptyAvroParam(a: A): F[Empty.type] =
          avro.use(_.emptyAvroParamCompressed(a))

        override def emptyAvroWithSchemaParam(a: A): F[Empty.type] =
          aws.use(_.emptyAvroWithSchemaParamCompressed(a))

        override def emptyAvroParamResponse: F[A] =
          avro.use(_.emptyAvroParamResponseCompressed(protocol.Empty))

        override def emptyAvroWithSchemaParamResponse: F[A] =
          aws.use(_.emptyAvroWithSchemaParamResponseCompressed(protocol.Empty))

        override def u(x: Int, y: Int): F[C] =
          avro.use(_.unaryCompressed(A(x, y)))

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

    val grpcConfigs: Resource[IO, List[GrpcConfig]] = List(
      ProtoRPCService.bindService[IO],
      AvroRPCService.bindService[IO],
      AvroWithSchemaRPCService.bindService[IO],
      CompressedProtoRPCService.bindService[IO],
      CompressedAvroRPCService.bindService[IO],
      CompressedAvroWithSchemaRPCService.bindService[IO]
    ).sequence.map(_.map(AddService))

    val grpcServer: Resource[IO, GrpcServer[IO]] =
      grpcConfigs.flatMap(createServerConf[IO])

    //////////////////////////////////
    // Client Runtime Configuration //
    //////////////////////////////////

    val protoRPCServiceClient: Resource[IO, ProtoRPCService[IO]] =
      ProtoRPCService.client[IO](createChannelFor)
    val avroRPCServiceClient: Resource[IO, AvroRPCService[IO]] =
      AvroRPCService.client[IO](createChannelFor)
    val awsRPCServiceClient: Resource[IO, AvroWithSchemaRPCService[IO]] =
      AvroWithSchemaRPCService.client[IO](createChannelFor)

    val compressedprotoRPCServiceClient: Resource[IO, CompressedProtoRPCService[
      IO
    ]] =
      CompressedProtoRPCService.client[IO](createChannelFor)
    val compressedavroRPCServiceClient: Resource[IO, CompressedAvroRPCService[
      IO
    ]] =
      CompressedAvroRPCService.client[IO](createChannelFor)
    val compressedawsRPCServiceClient: Resource[IO, CompressedAvroWithSchemaRPCService[
      IO
    ]] =
      CompressedAvroWithSchemaRPCService.client[IO](createChannelFor)

  }

  object implicits extends MuRuntime

}
