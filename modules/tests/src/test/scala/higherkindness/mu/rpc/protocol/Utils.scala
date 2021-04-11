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
import cats.effect.{Async, Resource}
import cats.effect.std.Dispatcher
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import io.grpc.Status
// import monix.reactive.Observable
import higherkindness.mu.rpc.common._
// import monix.execution.Scheduler

object Utils extends CommonUtils {

  object service {
    @service(Avro, Gzip) trait CompressedAvroRPCService[F[_]] {
      def unaryCompressed(a: A): F[C]
      def unaryCompressedWithError(e: E): F[C]
      def emptyAvroCompressed(empty: Empty.type): F[Empty.type]
      def emptyAvroParamCompressed(a: A): F[Empty.type]
      def emptyAvroParamResponseCompressed(empty: Empty.type): F[A]
      // def biStreamingCompressed(oe: Observable[E]): F[Observable[E]]
    }

    @service(Protobuf, Gzip) trait CompressedProtoRPCService[F[_]] {
      import ExternalScope._

      def emptyCompressed(empty: Empty.type): F[Empty.type]
      def emptyParamCompressed(a: A): F[Empty.type]
      def emptyParamResponseCompressed(empty: Empty.type): F[A]
      // def serverStreamingCompressed(b: B): F[Observable[C]]
      // def serverStreamingCompressedWithError(e: E): F[Observable[C]]
      // def clientStreamingCompressed(oa: Observable[A]): F[D]
      def scopeCompressed(empty: Empty.type): F[External]
    }

    @service(AvroWithSchema, Gzip) trait CompressedAvroWithSchemaRPCService[F[_]] {
      def unaryCompressedWithSchema(a: A): F[C]
      def emptyAvroWithSchemaCompressed(empty: Empty.type): F[Empty.type]
      def emptyAvroWithSchemaParamCompressed(a: A): F[Empty.type]
      def emptyAvroWithSchemaParamResponseCompressed(empty: Empty.type): F[A]
      // def biStreamingCompressedWithSchema(oe: Observable[E]): F[Observable[E]]
    }

    @service(Avro) trait AvroRPCService[F[_]] {
      def unary(a: A): F[C]
      def unaryWithError(e: E): F[C]
      def emptyAvro(empty: Empty.type): F[Empty.type]
      def emptyAvroParam(a: A): F[Empty.type]
      def emptyAvroParamResponse(empty: Empty.type): F[A]
      // def biStreaming(oe: Observable[E]): F[Observable[E]]
    }

    @service(Protobuf) trait ProtoRPCService[F[_]] {
      import ExternalScope._

      def empty(empty: Empty.type): F[Empty.type]
      def emptyParam(a: A): F[Empty.type]
      def emptyParamResponse(empty: Empty.type): F[A]
      // def serverStreaming(b: B): F[Observable[C]]
      // def serverStreamingWithError(e: E): F[Observable[C]]
      // def clientStreaming(oa: Observable[A]): F[D]
      def scope(empty: Empty.type): F[External]
    }

    @service(AvroWithSchema) trait AvroWithSchemaRPCService[F[_]] {
      def unaryWithSchema(a: A): F[C]
      def emptyAvroWithSchema(empty: Empty.type): F[Empty.type]
      def emptyAvroWithSchemaParam(a: A): F[Empty.type]
      def emptyAvroWithSchemaParamResponse(empty: Empty.type): F[A]
      // def biStreamingWithSchema(oe: Observable[E]): F[Observable[E]]
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
      def uws(x: Int, y: Int): F[C]
      def uwe(a: A, err: String): F[C]
      // def ss(a: Int, b: Int): F[List[C]]
      // def ss192(a: Int, b: Int): F[List[C]]
      // def sswe(a: A, err: String): F[List[C]]
      // def cs(cList: List[C], bar: Int): F[D]
      // def bs(eList: List[E]): F[E]
      // def bsws(eList: List[E]): F[E]
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

        // import monix.execution.Scheduler.Implicits.global

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

        // def serverStreaming(b: B): F[Observable[C]] = {
        //   debug(s"[SERVER] b -> $b")
        //   Observable.fromIterable(cList)
        // }.pure[F]

        // def serverStreamingWithError(e: E): F[Observable[C]] = {
        //   val observable: Observable[C] = e.foo match {
        //     case "SE" =>
        //       Observable.raiseError(Status.INVALID_ARGUMENT.withDescription(e.foo).asException)
        //     case "SRE" =>
        //       Observable.raiseError(
        //         Status.INVALID_ARGUMENT.withDescription(e.foo).asRuntimeException
        //       )
        //     case "RTE" =>
        //       Observable.raiseError(new IllegalArgumentException(e.foo))
        //     case _ =>
        //       sys.error(e.foo)
        //   }
        //   observable.pure[F]
        // }

        // def clientStreaming(oa: Observable[A]): F[D] =
        //   oa.foldLeftL(D(0)) { case (current, a) =>
        //     debug(s"[SERVER] Current -> $current / a -> $a")
        //     D(current.bar + a.x + a.y)
        //   }.toAsync[F]

        // def biStreaming(oe: Observable[E]): F[Observable[E]] =
        //   oe.flatMap { e: E =>
        //     save(e)

        //     Observable.fromIterable(eList)
        //   }.pure[F]

        // def biStreamingWithSchema(oe: Observable[E]): F[Observable[E]] = biStreaming(oe)

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

        // def serverStreamingCompressed(b: B): F[Observable[C]] = serverStreaming(b)

        // def serverStreamingCompressedWithError(e: E): F[Observable[C]] = serverStreamingWithError(e)

        // def clientStreamingCompressed(oa: Observable[A]): F[D] = clientStreaming(oa)

        // def biStreamingCompressed(oe: Observable[E]): F[Observable[E]] = biStreaming(oe)

        // def biStreamingCompressedWithSchema(oe: Observable[E]): F[Observable[E]] =
        //   biStreamingCompressed(oe)

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

        // import monix.execution.Scheduler.Implicits.global

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

        override def uws(x: Int, y: Int): F[C] =
          aws.use(_.unaryWithSchema(A(x, y)))

        override def uwe(a: A, err: String): F[C] =
          avro.use(_.unaryWithError(E(a, err)))

        // override def ss(a: Int, b: Int): F[List[C]] =
        //   proto
        //     .use(
        //       _.serverStreaming(B(A(a, a), A(b, b))).flatMap { obs =>
        //         obs.zipWithIndex
        //           .map { case (c, i) =>
        //             debug(s"[CLIENT] Result #$i: $c")
        //             c
        //           }
        //           .toListL
        //           .toAsync[F]
        //       }
        //     )

        // override def ss192(a: Int, b: Int): F[List[C]] =
        //   proto
        //     .use(
        //       _.serverStreaming(B(A(a, a), A(b, b))).flatMap { obs =>
        //         obs.toListL
        //           .toAsync[F]
        //       }
        //     )

        // override def sswe(a: A, err: String): F[List[C]] =
        //   proto
        //     .use(
        //       _.serverStreamingWithError(E(a, err)).flatMap { obs =>
        //         obs.zipWithIndex
        //           .map { case (c, i) =>
        //             debug(s"[CLIENT] Result #$i: $c")
        //             c
        //           }
        //           .toListL
        //           .toAsync[F]
        //       }
        //     )

        // override def cs(cList: List[C], bar: Int): F[D] =
        //   proto.use(_.clientStreaming(Observable.fromIterable(cList.map(c => c.a))))

        // override def bs(eList: List[E]): F[E] =
        //   avro
        //     .use(
        //       _.biStreaming(Observable.fromIterable(eList)).flatMap { obs =>
        //         obs.zipWithIndex
        //           .map { case (c, i) =>
        //             debug(s"[CLIENT] Result #$i: $c")
        //             c
        //           }
        //           .toListL
        //           .toAsync[F]
        //       }
        //     )
        //     .map(_.head)

        // override def bsws(eList: List[E]): F[E] =
        //   aws
        //     .use(
        //       _.biStreamingWithSchema(Observable.fromIterable(eList)).flatMap { obs =>
        //         obs.zipWithIndex
        //           .map { case (c, i) =>
        //             debug(s"[CLIENT] Result #$i: $c")
        //             c
        //           }
        //           .toListL
        //           .toAsync[F]
        //       }
        //     )
        //     .map(_.head)

      }

      class MuRPCServiceClientCompressedHandler[F[_]: Async](
          proto: Resource[F, CompressedProtoRPCService[F]],
          avro: Resource[F, CompressedAvroRPCService[F]],
          aws: Resource[F, CompressedAvroWithSchemaRPCService[F]]
      ) extends MyRPCClient[F] {

        // import monix.execution.Scheduler.Implicits.global

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

        override def uwe(a: A, err: String): F[C] =
          avro.use(_.unaryCompressedWithError(E(a, err)))

        override def uws(x: Int, y: Int): F[C] =
          aws.use(_.unaryCompressedWithSchema(A(x, y)))

        // override def ss(a: Int, b: Int): F[List[C]] =
        //   proto
        //     .use(
        //       _.serverStreamingCompressed(B(A(a, a), A(b, b))).flatMap { obs =>
        //         obs.zipWithIndex
        //           .map { case (c, i) =>
        //             debug(s"[CLIENT] Result #$i: $c")
        //             c
        //           }
        //           .toListL
        //           .toAsync[F]
        //       }
        //     )

        // override def ss192(a: Int, b: Int): F[List[C]] =
        //   proto
        //     .use(
        //       _.serverStreamingCompressed(B(A(a, a), A(b, b))).flatMap { obs =>
        //         obs.toListL
        //           .toAsync[F]
        //       }
        //     )

        // override def sswe(a: A, err: String): F[List[C]] =
        //   proto
        //     .use(
        //       _.serverStreamingCompressedWithError(E(a, err)).flatMap { obs =>
        //         obs.zipWithIndex
        //           .map { case (c, i) =>
        //             debug(s"[CLIENT] Result #$i: $c")
        //             c
        //           }
        //           .toListL
        //           .toAsync[F]
        //       }
        //     )

        // override def cs(cList: List[C], bar: Int): F[D] =
        //   proto.use(_.clientStreamingCompressed(Observable.fromIterable(cList.map(c => c.a))))

        // override def bs(eList: List[E]): F[E] =
        //   avro
        //     .use(
        //       _.biStreamingCompressed(Observable.fromIterable(eList)).flatMap { obs =>
        //         obs.zipWithIndex
        //           .map { case (c, i) =>
        //             debug(s"[CLIENT] Result #$i: $c")
        //             c
        //           }
        //           .toListL
        //           .toAsync[F]
        //       }
        //     )
        //     .map(_.head)

        // override def bsws(eList: List[E]): F[E] =
        //   aws
        //     .use(
        //       _.biStreamingCompressedWithSchema(Observable.fromIterable(eList)).flatMap { obs =>
        //         obs.zipWithIndex
        //           .map { case (c, i) =>
        //             debug(s"[CLIENT] Result #$i: $c")
        //             c
        //           }
        //           .toListL
        //           .toAsync[F]
        //       }
        //     )
        //     .map(_.head)

      }

    }

  }

  trait MuRuntime {

    import service._
    import handlers.server._
    import higherkindness.mu.rpc.server._

    //////////////////////////////////
    // Server Runtime Configuration //
    //////////////////////////////////

    // implicit val S: Scheduler = Scheduler.Implicits.global

    implicit val muRPCHandler: ServerRPCService[ConcurrentMonad] =
      new ServerRPCService[ConcurrentMonad]

    val (dispatcher, _) = Dispatcher[ConcurrentMonad].allocated.unsafeRunSync()

    val grpcConfigs: ConcurrentMonad[List[GrpcConfig]] = List(
      ProtoRPCService.bindService[ConcurrentMonad](dispatcher),
      AvroRPCService.bindService[ConcurrentMonad](dispatcher),
      AvroWithSchemaRPCService.bindService[ConcurrentMonad](dispatcher),
      CompressedProtoRPCService.bindService[ConcurrentMonad](dispatcher),
      CompressedAvroRPCService.bindService[ConcurrentMonad](dispatcher),
      CompressedAvroWithSchemaRPCService.bindService[ConcurrentMonad](dispatcher)
    ).sequence.map(_.map(AddService))

    implicit val grpcServer: GrpcServer[ConcurrentMonad] =
      grpcConfigs.flatMap(createServerConf[ConcurrentMonad]).unsafeRunSync()

    //////////////////////////////////
    // Client Runtime Configuration //
    //////////////////////////////////

    val protoRPCServiceClient: Resource[ConcurrentMonad, ProtoRPCService[ConcurrentMonad]] =
      ProtoRPCService.client[ConcurrentMonad](createChannelFor)
    val avroRPCServiceClient: Resource[ConcurrentMonad, AvroRPCService[ConcurrentMonad]] =
      AvroRPCService.client[ConcurrentMonad](createChannelFor)
    val awsRPCServiceClient: Resource[ConcurrentMonad, AvroWithSchemaRPCService[ConcurrentMonad]] =
      AvroWithSchemaRPCService.client[ConcurrentMonad](createChannelFor)

    val compressedprotoRPCServiceClient: Resource[ConcurrentMonad, CompressedProtoRPCService[
      ConcurrentMonad
    ]] =
      CompressedProtoRPCService.client[ConcurrentMonad](createChannelFor)
    val compressedavroRPCServiceClient: Resource[ConcurrentMonad, CompressedAvroRPCService[
      ConcurrentMonad
    ]] =
      CompressedAvroRPCService.client[ConcurrentMonad](createChannelFor)
    val compressedawsRPCServiceClient: Resource[ConcurrentMonad, CompressedAvroWithSchemaRPCService[
      ConcurrentMonad
    ]] =
      CompressedAvroWithSchemaRPCService.client[ConcurrentMonad](createChannelFor)

  }

  object implicits extends MuRuntime

}
