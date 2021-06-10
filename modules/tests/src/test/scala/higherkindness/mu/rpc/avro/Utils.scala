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
package avro

import cats.effect.{IO, Resource, Sync}
import higherkindness.mu.rpc.protocol._
import shapeless.{:+:, CNil, Coproduct}

object Utils extends CommonUtils {

  case class Request(a: String = "foo", b: Int = 123)
  case class RequestAddedBoolean(a: String, b: Int, c: Boolean = true)
  case class RequestAddedOptionalBoolean(a: String, b: Int, c: Option[Boolean] = None)
  case class RequestDroppedField(a: String)
  case class RequestReplacedType(a: String, c: Boolean = true)
  case class RequestRenamedField(a: String, c: Int = 0)
  case class RequestCoproduct[A](a: Int :+: String :+: A :+: CNil)
  case class RequestSuperCoproduct[A](a: Int :+: String :+: Boolean :+: A :+: CNil)
  case class RequestCoproductNoInt[A](
      a: String :+: A :+: CNil = Coproduct[String :+: A :+: CNil]("")
  )
  case class RequestCoproductReplaced[A](a: Int :+: Boolean :+: A :+: CNil)

  case class Response(a: String, b: Int = 123)
  case class ResponseAddedBoolean(a: String, b: Int, c: Boolean)
  case class ResponseAddedOptionalBoolean(a: String, b: Int, c: Option[Boolean])
  case class ResponseReplacedType(a: String, b: Int = 123, c: Boolean)
  case class ResponseRenamedField(a: String, b: Int = 123, c: Int)
  case class ResponseDroppedField(a: String)
  case class ResponseCoproduct[A](a: Int :+: String :+: A :+: CNil)
  case class ResponseSuperCoproduct[A](
      a: Int :+: String :+: A :+: CNil = Coproduct[Int :+: String :+: A :+: CNil](0),
      b: Int :+: String :+: Boolean :+: A :+: CNil
  )
  case class ResponseCoproductNoInt[A](a: String :+: A :+: CNil)
  case class ResponseCoproductReplaced[A](a: Int :+: Boolean :+: A :+: CNil)

  val request                   = Request("foo", 123)
  def requestCoproduct[A](a: A) = RequestCoproduct(Coproduct[Int :+: String :+: A :+: CNil](a))
  val requestCoproductInt       = RequestCoproduct(Coproduct[Int :+: String :+: Request :+: CNil](1))
  val requestCoproductString = RequestCoproduct(
    Coproduct[Int :+: String :+: Request :+: CNil]("hi")
  )

  val response                        = Response("foo", 123)
  val responseAddedBoolean            = ResponseAddedBoolean(response.a, response.b, true)
  val responseReplacedType            = ResponseReplacedType(a = response.a, c = true)
  val responseRenamedField            = ResponseRenamedField(a = response.a, c = 456)
  val responseDroppedField            = ResponseDroppedField(response.a)
  def responseCoproduct[A](a: A)      = ResponseCoproduct(Coproduct[Int :+: String :+: A :+: CNil](a))
  def responseCoproductNoInt[A](a: A) = ResponseCoproductNoInt(Coproduct[String :+: A :+: CNil](a))
  def responseCoproductReplaced[A](a: A) =
    ResponseCoproductReplaced(Coproduct[Int :+: Boolean :+: A :+: CNil](a))

  //Original Service

  object service {
    @service(AvroWithSchema)
    trait RPCService[F[_]] {
      def get(a: Request): F[Response]

      def getCoproduct(a: RequestCoproduct[Request]): F[ResponseCoproduct[Response]]
    }
  }

  //Updated request services

  object serviceRequestAddedBoolean {
    @service(AvroWithSchema)
    trait RPCService[F[_]] {
      def get(a: RequestAddedBoolean): F[Response]

      def getCoproduct(a: RequestCoproduct[RequestAddedBoolean]): F[ResponseCoproduct[Response]]
    }
  }

  object serviceRequestAddedOptionalBoolean {
    @service(AvroWithSchema)
    trait RPCService[F[_]] {
      def get(a: RequestAddedOptionalBoolean): F[Response]

      def getCoproduct(
          a: RequestCoproduct[RequestAddedOptionalBoolean]
      ): F[ResponseCoproduct[Response]]
    }
  }

  object serviceRequestAddedCoproductItem {
    @service(AvroWithSchema)
    trait RPCService[F[_]] {
      def getCoproduct(a: RequestSuperCoproduct[Request]): F[ResponseCoproduct[Response]]
    }
  }

  object serviceRequestRemovedCoproductItem {
    @service(AvroWithSchema)
    trait RPCService[F[_]] {
      def getCoproduct(a: RequestCoproductNoInt[Request]): F[ResponseCoproduct[Response]]
    }
  }

  object serviceRequestReplacedCoproductItem {
    @service(AvroWithSchema)
    trait RPCService[F[_]] {
      def getCoproduct(a: RequestCoproductReplaced[Request]): F[ResponseCoproduct[Response]]
    }
  }

  object serviceRequestDroppedField {
    @service(AvroWithSchema)
    trait RPCService[F[_]] {
      def get(a: RequestDroppedField): F[Response]

      def getCoproduct(a: RequestCoproduct[RequestDroppedField]): F[ResponseCoproduct[Response]]
    }
  }

  object serviceRequestReplacedType {
    @service(AvroWithSchema)
    trait RPCService[F[_]] {
      def get(a: RequestReplacedType): F[Response]

      def getCoproduct(a: RequestCoproduct[RequestReplacedType]): F[ResponseCoproduct[Response]]
    }
  }

  object serviceRequestRenamedField {
    @service(AvroWithSchema)
    trait RPCService[F[_]] {
      def get(a: RequestRenamedField): F[Response]

      def getCoproduct(a: RequestCoproduct[RequestRenamedField]): F[ResponseCoproduct[Response]]
    }
  }

  //Updated response services

  object serviceResponseAddedBoolean {
    @service(AvroWithSchema)
    trait RPCService[F[_]] {
      def get(a: Request): F[ResponseAddedBoolean]

      def getCoproduct(a: RequestCoproduct[Request]): F[ResponseCoproduct[ResponseAddedBoolean]]
    }
  }

  object serviceResponseAddedBooleanCoproduct {
    @service(AvroWithSchema)
    trait RPCService[F[_]] {
      def getCoproduct(a: RequestCoproduct[Request]): F[ResponseSuperCoproduct[Response]]
    }
  }

  object serviceResponseRemovedIntCoproduct {
    @service(AvroWithSchema)
    trait RPCService[F[_]] {
      def getCoproduct(a: RequestCoproduct[Request]): F[ResponseCoproductNoInt[Response]]
    }
  }

  object serviceResponseReplacedCoproduct {
    @service(AvroWithSchema)
    trait RPCService[F[_]] {
      def getCoproduct(a: RequestCoproduct[Request]): F[ResponseCoproductReplaced[Response]]
    }
  }

  object serviceResponseReplacedType {
    @service(AvroWithSchema)
    trait RPCService[F[_]] {
      def get(a: Request): F[ResponseReplacedType]

      def getCoproduct(a: RequestCoproduct[Request]): F[ResponseCoproduct[ResponseReplacedType]]
    }
  }

  object serviceResponseRenamedField {
    @service(AvroWithSchema)
    trait RPCService[F[_]] {
      def get(a: Request): F[ResponseRenamedField]

      def getCoproduct(a: RequestCoproduct[Request]): F[ResponseCoproduct[ResponseRenamedField]]
    }
  }

  object serviceResponseDroppedField {
    @service(AvroWithSchema)
    trait RPCService[F[_]] {
      def get(a: Request): F[ResponseDroppedField]

      def getCoproduct(a: RequestCoproduct[Request]): F[ResponseCoproduct[ResponseDroppedField]]
    }
  }

  object handlers {

    class RPCServiceHandler[F[_]: Sync] extends service.RPCService[F] {
      def get(a: Request): F[Response] = Sync[F].delay(response)
      def getCoproduct(a: RequestCoproduct[Request]): F[ResponseCoproduct[Response]] =
        Sync[F].delay(
          ResponseCoproduct(Coproduct[Int :+: String :+: Response :+: CNil](response))
        )
    }

    class RequestAddedBooleanRPCServiceHandler[F[_]: Sync]
        extends serviceRequestAddedBoolean.RPCService[F] {
      def get(a: RequestAddedBoolean): F[Response] = Sync[F].delay(response)
      def getCoproduct(a: RequestCoproduct[RequestAddedBoolean]): F[ResponseCoproduct[Response]] =
        Sync[F].delay(responseCoproduct(response))
    }

    class RequestAddedOptionalBooleanRPCServiceHandler[F[_]: Sync]
        extends serviceRequestAddedOptionalBoolean.RPCService[F] {
      def get(a: RequestAddedOptionalBoolean): F[Response] = Sync[F].delay(response)
      def getCoproduct(
          a: RequestCoproduct[RequestAddedOptionalBoolean]
      ): F[ResponseCoproduct[Response]] =
        Sync[F].delay(responseCoproduct(response))
    }

    class RequestAddedCoproductItemRPCServiceHandler[F[_]: Sync]
        extends serviceRequestAddedCoproductItem.RPCService[F] {
      def getCoproduct(a: RequestSuperCoproduct[Request]): F[ResponseCoproduct[Response]] =
        Sync[F].delay(responseCoproduct(response))
    }

    class RequestRemovedCoproductItemRPCServiceHandler[F[_]: Sync]
        extends serviceRequestRemovedCoproductItem.RPCService[F] {
      def getCoproduct(a: RequestCoproductNoInt[Request]): F[ResponseCoproduct[Response]] =
        Sync[F].delay(responseCoproduct(response))
    }

    class RequestReplacedCoproductItemRPCServiceHandler[F[_]: Sync]
        extends serviceRequestReplacedCoproductItem.RPCService[F] {
      def getCoproduct(a: RequestCoproductReplaced[Request]): F[ResponseCoproduct[Response]] =
        Sync[F].delay(responseCoproduct(response))
    }

    class RequestDroppedFieldRPCServiceHandler[F[_]: Sync]
        extends serviceRequestDroppedField.RPCService[F] {
      def get(a: RequestDroppedField): F[Response] = Sync[F].delay(response)
      def getCoproduct(a: RequestCoproduct[RequestDroppedField]): F[ResponseCoproduct[Response]] =
        Sync[F].delay(responseCoproduct(response))
    }

    class RequestReplacedTypeRPCServiceHandler[F[_]: Sync]
        extends serviceRequestReplacedType.RPCService[F] {
      def get(a: RequestReplacedType): F[Response] = Sync[F].delay(response)
      def getCoproduct(a: RequestCoproduct[RequestReplacedType]): F[ResponseCoproduct[Response]] =
        Sync[F].delay(responseCoproduct(response))
    }

    class RequestRenamedFieldRPCServiceHandler[F[_]: Sync]
        extends serviceRequestRenamedField.RPCService[F] {
      def get(a: RequestRenamedField): F[Response] = Sync[F].delay(response)
      def getCoproduct(a: RequestCoproduct[RequestRenamedField]): F[ResponseCoproduct[Response]] =
        Sync[F].delay(responseCoproduct(response))
    }

    class ResponseAddedBooleanRPCServiceHandler[F[_]: Sync]
        extends serviceResponseAddedBoolean.RPCService[F] {
      def get(a: Request): F[ResponseAddedBoolean] = Sync[F].delay(responseAddedBoolean)
      def getCoproduct(a: RequestCoproduct[Request]): F[ResponseCoproduct[ResponseAddedBoolean]] =
        Sync[F].delay(responseCoproduct(responseAddedBoolean))
    }

    class ResponseAddedBooleanCoproductRPCServiceHandler[F[_]: Sync]
        extends serviceResponseAddedBooleanCoproduct.RPCService[F] {
      def getCoproduct(a: RequestCoproduct[Request]): F[ResponseSuperCoproduct[Response]] =
        Sync[F].delay(
          ResponseSuperCoproduct(
            b = Coproduct[Int :+: String :+: Boolean :+: Response :+: CNil](true)
          )
        )
    }

    class ResponseRemovedIntCoproductRPCServiceHandler[F[_]: Sync]
        extends serviceResponseRemovedIntCoproduct.RPCService[F] {
      def getCoproduct(a: RequestCoproduct[Request]): F[ResponseCoproductNoInt[Response]] =
        Sync[F].delay(responseCoproductNoInt(response))
    }

    class ResponseReplacedCoproductRPCServiceHandler[F[_]: Sync]
        extends serviceResponseReplacedCoproduct.RPCService[F] {
      def getCoproduct(a: RequestCoproduct[Request]): F[ResponseCoproductReplaced[Response]] =
        Sync[F].delay(responseCoproductReplaced(response))
    }

    class ResponseReplacedTypeRPCServiceHandler[F[_]: Sync]
        extends serviceResponseReplacedType.RPCService[F] {
      def get(a: Request): F[ResponseReplacedType] = Sync[F].delay(responseReplacedType)
      def getCoproduct(a: RequestCoproduct[Request]): F[ResponseCoproduct[ResponseReplacedType]] =
        Sync[F].delay(responseCoproduct(responseReplacedType))
    }

    class ResponseRenamedFieldRPCServiceHandler[F[_]: Sync]
        extends serviceResponseRenamedField.RPCService[F] {
      def get(a: Request): F[ResponseRenamedField] = Sync[F].delay(responseRenamedField)
      def getCoproduct(a: RequestCoproduct[Request]): F[ResponseCoproduct[ResponseRenamedField]] =
        Sync[F].delay(responseCoproduct(responseRenamedField))
    }

    class ResponseDroppedFieldRPCServiceHandler[F[_]: Sync]
        extends serviceResponseDroppedField.RPCService[F] {
      def get(a: Request): F[ResponseDroppedField] = Sync[F].delay(responseDroppedField)
      def getCoproduct(a: RequestCoproduct[Request]): F[ResponseCoproduct[ResponseDroppedField]] =
        Sync[F].delay(responseCoproduct(responseDroppedField))
    }

  }

  trait MuRuntime {

    import handlers._

    //////////////////////////////////
    // Server Runtime Configuration //
    //////////////////////////////////

    implicit val rpcServiceHandler: service.RPCService[IO] =
      new RPCServiceHandler[IO]

    implicit val requestAddedBooleanRPCServiceHandler: serviceRequestAddedBoolean.RPCService[
      IO
    ] =
      new RequestAddedBooleanRPCServiceHandler[IO]

    implicit val requestAddedOptionalBooleanRPCServiceHandler: serviceRequestAddedOptionalBoolean.RPCService[
      IO
    ] =
      new RequestAddedOptionalBooleanRPCServiceHandler[IO]

    implicit val requestAddedCoproductItemRPCServiceHandler: serviceRequestAddedCoproductItem.RPCService[
      IO
    ] =
      new RequestAddedCoproductItemRPCServiceHandler[IO]

    implicit val requestRemovedCoproductItemRPCServiceHandler: serviceRequestRemovedCoproductItem.RPCService[
      IO
    ] =
      new RequestRemovedCoproductItemRPCServiceHandler[IO]

    implicit val requestReplacedCoproductItemRPCServiceHandler: serviceRequestReplacedCoproductItem.RPCService[
      IO
    ] =
      new RequestReplacedCoproductItemRPCServiceHandler[IO]

    implicit val requestDroppedFieldRPCServiceHandler: serviceRequestDroppedField.RPCService[
      IO
    ] =
      new RequestDroppedFieldRPCServiceHandler[IO]

    implicit val requestReplacedTypeRPCServiceHandler: serviceRequestReplacedType.RPCService[
      IO
    ] =
      new RequestReplacedTypeRPCServiceHandler[IO]

    implicit val requestRenamedFieldRPCServiceHandler: serviceRequestRenamedField.RPCService[
      IO
    ] =
      new RequestRenamedFieldRPCServiceHandler[IO]

    implicit val responseAddedBooleanRPCServiceHandler: serviceResponseAddedBoolean.RPCService[
      IO
    ] =
      new ResponseAddedBooleanRPCServiceHandler[IO]

    implicit val responseAddedBooleanCoproductRPCServiceHandler: serviceResponseAddedBooleanCoproduct.RPCService[
      IO
    ] =
      new ResponseAddedBooleanCoproductRPCServiceHandler[IO]

    implicit val responseRemovedIntCoproductRPCServiceHandler: serviceResponseRemovedIntCoproduct.RPCService[
      IO
    ] =
      new ResponseRemovedIntCoproductRPCServiceHandler[IO]

    implicit val responseReplacedCoproductRPCServiceHandler: serviceResponseReplacedCoproduct.RPCService[
      IO
    ] =
      new ResponseReplacedCoproductRPCServiceHandler[IO]

    implicit val responseReplacedTypeRPCServiceHandler: serviceResponseReplacedType.RPCService[
      IO
    ] =
      new ResponseReplacedTypeRPCServiceHandler[IO]

    implicit val responseRenamedFieldRPCServiceHandler: serviceResponseRenamedField.RPCService[
      IO
    ] =
      new ResponseRenamedFieldRPCServiceHandler[IO]

    implicit val responseDroppedFieldRPCServiceHandler: serviceResponseDroppedField.RPCService[
      IO
    ] =
      new ResponseDroppedFieldRPCServiceHandler[IO]

    //////////////////////////////////
    // Client Runtime Configuration //
    //////////////////////////////////

    implicit val muRPCServiceClient: Resource[IO, service.RPCService[
      IO
    ]] =
      service.RPCService.client[IO](createChannelFor)

  }

  object implicits extends MuRuntime

}
