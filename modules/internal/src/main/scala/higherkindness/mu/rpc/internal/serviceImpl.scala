/*
 * Copyright 2017-2019 47 Degrees, LLC. <http://www.47deg.com>
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
package internal

import higherkindness.mu.rpc.protocol._

import scala.reflect.api.Trees
import scala.reflect.macros.blackbox

// $COVERAGE-OFF$
object serviceImpl {

  //todo: move the Context-dependent inner classes and functions elsewhere, if possible
  def service(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._
    import Flag._

    trait SupressWarts[T] {
      def supressWarts(warts: String*)(t: T): T
    }

    object SupressWarts {
      def apply[A](implicit A: SupressWarts[A]): SupressWarts[A] = A

      implicit val supressWartsOnModifier: SupressWarts[Modifiers] = new SupressWarts[Modifiers] {
        def supressWarts(warts: String*)(mod: Modifiers): Modifiers = {
          val argList = warts.map(ws => s"org.wartremover.warts.$ws")

          Modifiers(
            mod.flags,
            mod.privateWithin,
            q"new _root_.java.lang.SuppressWarnings(_root_.scala.Array(..$argList))" :: mod.annotations)
        }
      }

      implicit val supressWartsOnClassDef: SupressWarts[ClassDef] = new SupressWarts[ClassDef] {
        def supressWarts(warts: String*)(clazz: ClassDef): ClassDef = {
          ClassDef(
            SupressWarts[Modifiers].supressWarts(warts: _*)(clazz.mods),
            clazz.name,
            clazz.tparams,
            clazz.impl
          )
        }
      }

      implicit val supressWartsOnDefDef: SupressWarts[DefDef] = new SupressWarts[DefDef] {
        def supressWarts(warts: String*)(defdef: DefDef): DefDef = {
          DefDef(
            SupressWarts[Modifiers].supressWarts(warts: _*)(defdef.mods),
            defdef.name,
            defdef.tparams,
            defdef.vparamss,
            defdef.tpt,
            defdef.rhs
          )
        }
      }

      implicit val supressWartsOnValDef: SupressWarts[ValDef] = new SupressWarts[ValDef] {
        def supressWarts(warts: String*)(valdef: ValDef): ValDef = {
          ValDef(
            SupressWarts[Modifiers].supressWarts(warts: _*)(valdef.mods),
            valdef.name,
            valdef.tpt,
            valdef.rhs
          )
        }
      }

      class PimpedSupressWarts[A](value: A)(implicit A: SupressWarts[A]) {
        def supressWarts(warts: String*): A = A.supressWarts(warts: _*)(value)
      }

      implicit def pimpSupressWarts[A: SupressWarts](a: A) = new PimpedSupressWarts(a)
    }

    import SupressWarts._

    class RpcService(serviceDef: ClassDef) {
      val serviceName: TypeName = serviceDef.name

      require(
        serviceDef.tparams.length == 1,
        s"@service-annotated class $serviceName must have a single type parameter")

      val F_ : TypeDef = serviceDef.tparams.head
      val F: TypeName  = F_.name

      private val defs: List[Tree] = serviceDef.impl.body

      private val (rpcDefs, nonRpcDefs) = defs.collect {
        case d: DefDef => d
      } partition (_.rhs.isEmpty)

      private def compressionType(anns: List[Tree]): Tree =
        annotationParam(anns, 1, "compressionType", Some("Identity")) match {
          case "Identity" => q"None"
          case "Gzip"     => q"""Some("gzip")"""
        }

      private val rpcRequests: List[RpcRequest] = for {
        d      <- rpcDefs
        params <- d.vparamss
        _ = require(params.length == 1, s"RPC call ${d.name} has more than one request parameter")
        p <- params.headOption.toList
      } yield RpcRequest(d.name, p.tpt, d.tpt, compressionType(serviceDef.mods.annotations))

      val imports: List[Tree] = defs.collect {
        case imp: Import => imp
      }

      private val serializationType: SerializationType =
        c.prefix.tree match {
          case q"new service($serializationType)" =>
            serializationType.toString match {
              case "Protobuf"       => Protobuf
              case "Avro"           => Avro
              case "AvroWithSchema" => AvroWithSchema
              case _ =>
                sys.error(
                  "@service annotation should have a SerializationType parameter [Protobuf|Avro|AvroWithSchema]")
            }
          case q"new service($serializationType, $_)" =>
            serializationType.toString match {
              case "Protobuf"       => Protobuf
              case "Avro"           => Avro
              case "AvroWithSchema" => AvroWithSchema
              case _ =>
                sys.error(
                  "@service annotation should have a SerializationType parameter [Protobuf|Avro|AvroWithSchema], and a CompressionType parameter [Identity|Gzip]")
            }
          case _ =>
            sys.error(
              "@service annotation should have a SerializationType parameter [Protobuf|Avro|AvroWithSchema]")
        }

      val encodersImport = serializationType match {
        case Protobuf =>
          List(q"import _root_.higherkindness.mu.rpc.internal.encoders.pbd._")
        case Avro =>
          List(q"import _root_.higherkindness.mu.rpc.internal.encoders.avro._")
        case AvroWithSchema =>
          List(q"import _root_.higherkindness.mu.rpc.internal.encoders.avrowithschema._")
      }

      val methodDescriptors: List[Tree] = rpcRequests.map(_.methodDescriptor)
      private val serverCallDescriptorsAndHandlers: List[Tree] =
        rpcRequests.map(_.descriptorAndHandler)
      val bindService: DefDef = q"""
        def bindService[$F_](implicit
          F: _root_.cats.effect.ConcurrentEffect[$F],
          algebra: $serviceName[$F],
          EC: _root_.scala.concurrent.ExecutionContext
        ): $F[_root_.io.grpc.ServerServiceDefinition] =
          _root_.higherkindness.mu.rpc.internal.service.GRPCServiceDefBuilder.build[$F](${lit(
        serviceName)}, ..$serverCallDescriptorsAndHandlers)
        """

      private val clientCallMethods: List[Tree] = rpcRequests.map(_.clientDef)
      private val Client                        = TypeName("Client")
      val clientClass: ClassDef =
        q"""
        class $Client[$F_](
          channel: _root_.io.grpc.Channel,
          options: _root_.io.grpc.CallOptions = _root_.io.grpc.CallOptions.DEFAULT
        )(implicit
          F: _root_.cats.effect.ConcurrentEffect[$F],
          EC: _root_.scala.concurrent.ExecutionContext
        ) extends _root_.io.grpc.stub.AbstractStub[$Client[$F]](channel, options) with $serviceName[$F] {
          override def build(channel: _root_.io.grpc.Channel, options: _root_.io.grpc.CallOptions): $Client[$F] =
              new $Client[$F](channel, options)

          ..$clientCallMethods
          ..$nonRpcDefs
        }""".supressWarts("DefaultArguments")

      val client: DefDef =
        q"""
        def client[$F_](
          channelFor: _root_.higherkindness.mu.rpc.ChannelFor,
          channelConfigList: List[_root_.higherkindness.mu.rpc.channel.ManagedChannelConfig] = List(
            _root_.higherkindness.mu.rpc.channel.UsePlaintext()),
            options: _root_.io.grpc.CallOptions = _root_.io.grpc.CallOptions.DEFAULT
          )(implicit
          F: _root_.cats.effect.ConcurrentEffect[$F],
          EC: _root_.scala.concurrent.ExecutionContext
        ): _root_.cats.effect.Resource[F, $serviceName[$F]] =
          _root_.cats.effect.Resource.make {
            new _root_.higherkindness.mu.rpc.channel.ManagedChannelInterpreter[$F](channelFor, channelConfigList).build
          }(channel => F.void(F.delay(channel.shutdown()))).flatMap(ch =>
          _root_.cats.effect.Resource.make[F, $serviceName[$F]](F.delay(new $Client[$F](ch, options)))(_ => F.unit))
        """.supressWarts("DefaultArguments")

      val clientFromChannel: DefDef =
        q"""
        def clientFromChannel[$F_](
          channel: F[_root_.io.grpc.ManagedChannel],
          options: _root_.io.grpc.CallOptions = _root_.io.grpc.CallOptions.DEFAULT
        )(implicit
          F: _root_.cats.effect.ConcurrentEffect[$F],
          EC: _root_.scala.concurrent.ExecutionContext
        ): _root_.cats.effect.Resource[F, $serviceName[$F]] = _root_.cats.effect.Resource.make(channel)(channel =>
        F.void(F.delay(channel.shutdown()))).flatMap(ch =>
        _root_.cats.effect.Resource.make[F, $serviceName[$F]](F.delay(new $Client[$F](ch, options)))(_ => F.unit))
        """.supressWarts("DefaultArguments")

      val unsafeClient: DefDef =
        q"""
        def unsafeClient[$F_](
          channelFor: _root_.higherkindness.mu.rpc.ChannelFor,
          channelConfigList: List[_root_.higherkindness.mu.rpc.channel.ManagedChannelConfig] = List(
            _root_.higherkindness.mu.rpc.channel.UsePlaintext()),
            options: _root_.io.grpc.CallOptions = _root_.io.grpc.CallOptions.DEFAULT
          )(implicit
          F: _root_.cats.effect.ConcurrentEffect[$F],
          EC: _root_.scala.concurrent.ExecutionContext
        ): $serviceName[$F] = {
          val managedChannelInterpreter =
            new _root_.higherkindness.mu.rpc.channel.ManagedChannelInterpreter[$F](channelFor, channelConfigList).unsafeBuild
          new $Client[$F](managedChannelInterpreter, options)
        }""".supressWarts("DefaultArguments")

      val unsafeClientFromChannel: DefDef =
        q"""
        def unsafeClientFromChannel[$F_](
          channel: _root_.io.grpc.Channel,
          options: _root_.io.grpc.CallOptions = _root_.io.grpc.CallOptions.DEFAULT
        )(implicit
          F: _root_.cats.effect.ConcurrentEffect[$F],
          EC: _root_.scala.concurrent.ExecutionContext
        ): $serviceName[$F] = new $Client[$F](channel, options)
        """.supressWarts("DefaultArguments")

      private def lit(x: Any): Literal = Literal(Constant(x.toString))

      private def annotationParam(
          params: List[Tree],
          pos: Int,
          name: String,
          default: Option[String] = None): String =
        params
          .collectFirst {
            case q"$pName = $pValue" if pName.toString == name => pValue.toString
          }
          .getOrElse(if (params.isDefinedAt(pos)) params(pos).toString
          else default.getOrElse(sys.error(s"Missing annotation parameter $name")))

      private def findAnnotation(mods: Modifiers, name: String): Option[Tree] =
        mods.annotations find {
          case Apply(Select(New(Ident(TypeName(`name`))), _), _)     => true
          case Apply(Select(New(Select(_, TypeName(`name`))), _), _) => true
          case _                                                     => false
        }

      //todo: validate that the request and responses are case classes, if possible
      case class RpcRequest(
          methodName: TermName,
          requestType: Tree,
          responseType: Tree,
          compressionOption: Tree
      ) {

        private val requestStreamingImpl: Option[StreamingImpl]  = streamingImplFor(requestType)
        private val responseStreamingImpl: Option[StreamingImpl] = streamingImplFor(responseType)
        private val streamingImpls: Set[StreamingImpl] =
          Set(requestStreamingImpl, responseStreamingImpl).flatten
        require(
          streamingImpls.size < 2,
          s"RPC service $serviceName has different streaming implementations for request and response")
        private val streamingImpl: Option[StreamingImpl] = streamingImpls.headOption

        private val streamingType: Option[StreamingType] =
          if (requestStreamingImpl.isDefined && responseStreamingImpl.isDefined)
            Some(BidirectionalStreaming)
          else if (requestStreamingImpl.isDefined) Some(RequestStreaming)
          else if (responseStreamingImpl.isDefined) Some(ResponseStreaming)
          else None

        private def streamingImplFor(t: Tree): Option[StreamingImpl] = t match {
          case tq"$tpt[..$tpts]" if tpt.toString.endsWith("Observable") => Some(MonixObservable)
          case tq"$tpt[..$tpts]" if tpt.toString.endsWith("Stream")     => Some(Fs2Stream)
          case _                                                        => None
        }

        private val clientCallsImpl = streamingImpl match {
          case Some(Fs2Stream)       => q"_root_.higherkindness.mu.rpc.internal.client.fs2Calls"
          case Some(MonixObservable) => q"_root_.higherkindness.mu.rpc.internal.client.monixCalls"
          case None                  => q"_root_.higherkindness.mu.rpc.internal.client.unaryCalls"
        }

        private val streamingMethodType = {
          val suffix = streamingType match {
            case Some(RequestStreaming)       => "CLIENT_STREAMING"
            case Some(ResponseStreaming)      => "SERVER_STREAMING"
            case Some(BidirectionalStreaming) => "BIDI_STREAMING"
            case None                         => "UNARY"
          }
          q"_root_.io.grpc.MethodDescriptor.MethodType.${TermName(suffix)}"
        }

        private val methodDescriptorName = TermName(methodName + "MethodDescriptor")

        private val reqType = requestType match {
          case tq"$s[..$tpts]" if requestStreamingImpl.isDefined => tpts.last
          case other                                             => other
        }
        private val respType = responseType match {
          case tq"$x[..$tpts]" => tpts.last
        }

        val methodDescriptor: DefDef = q"""
          def $methodDescriptorName(implicit
            ReqM: _root_.io.grpc.MethodDescriptor.Marshaller[$reqType],
            RespM: _root_.io.grpc.MethodDescriptor.Marshaller[$respType]
          ): _root_.io.grpc.MethodDescriptor[$reqType, $respType] = {
            _root_.io.grpc.MethodDescriptor
              .newBuilder(
                ReqM,
                RespM)
              .setType($streamingMethodType)
              .setFullMethodName(
                _root_.io.grpc.MethodDescriptor.generateFullMethodName(${lit(serviceName)}, ${lit(
          methodName)}))
              .build()
          }
        """.supressWarts("Null", "ExplicitImplicitTypes")

        private def clientCallMethodFor(clientMethodName: String) =
          q"$clientCallsImpl.${TermName(clientMethodName)}(input, $methodDescriptorName, channel, options)"

        val clientDef: Tree = streamingType match {
          case Some(RequestStreaming) =>
            q"""
            def $methodName(input: $requestType): $responseType = ${clientCallMethodFor(
              "clientStreaming")}"""
          case Some(ResponseStreaming) =>
            q"""
            def $methodName(input: $requestType): $responseType = ${clientCallMethodFor(
              "serverStreaming")}"""
          case Some(BidirectionalStreaming) =>
            q"""
            def $methodName(input: $requestType): $responseType = ${clientCallMethodFor(
              "bidiStreaming")}"""
          case None =>
            q"""
            def $methodName(input: $requestType): $responseType = ${clientCallMethodFor("unary")}"""
        }

        private def serverCallMethodFor(serverMethodName: String) =
          q"_root_.higherkindness.mu.rpc.internal.server.monixCalls.${TermName(serverMethodName)}(algebra.$methodName, $compressionOption)"

        val descriptorAndHandler: Tree = {
          val handler = (streamingType, streamingImpl) match {
            case (Some(RequestStreaming), Some(Fs2Stream)) =>
              q"_root_.higherkindness.mu.rpc.internal.server.fs2Calls.clientStreamingMethod(algebra.$methodName, $compressionOption)"
            case (Some(RequestStreaming), Some(MonixObservable)) =>
              q"_root_.io.grpc.stub.ServerCalls.asyncClientStreamingCall(${serverCallMethodFor("clientStreamingMethod")})"
            case (Some(ResponseStreaming), Some(Fs2Stream)) =>
              q"_root_.higherkindness.mu.rpc.internal.server.fs2Calls.serverStreamingMethod(algebra.$methodName, $compressionOption)"
            case (Some(ResponseStreaming), Some(MonixObservable)) =>
              q"_root_.io.grpc.stub.ServerCalls.asyncServerStreamingCall(${serverCallMethodFor("serverStreamingMethod")})"
            case (Some(BidirectionalStreaming), Some(Fs2Stream)) =>
              q"_root_.higherkindness.mu.rpc.internal.server.fs2Calls.bidiStreamingMethod(algebra.$methodName, $compressionOption)"
            case (Some(BidirectionalStreaming), Some(MonixObservable)) =>
              q"_root_.io.grpc.stub.ServerCalls.asyncBidiStreamingCall(${serverCallMethodFor("bidiStreamingMethod")})"
            case (None, None) =>
              q"_root_.io.grpc.stub.ServerCalls.asyncUnaryCall(_root_.higherkindness.mu.rpc.internal.server.unaryCalls.unaryMethod(algebra.$methodName, $compressionOption))"
            case _ =>
              sys.error(
                s"Unable to define a handler for the streaming type $streamingType and $streamingImpl for the method $methodName in the service $serviceName")
          }
          q"($methodDescriptorName, $handler)"
        }
      }

      //----------
      // HTTP/REST
      //----------
      //TODO: derive server as well
      //TODO: move HTTP-related code to its own module (on last attempt this did not work)

      abstract class TypeTypology(tpe: Tree, inner: Option[Tree], isStreaming: Boolean)
          extends Product
          with Serializable {
        def getTpe: Tree           = tpe
        def getInner: Option[Tree] = inner
        def streaming: Boolean     = isStreaming
        def safeInner: Tree        = inner.getOrElse(tpe)
      }
      object TypeTypology {
        def apply(t: Tree): TypeTypology = t match {
          case tq"Observable[..$tpts]"       => MonixObservableTpe(t, tpts.headOption)
          case tq"Stream[$carrier, ..$tpts]" => Fs2StreamTpe(t, tpts.headOption)
          case tq"Empty.type"                => EmptyTpe(t)
          case tq"$carrier[..$tpts]"         => UnaryTpe(t, tpts.headOption)
        }
      }
      case class EmptyTpe(tpe: Tree)                          extends TypeTypology(tpe, None, false)
      case class UnaryTpe(tpe: Tree, inner: Option[Tree])     extends TypeTypology(tpe, inner, false)
      case class Fs2StreamTpe(tpe: Tree, inner: Option[Tree]) extends TypeTypology(tpe, inner, true)
      case class MonixObservableTpe(tpe: Tree, inner: Option[Tree])
          extends TypeTypology(tpe, inner, true)

      case class Operation(name: TermName, request: TypeTypology, response: TypeTypology) {

        val isStreaming: Boolean = request.streaming || response.streaming

        val streamingType: Option[StreamingType] = (request.streaming, response.streaming) match {
          case (true, true)  => Some(BidirectionalStreaming)
          case (true, false) => Some(RequestStreaming)
          case (false, true) => Some(ResponseStreaming)
          case _             => None
        }

        val validStreamingComb: Boolean = (request, response) match {
          case (Fs2StreamTpe(_, _), MonixObservableTpe(_, _)) => false
          case (MonixObservableTpe(_, _), Fs2StreamTpe(_, _)) => false
          case _                                              => true
        }

        require(
          validStreamingComb,
          s"RPC service $name has different streaming implementations for request and response")

        val prevalentStreamingTarget: TypeTypology = streamingType match {
          case Some(ResponseStreaming) => response
          case _                       => request
        }

      }

      case class HttpOperation(operation: Operation) {

        import operation._

        val uri = name.toString

        val method: TermName = request match {
          case _: EmptyTpe => TermName("GET")
          case _           => TermName("POST")
        }

        val executionClient: Tree = response match {
          case r: MonixObservableTpe =>
            q"Observable.fromReactivePublisher(client.streaming(request)(_.body.chunks.parseJsonStream.map(_.as[${r.safeInner}]).rethrow).toUnicastPublisher)"
          case r: Fs2StreamTpe =>
            q"client.streaming(request)(_.body.chunks.parseJsonStream.map(_.as[${r.safeInner}]).rethrow)"
          case _ =>
            q"client.expect[${response.safeInner}](request)"
        }

        val requestTypology: Tree = request match {
          case _: UnaryTpe =>
            q"val request = Request[F](Method.$method, uri / ${uri.replace("\"", "")}).withEntity(req.asJson)"
          case _: Fs2StreamTpe =>
            q"val request = Request[F](Method.$method, uri / ${uri.replace("\"", "")}).withEntity(req.map(_.asJson))"
          case _ =>
            q"val request = Request[F](Method.$method, uri / ${uri.replace("\"", "")})"
        }

        val responseEncoder =
          q"""implicit val responseDecoder: EntityDecoder[F, ${response.safeInner}] = jsonOf[F, ${response.safeInner}]"""

        def toTree: Tree = request match {
          case _: EmptyTpe =>
            q"""
               def $name(implicit client: _root_.org.http4s.client.Client[F]): ${response.getTpe} = {
                $responseEncoder
                $requestTypology
                $executionClient
               }"""
          case _ =>
            q"""def $name(req: ${request.getTpe})(implicit client: _root_.org.http4s.client.Client[F]): ${response.getTpe} = {
                $responseEncoder
                $requestTypology
                $executionClient
               }"""
        }

      }


      val httpRequests: List[Tree] = (for {
        d      <- rpcDefs.collect { case x if findAnnotation(x.mods, "http").isDefined => x }
        args   <- findAnnotation(d.mods, "http").collect({ case Apply(_, args) => args }).toList
        params <- d.vparamss
        _ = require(params.length == 1, s"RPC call ${d.name} has more than one request parameter")
        p <- params.headOption.toList
      } yield
        HttpOperation(Operation(d.name, TypeTypology(p.tpt), TypeTypology(d.tpt)))).map(_.toTree)

      val httpRequests2 = (for {
        d      <- rpcDefs.collect { case x if findAnnotation(x.mods, "http").isDefined => x }
        args   <- findAnnotation(d.mods, "http").collect({ case Apply(_, args) => args }).toList
        params <- d.vparamss
        _ = require(params.length == 1, s"RPC call ${d.name} has more than one request parameter")
        p <- params.headOption.toList
      } yield {

        val method: c.universe.TermName = if (isEmpty(p.tpt)) TermName("GET") else TermName("POST")
        val uri                         = d.name.toString

//        val method: c.universe.TermName = TermName(args(0).toString) // TODO: fix direct index access
//        val uri                         = args(1).toString // TODO: fix direct index access

        val responseType: Tree = d.tpt match {
          case tq"Observable[..$tpts]"       => tpts.head
          case tq"Stream[$carrier, ..$tpts]" => tpts.head
          case tq"$carrier[..$tpts]"         => tpts.head
          case _                             => ???
        }

        (method, uri, d.name, p.tpt, responseType, d.tpt)
      }).map(toHttpRequest)

      val HttpClient                = TypeName("HttpClient")
      val httpClientClass: ClassDef = q"""
        class $HttpClient[$F_](uri: Uri)(implicit F: _root_.cats.effect.Effect[$F], ec: scala.concurrent.ExecutionContext) {
          ..$httpRequests
      }"""

      val httpClient: DefDef = q"""
        def httpClient[$F_](uri: Uri)
          (implicit F: _root_.cats.effect.Effect[$F], ec: scala.concurrent.ExecutionContext): $HttpClient[$F] = {
          new $HttpClient[$F](uri)
      }"""

      val http =
        if (httpRequests.isEmpty) Nil
        else
          List(
            q"import _root_.higherkindness.mu.rpc.http.Utils._",
            q"import _root_.org.http4s._",
            q"import _root_.org.http4s.circe._",
            q"import _root_.io.circe._",
            q"import _root_.io.circe.generic.auto._",
            q"import _root_.io.circe.syntax._",
            httpClientClass,
            httpClient
          )
    }

    val classAndMaybeCompanion = annottees.map(_.tree)
    val result: List[Tree] = classAndMaybeCompanion.head match {
      case serviceDef: ClassDef
          if serviceDef.mods.hasFlag(TRAIT) || serviceDef.mods.hasFlag(ABSTRACT) => {
        val service = new RpcService(serviceDef)
        val companion: ModuleDef = classAndMaybeCompanion.lastOption match {
          case Some(obj: ModuleDef) => obj
          case _ =>
            ModuleDef(
              NoMods,
              serviceDef.name.toTermName,
              Template(
                List(TypeTree(typeOf[AnyRef])),
                noSelfType,
                List(
                  DefDef(
                    Modifiers(),
                    termNames.CONSTRUCTOR,
                    List(),
                    List(List()),
                    TypeTree(),
                    Block(List(pendingSuperCall), Literal(Constant(())))))
              )
            )
        }

        val enrichedCompanion = ModuleDef(
          companion.mods.supressWarts("Any", "NonUnitStatements", "StringPlusAny", "Throw"),
          companion.name,
          Template(
            companion.impl.parents,
            companion.impl.self,
            companion.impl.body ++ service.imports ++ service.methodDescriptors ++ service.encodersImport ++ List(
              service.bindService,
              service.clientClass,
              service.client,
              service.clientFromChannel,
              service.unsafeClient,
              service.unsafeClientFromChannel
            ) ++ service.http
          )
        )
        if (service.httpRequests.nonEmpty) {
          println("#######################")
          println(enrichedCompanion.toString)
        }
        List(serviceDef, enrichedCompanion)
      }
      case _ => sys.error("@service-annotated definition must be a trait or abstract class")
    }
    c.Expr(Block(result, Literal(Constant(()))))
  }
}

sealed trait StreamingImpl  extends Product with Serializable
case object Fs2Stream       extends StreamingImpl
case object MonixObservable extends StreamingImpl

// $COVERAGE-ON$
