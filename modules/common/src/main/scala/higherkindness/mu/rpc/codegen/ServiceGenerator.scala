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

package higherkindness.mu.rpc.codegen

import higherkindness.mu.rpc.protocol._
import java.io._
import scala.tools.nsc._
import scala.tools.nsc.plugins._

// $COVERAGE-OFF$

object ServiceGenerator {
  val OutputDirOption = "outputDir"
}

class ServiceGenerator(val global: Global) extends Plugin {
  import ServiceGenerator._

  val name        = "service-generator"
  val description = "Generates client and server sources from @service annotations"

  override val optionsHelp: Option[String] = Some(
    s"$OutputDirOption: output directory for generated client and server sources")

  private val parsedOptions: Map[String, String] = options.map { option =>
    val parsedOption = option.split('=')
    require(parsedOption.length == 2, s"Invalid option syntax: $option")
    parsedOption.head -> parsedOption.last
  }.toMap

  private val outputDir = new File(option(OutputDirOption))

  lazy val components: List[PluginComponent] = List(
    new ServiceGeneratorComponent(global, name, outputDir))

  override def init(options: List[String], error: String => Unit): Boolean = true

  private def option(optionName: String): String =
    parsedOptions.getOrElse(
      optionName,
      throw new IllegalArgumentException(
        s"Plugin $name missing option $optionName.\n${optionsHelp.get}"))
}

class ServiceGeneratorComponent(val global: Global, val phaseName: String, outputDir: File)
    extends PluginComponent {
  import global._

  private lazy val serviceFilter = new FilterTreeTraverser({
    case md: MemberDef =>
      findAnnotation(md.mods, "serviceCodeGen").isDefined // TODO: rename to "service"
    case x => false
  })

  val runsAfter: List[String] = List("parser")

  def newPhase(previous: Phase) = new ServiceGeneratorPhase(phaseName, previous)

  class ServiceGeneratorPhase(override val name: String, previous: Phase)
      extends StdPhase(previous) {

    def apply(unit: CompilationUnit): Unit = {
      serviceFilter.traverse(unit.body)
      serviceFilter.hits.foreach {
        case serviceDef: ClassDef
            if serviceDef.mods.hasFlag(Flag.TRAIT) || serviceDef.mods.hasFlag(Flag.ABSTRACT) =>
          process(new RpcService(serviceDef), "example.codegen") // TODO: get the actual package of the service
        case _ => sys.error("Only traits and abstract classes can be annotated with @service")
      }
    }
  }

  abstract class TypeTypology(tpe: Tree, inner: Option[Tree]) extends Product with Serializable {
    def getTpe: Tree           = tpe
    def getInner: Option[Tree] = inner
    def safeInner: Tree        = inner.getOrElse(tpe)
    def safeType: Tree = tpe match {
      case tq"$s[..$tpts]" if isStreaming => tpts.last
      case other                          => other
    }
    def flatName: String = safeInner.toString

    def isEmpty: Boolean = this match {
      case _: EmptyTpe => true
      case _           => false
    }

    def isStreaming: Boolean = this match {
      case _: Fs2StreamTpe       => true
      case _: MonixObservableTpe => true
      case _                     => false
    }
  }
  object TypeTypology {
    def apply(t: Tree): TypeTypology = t match {
      case tq"Observable[..$tpts]"       => MonixObservableTpe(t, tpts.headOption)
      case tq"Stream[$carrier, ..$tpts]" => Fs2StreamTpe(t, tpts.headOption)
      case tq"Empty.type"                => EmptyTpe(t)
      case tq"$carrier[..$tpts]"         => UnaryTpe(t, tpts.headOption)
    }
  }
  case class EmptyTpe(tpe: Tree)                                extends TypeTypology(tpe, None)
  case class UnaryTpe(tpe: Tree, inner: Option[Tree])           extends TypeTypology(tpe, inner)
  case class Fs2StreamTpe(tpe: Tree, inner: Option[Tree])       extends TypeTypology(tpe, inner)
  case class MonixObservableTpe(tpe: Tree, inner: Option[Tree]) extends TypeTypology(tpe, inner)

  case class Operation(name: TermName, request: TypeTypology, response: TypeTypology) {

    val isStreaming: Boolean = request.isStreaming || response.isStreaming

    val streamingType: Option[StreamingType] = (request.isStreaming, response.isStreaming) match {
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

    val isMonixObservable: Boolean = List(request, response).collect {
      case m: MonixObservableTpe => m
    }.nonEmpty

    val prevalentStreamingTarget: TypeTypology =
      if (streamingType.contains(ResponseStreaming)) response else request

  }

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

    val annotationParams: List[Either[String, (String, String)]] = {
      findAnnotation(serviceDef.mods, "serviceCodeGen").get match { // TODO: rename to "service"
        case q"new serviceCodeGen(..$seq)" =>
          seq.toList.map {
            case q"$pName = $pValue" => Right((pName.toString(), pValue.toString()))
            case param               => Left(param.toString())
          }
        case x => Nil
      }
    }

    private val compressionType: Tree =
      annotationParam(1, "compressionType") {
        case "Identity" => q"None"
        case "Gzip"     => q"""Some("gzip")"""
      }.getOrElse(q"None")

    private val OptionString = "Some\\(\"(.+)\"\\)".r

    private val namespacePrefix: String =
      annotationParam(2, "namespace") {
        case OptionString(s) => s"$s."
        case "None"          => ""
      }.getOrElse("")

    private val fullyServiceName = namespacePrefix + serviceName.toString

    private val methodNameStyle: MethodNameStyle =
      annotationParam(3, "methodNameStyle") {
        case "Capitalize" => Capitalize
        case "Unchanged"  => Unchanged
      }.getOrElse(Unchanged)

    private val rpcRequests: List[RpcRequest] = for {
      d      <- rpcDefs
      params <- d.vparamss
      _ = require(params.length == 1, s"RPC call ${d.name} has more than one request parameter")
      p <- params.headOption.toList
    } yield
      RpcRequest(
        Operation(d.name, TypeTypology(p.tpt), TypeTypology(d.tpt)),
        compressionType,
        methodNameStyle
      )

    val imports: List[Tree] = defs.collect {
      case imp: Import => imp
    }

    private val serializationType: SerializationType =
      annotationParam(0, "serializationType") {
        case "Protobuf"       => Protobuf
        case "Avro"           => Avro
        case "AvroWithSchema" => AvroWithSchema
      }.getOrElse(sys.error(
        "@service annotation should have a SerializationType parameter [Protobuf|Avro|AvroWithSchema]"))

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

    val ceImplicit: Tree        = q"CE: _root_.cats.effect.ConcurrentEffect[$F]"
    val csImplicit: Tree        = q"CS: _root_.cats.effect.ContextShift[$F]"
    val schedulerImplicit: Tree = q"S: _root_.monix.execution.Scheduler"

    val bindImplicits: List[Tree] = ceImplicit :: q"algebra: $serviceName[$F]" :: rpcRequests
      .find(_.operation.isMonixObservable)
      .map(_ => schedulerImplicit)
      .toList

    val classImplicits: List[Tree] = ceImplicit :: csImplicit :: rpcRequests
      .find(_.operation.isMonixObservable)
      .map(_ => schedulerImplicit)
      .toList

    val bindService: DefDef = q"""
        def bindService[$F_](implicit ..$bindImplicits): $F[_root_.io.grpc.ServerServiceDefinition] =
          _root_.higherkindness.mu.rpc.internal.service.GRPCServiceDefBuilder.build[$F](${lit(
      fullyServiceName)}, ..$serverCallDescriptorsAndHandlers)
        """

    private val clientCallMethods: List[Tree] = rpcRequests.map(_.clientDef)
    private val Client                        = TypeName("Client")
    val clientClass: ClassDef =
      q"""
        class $Client[$F_](
          channel: _root_.io.grpc.Channel,
          options: _root_.io.grpc.CallOptions = _root_.io.grpc.CallOptions.DEFAULT
        )(implicit ..$classImplicits) extends _root_.io.grpc.stub.AbstractStub[$Client[$F]](channel, options) with $serviceName[$F] {
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
          )(implicit ..$classImplicits): _root_.cats.effect.Resource[F, $serviceName[$F]] =
          _root_.cats.effect.Resource.make {
            new _root_.higherkindness.mu.rpc.channel.ManagedChannelInterpreter[$F](channelFor, channelConfigList).build
          }(channel => CE.void(CE.delay(channel.shutdown()))).flatMap(ch =>
          _root_.cats.effect.Resource.make[F, $serviceName[$F]](CE.delay(new $Client[$F](ch, options)))(_ => CE.unit))
        """.supressWarts("DefaultArguments")

    val clientFromChannel: DefDef =
      q"""
        def clientFromChannel[$F_](
          channel: $F[_root_.io.grpc.ManagedChannel],
          options: _root_.io.grpc.CallOptions = _root_.io.grpc.CallOptions.DEFAULT
        )(implicit ..$classImplicits): _root_.cats.effect.Resource[$F, $serviceName[$F]] = _root_.cats.effect.Resource.make(channel)(channel =>
        CE.void(CE.delay(channel.shutdown()))).flatMap(ch =>
        _root_.cats.effect.Resource.make[$F, $serviceName[$F]](CE.delay(new $Client[$F](ch, options)))(_ => CE.unit))
        """.supressWarts("DefaultArguments")

    val unsafeClient: DefDef =
      q"""
        def unsafeClient[$F_](
          channelFor: _root_.higherkindness.mu.rpc.ChannelFor,
          channelConfigList: List[_root_.higherkindness.mu.rpc.channel.ManagedChannelConfig] = List(
            _root_.higherkindness.mu.rpc.channel.UsePlaintext()),
            options: _root_.io.grpc.CallOptions = _root_.io.grpc.CallOptions.DEFAULT
          )(implicit ..$classImplicits): $serviceName[$F] = {
          val managedChannelInterpreter =
            new _root_.higherkindness.mu.rpc.channel.ManagedChannelInterpreter[$F](channelFor, channelConfigList).unsafeBuild
          new $Client[$F](managedChannelInterpreter, options)
        }""".supressWarts("DefaultArguments")

    val unsafeClientFromChannel: DefDef =
      q"""
        def unsafeClientFromChannel[$F_](
          channel: _root_.io.grpc.Channel,
          options: _root_.io.grpc.CallOptions = _root_.io.grpc.CallOptions.DEFAULT
        )(implicit ..$classImplicits): $serviceName[$F] = new $Client[$F](channel, options)
        """.supressWarts("DefaultArguments")

    private def lit(x: Any): Literal = Literal(Constant(x.toString))

    private def annotationParam[A](pos: Int, name: String)(
        pf: PartialFunction[String, A]): Option[A] = {

      def findNamed: Option[Either[String, (String, String)]] =
        annotationParams.find(_.exists(_._1 == name))

      def findIndexed: Option[Either[String, (String, String)]] =
        annotationParams.lift(pos).filter(_.isLeft)

      (findNamed orElse findIndexed).map(_.fold(identity, _._2)).map { s =>
        pf.lift(s).getOrElse(sys.error(s"Invalid `$name` annotation value ($s)"))
      }
    }

    //todo: validate that the request and responses are case classes, if possible
    case class RpcRequest(
        operation: Operation,
        compressionOption: Tree,
        methodNameStyle: MethodNameStyle
    ) {

      import operation._

      private val clientCallsImpl = prevalentStreamingTarget match {
        case _: Fs2StreamTpe       => q"_root_.higherkindness.mu.rpc.internal.client.fs2Calls"
        case _: MonixObservableTpe => q"_root_.higherkindness.mu.rpc.internal.client.monixCalls"
        case _                     => q"_root_.higherkindness.mu.rpc.internal.client.unaryCalls"
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

      private val updatedName = methodNameStyle match {
        case Unchanged  => name.toString
        case Capitalize => name.toString.capitalize
      }

      private val methodDescriptorName = TermName(updatedName + "MethodDescriptor")

      private val reqType = request.safeType

      private val respType = response.safeInner

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
                _root_.io.grpc.MethodDescriptor.generateFullMethodName(
          ${lit(fullyServiceName)}, ${lit(updatedName)}))
              .build()
          }
        """.supressWarts("Null", "ExplicitImplicitTypes")

      private def clientCallMethodFor(clientMethodName: String) =
        q"$clientCallsImpl.${TermName(clientMethodName)}(input, $methodDescriptorName, channel, options)"

      val clientDef: Tree = streamingType match {
        case Some(RequestStreaming) =>
          q"""
            def $name(input: ${request.getTpe}): ${response.getTpe} = ${clientCallMethodFor(
            "clientStreaming")}"""
        case Some(ResponseStreaming) =>
          q"""
            def $name(input: ${request.getTpe}): ${response.getTpe} = ${clientCallMethodFor(
            "serverStreaming")}"""
        case Some(BidirectionalStreaming) =>
          q"""
            def $name(input: ${request.getTpe}): ${response.getTpe} = ${clientCallMethodFor(
            "bidiStreaming")}"""
        case None =>
          q"""
            def $name(input: ${request.getTpe}): ${response.getTpe} = ${clientCallMethodFor("unary")}"""
      }

      private def serverCallMethodFor(serverMethodName: String) =
        q"_root_.higherkindness.mu.rpc.internal.server.monixCalls.${TermName(serverMethodName)}(algebra.$name, $compressionOption)"

      val descriptorAndHandler: Tree = {
        val handler = (streamingType, prevalentStreamingTarget) match {
          case (Some(RequestStreaming), Fs2StreamTpe(_, _)) =>
            q"_root_.higherkindness.mu.rpc.internal.server.fs2Calls.clientStreamingMethod(algebra.$name, $compressionOption)"
          case (Some(RequestStreaming), MonixObservableTpe(_, _)) =>
            q"_root_.io.grpc.stub.ServerCalls.asyncClientStreamingCall(${serverCallMethodFor("clientStreamingMethod")})"
          case (Some(ResponseStreaming), Fs2StreamTpe(_, _)) =>
            q"_root_.higherkindness.mu.rpc.internal.server.fs2Calls.serverStreamingMethod(algebra.$name, $compressionOption)"
          case (Some(ResponseStreaming), MonixObservableTpe(_, _)) =>
            q"_root_.io.grpc.stub.ServerCalls.asyncServerStreamingCall(${serverCallMethodFor("serverStreamingMethod")})"
          case (Some(BidirectionalStreaming), Fs2StreamTpe(_, _)) =>
            q"_root_.higherkindness.mu.rpc.internal.server.fs2Calls.bidiStreamingMethod(algebra.$name, $compressionOption)"
          case (Some(BidirectionalStreaming), MonixObservableTpe(_, _)) =>
            q"_root_.io.grpc.stub.ServerCalls.asyncBidiStreamingCall(${serverCallMethodFor("bidiStreamingMethod")})"
          case (None, _) =>
            q"_root_.io.grpc.stub.ServerCalls.asyncUnaryCall(_root_.higherkindness.mu.rpc.internal.server.unaryCalls.unaryMethod(algebra.$name, $compressionOption))"
          case _ =>
            sys.error(
              s"Unable to define a handler for the streaming type $streamingType and $prevalentStreamingTarget for the method $name in the service $serviceName")
        }
        q"($methodDescriptorName, $handler)"
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
        case Fs2StreamTpe(_, _) =>
          q"client.stream(request).flatMap(_.asStream[${response.safeInner}])"
        case _ =>
          q"""client.expectOr[${response.safeInner}](request)(handleResponseError)(jsonOf[F, ${response.safeInner}])"""
      }

      val requestTypology: Tree = request match {
        case _: UnaryTpe =>
          q"val request = _root_.org.http4s.Request[F](_root_.org.http4s.Method.$method, uri / ${uri
            .replace("\"", "")}).withEntity(req.asJson)"
        case _: Fs2StreamTpe =>
          q"val request = _root_.org.http4s.Request[F](_root_.org.http4s.Method.$method, uri / ${uri
            .replace("\"", "")}).withEntity(req.map(_.asJson))"
        case _ =>
          q"val request = _root_.org.http4s.Request[F](_root_.org.http4s.Method.$method, uri / ${uri
            .replace("\"", "")})"
      }

      val responseEncoder =
        q"""implicit val responseEntityDecoder: _root_.org.http4s.EntityDecoder[F, ${response.safeInner}] = jsonOf[F, ${response.safeInner}]"""

      def toRequestTree: Tree = request match {
        case _: EmptyTpe =>
          q"""def $name(client: _root_.org.http4s.client.Client[F])(
               implicit responseDecoder: _root_.io.circe.Decoder[${response.safeInner}]): ${response.getTpe} = {
		                  $responseEncoder
		                  $requestTypology
		                  $executionClient
		                 }"""
        case _ =>
          q"""def $name(req: ${request.getTpe})(client: _root_.org.http4s.client.Client[F])(
               implicit requestEncoder: _root_.io.circe.Encoder[${request.safeInner}],
               responseDecoder: _root_.io.circe.Decoder[${response.safeInner}]
            ): ${response.getTpe} = {
		                  $responseEncoder
		                  $requestTypology
		                  $executionClient
		                 }"""
      }

      val routeTypology: Tree = (request, response) match {
        case (_: Fs2StreamTpe, _: UnaryTpe) =>
          q"""val requests = msg.asStream[${operation.request.safeInner}]
              _root_.org.http4s.Status.Ok.apply(handler.${operation.name}(requests).map(_.asJson))"""

        case (_: UnaryTpe, _: Fs2StreamTpe) =>
          q"""for {
              request   <- msg.as[${operation.request.safeInner}]
              responses <- _root_.org.http4s.Status.Ok.apply(handler.${operation.name}(request).asJsonEither)
            } yield responses"""

        case (_: Fs2StreamTpe, _: Fs2StreamTpe) =>
          q"""val requests = msg.asStream[${operation.request.safeInner}]
             _root_.org.http4s.Status.Ok.apply(handler.${operation.name}(requests).asJsonEither)"""

        case (_: EmptyTpe, _) =>
          q"""_root_.org.http4s.Status.Ok.apply(handler.${operation.name}(_root_.higherkindness.mu.rpc.protocol.Empty).map(_.asJson))"""

        case _ =>
          q"""for {
              request  <- msg.as[${operation.request.safeInner}]
              response <- _root_.org.http4s.Status.Ok.apply(handler.${operation.name}(request).map(_.asJson)).adaptErrors
            } yield response"""
      }

      val getPattern =
        pq"_root_.org.http4s.Method.GET -> _root_.org.http4s.dsl.impl.Root / ${operation.name.toString}"
      val postPattern =
        pq"msg @ _root_.org.http4s.Method.POST -> _root_.org.http4s.dsl.impl.Root / ${operation.name.toString}"

      def toRouteTree: Tree = request match {
        case _: EmptyTpe => cq"$getPattern => $routeTypology"
        case _           => cq"$postPattern => $routeTypology"
      }

    }

    val operations: List[HttpOperation] = for {
      d      <- rpcDefs.collect { case x if findAnnotation(x.mods, "http").isDefined => x }
      args   <- findAnnotation(d.mods, "http").collect({ case Apply(_, args) => args }).toList
      params <- d.vparamss
      _ = require(params.length == 1, s"RPC call ${d.name} has more than one request parameter")
      p <- params.headOption.toList
      op = Operation(d.name, TypeTypology(p.tpt), TypeTypology(d.tpt))
      _ = if (op.isMonixObservable)
        sys.error(
          "Monix.Observable is not compatible with streaming services. Please consider using Fs2.Stream instead.")
    } yield HttpOperation(op)

    val streamConstraints: List[Tree] = List(q"F: _root_.cats.effect.Sync[$F]")

    val httpRequests = operations.map(_.toRequestTree)

    val HttpClient      = TypeName("HttpClient")
    val httpClientClass = q"""
        class $HttpClient[$F_](uri: _root_.org.http4s.Uri)(implicit ..$streamConstraints) {
          ..$httpRequests
      }"""

    val httpClient = q"""
        def httpClient[$F_](uri: _root_.org.http4s.Uri)
          (implicit ..$streamConstraints): $HttpClient[$F] = {
          new $HttpClient[$F](uri / ${serviceDef.name.toString})
      }"""

    val httpImports: List[Tree] = List(
      q"import _root_.higherkindness.mu.http.implicits._",
      q"import _root_.cats.syntax.flatMap._",
      q"import _root_.cats.syntax.functor._",
      q"import _root_.org.http4s.circe._",
      q"import _root_.io.circe.syntax._"
    )

    val httpRoutesCases: Seq[Tree] = operations.map(_.toRouteTree)

    val routesPF: Tree = q"{ case ..$httpRoutesCases }"

    val requestTypes: Set[String] =
      operations.filterNot(_.operation.request.isEmpty).map(_.operation.request.flatName).toSet

    val responseTypes: Set[String] =
      operations.filterNot(_.operation.response.isEmpty).map(_.operation.response.flatName).toSet

    val requestDecoders =
      requestTypes.map(n =>
        q"""implicit private val ${TermName("entityDecoder" + n)}:_root_.org.http4s.EntityDecoder[F, ${TypeName(
          n)}] = jsonOf[F, ${TypeName(n)}]""")

    val HttpRestService: TypeName = TypeName(serviceDef.name.toString + "RestService")

    val arguments: List[Tree] = List(q"handler: ${serviceDef.name}[F]") ++
      requestTypes.map(n => q"${TermName("decoder" + n)}: _root_.io.circe.Decoder[${TypeName(n)}]") ++
      responseTypes.map(n => q"${TermName("encoder" + n)}: _root_.io.circe.Encoder[${TypeName(n)}]") ++
      streamConstraints

    val httpRestServiceClass: Tree = q"""
        class $HttpRestService[$F_](implicit ..$arguments) extends _root_.org.http4s.dsl.Http4sDsl[F] {
         ..$requestDecoders
         def service = _root_.org.http4s.HttpRoutes.of[F]{$routesPF}
      }"""

    val httpService = q"""
        def route[$F_](implicit ..$arguments): _root_.higherkindness.mu.http.protocol.RouteMap[F] = {
          _root_.higherkindness.mu.http.protocol.RouteMap[F](${serviceDef.name.toString}, new $HttpRestService[$F].service)
      }"""

    val http =
      if (httpRequests.isEmpty) Nil
      else
        httpImports ++ List(httpClientClass, httpClient, httpRestServiceClass, httpService)
  }

  private def process(service: RpcService, packageName: String): Unit = {
    val serviceObject = ModuleDef(
      Modifiers().supressWarts("Any", "NonUnitStatements", "StringPlusAny", "Throw"),
      TermName(service.serviceName + "Object"),
      Template(
        List(TypeTree(typeOf[AnyRef])),
        noSelfType,
        List(
          DefDef(
            Modifiers(),
            termNames.CONSTRUCTOR,
            Nil,
            List(Nil),
            TypeTree(),
            Block(List(pendingSuperCall), Literal(Constant(()))))) ++
          service.imports ++
          service.methodDescriptors ++
          service.encodersImport ++ List(
          service.bindService,
          service.clientClass,
          service.client,
          service.clientFromChannel,
          service.unsafeClient,
          service.unsafeClientFromChannel
        ) ++ service.http
      )
    )
    printSourceFile(packageName, Seq(), TypeName(service.serviceName + "Object"), serviceObject)
  }

  private def printSourceFile(
      packageName: String,
      imports: Seq[String], //TODO: copy imports from the service definition
      className: TypeName,
      contents: Tree*): Unit = {
    val packagePath = new File(outputDir, packageName.replace('.', '/'))
    packagePath.mkdirs()
    withResource(new PrintWriter(new File(packagePath, s"$className.scala"))) { writer =>
      writer.println(s"package $packageName")
      writer.println()
      imports.foreach(imp => writer.println(s"import $imp"))
      writer.println("import higherkindness.mu.rpc.protocol._")
      writer.println()
      val printer = new CodePrinter(writer, printRootPkg = false)
      contents.foreach(content => printer.print(content))
    }
  }

  private def withResource[T <: AutoCloseable, V](resource: T)(f: T => V): V =
    try f(resource)
    finally resource.close()

  private def findAnnotation(mods: Modifiers, name: String): Option[Tree] =
    mods.annotations find {
      case Apply(Select(New(Ident(TypeName(`name`))), _), _)     => true
      case Apply(Select(New(Select(_, TypeName(`name`))), _), _) => true
      case _                                                     => false
    }
}
// $COVERAGE-ON$
