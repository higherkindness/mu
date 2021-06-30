---
layout: docs
title: Distributed Tracing
section: guides
permalink: /guides/distributed-tracing
---

# Distributed Tracing

Mu provides an integration with [Natchez] to enable distributed tracing of
[gRPC] calls.

<a href="../img/guides/tracing.png">
  <img src="../img/guides/tracing.png" alt="Example of a distributed trace" style="width: 100%; height: auto;">
</a>

Specifically, the integration provides the following features.

## Client

* For every RPC call, the client will create a child span with the fully
  qualified name of the RPC method being called (e.g.
  `com.foo.MyService/SayHello`)
* It will automatically add all necessary trace-related headers to RPC requests.

## Server

* The server will attempt to extract trace-related information from the request
  headers.
* It will create a span using the same naming convention as the client.
    * If the relevant headers were present, it will continue the trace that was
      started upstream, creating a child span.
    * Otherwise, it will create a root span, i.e. a new trace.

## How to use

Let's look at how to enable tracing on the server side first.

### Server side

We'll assume the following service definition:

```scala mdoc:silent
import higherkindness.mu.rpc.protocol._

case class HelloRequest(name: String)
case class HelloResponse(greeting: String)

@service(Protobuf, namespace = Some("com.foo"))
trait MyService[F[_]] {

  def SayHello(req: HelloRequest): F[HelloResponse]

}
```

and an implementation of that definition:

```scala mdoc:silent
import cats.Applicative
import cats.syntax.applicative._

class MyAmazingService[F[_]: Applicative] extends MyService[F] {

  def SayHello(req: HelloRequest): F[HelloResponse] =
    HelloResponse(s"Hello, ${req.name}!").pure[F]

}
```

Ordinarily, if you were not using tracing, you would create a gRPC service
definition using the macro-generated `MyService.bindService` method, specifying
your effect monad of choice:

```scala mdoc:silent
import cats.effect.{IO, IOApp, ExitCode}
import higherkindness.mu.rpc.server.{GrpcServer, AddService}

object OrdinaryServer extends IOApp {

  implicit val service: MyService[IO] = new MyAmazingService[IO]

  def run(args: List[String]): IO[ExitCode] = (for {
    serviceDef <- MyService.bindService[IO]
    _          <- GrpcServer.defaultServer[IO](8080, List(AddService(serviceDef)))
  } yield ()).useForever

}
```

To use the same service with tracing enabled, you need to call the
`MyService.bindTracingService` method instead.

`bindTracingService[F[_]]` differs from `bindService[F[_]]` in two ways, which
we will explain below.

1. It takes a [Natchez] `EntryPoint` as an argument.
2. It takes a `MyService` as an implicit argument, but instead of a
   `MyService[F]` it requires a `MyService[Kleisli[F, Span[F], *]]`.

#### EntryPoint

`EntryPoint[F[_]]`, as the name suggests, is the "entrypoint" into the [Natchez]
API. It's what allows Mu to do things like create root spans.

How you create an `EntryPoint` will depend on what tracing implementation you
want to use. For example, if you use `natchez-jaeger`, you might create a
`Resource` of an `EntryPoint` like this:

```scala mdoc:silent
import cats.effect.{Sync, Resource}

import natchez.EntryPoint
import natchez.jaeger.Jaeger
import io.jaegertracing.Configuration.SamplerConfiguration
import io.jaegertracing.Configuration.ReporterConfiguration

def entryPoint[F[_]: Sync]: Resource[F, EntryPoint[F]] = {
  Jaeger.entryPoint[F]("my-Mu-service") { c =>
    Sync[F].delay {
      c.withSampler(SamplerConfiguration.fromEnv)
       .withReporter(ReporterConfiguration.fromEnv)
       .getTracer
    }
  }
}
```

#### Kleisli

When you instantiate your `MyService` implementation, you need to set its type
parameter to `Kleisli[F, Span[F], *]`. (Note: we are using [kind-projector]
syntax here, but you don't have to.)

Intuitively, this creates a service which, given the current span as input,
returns a result inside the `F` effect.

Luckily, there are instances of most of the cats-effect type classes for
`Kleisli`, all the way down to `Concurrent` (but not `Effect`). So you should be
able to substitute `MyService[Kleisli[F, Span[F], *]]` for `MyService[F]`
without requiring any changes to your service implementation code.

#### Using bindTracingService

Putting all this together, your server setup code will look something like this:

```scala mdoc:silent
import cats.data.Kleisli
import natchez.Span

object TracingServer extends IOApp {

  implicit val service: MyService[Kleisli[IO, Span[IO], *]] =
    new MyAmazingService[Kleisli[IO, Span[IO], *]]

  def run(args: List[String]): IO[ExitCode] =
    entryPoint[IO]
      .flatMap { ep => MyService.bindTracingService[IO](ep) }
      .flatMap { serviceDef =>
        GrpcServer.defaultServer[IO](8080, List(AddService(serviceDef)))
      }.useForever

}
```

#### Tracing your service code

If you wish, you can make use of the [Natchez] `Trace` typeclass to create child
spans:

```scala mdoc:silent
import natchez.Trace
import cats.Monad
import cats.syntax.all._

class MyTracingService[F[_]: Monad: Trace] extends MyService[F] {

  def SayHello(req: HelloRequest): F[HelloResponse] =
    for {
      _ <- Trace[F].span("look stuff up in the database"){ Monad[F].unit }
      _ <- Trace[F].span("do some stuff with Redis"){ Monad[F].unit }
      _ <- Trace[F].span("make an HTTP call"){ Monad[F].unit }
    } yield HelloResponse(s"Hi, ${req.name}!")

}
```

### Client side

Ordinarily, if you were not using tracing, you would create a cats-effect
`Resource` of an RPC client using the macro-generated `MyService.client` method:

```scala mdoc:silent
import higherkindness.mu.rpc.{ChannelFor, ChannelForAddress}

object OrdinaryClientApp extends IOApp {

  val channelFor: ChannelFor = ChannelForAddress("localhost", 8080)

  val clientRes: Resource[IO, MyService[IO]] =
    MyService.client[IO](channelFor)

  def run(args: List[String]): IO[ExitCode] =
    clientRes.use { client =>
      for {
        resp <- client.SayHello(HelloRequest("Chris"))
        _    <- IO(println(s"Response: $resp"))
      } yield (ExitCode.Success)
    }
}
```

To obtain a tracing client, use `MyService.tracingClient` instead of
`MyService.client`.

This returns a `MyService[Kleisli[F, Span[F], *]]`, i.e. a client which takes
the current span as input and returns a response inside the `F` effect.

For example:

```scala mdoc:silent
object TracingClientApp extends IOApp {

  val channelFor: ChannelFor = ChannelForAddress("localhost", 8080)

  val clientRes: Resource[IO, MyService[Kleisli[IO, Span[IO], *]]] =
    MyService.tracingClient[IO](channelFor)

  def run(args: List[String]): IO[ExitCode] =
    entryPoint[IO].use { ep =>
      ep.root("this is the root span").use { currentSpan =>
        clientRes.use { client =>
          val kleisli = client.SayHello(HelloRequest("Chris"))
          for {
            resp <- kleisli.run(currentSpan)
            _    <- IO(println(s"Response: $resp"))
          } yield (ExitCode.Success)
        }
      }
    }

}
```

## Working example

To see a full working example of distributed tracing across multiple Mu
services, take a look at this repo:
[cb372/mu-tracing-example](https://github.com/cb372/mu-tracing-example).

The README explains how to run the example and inspect the resulting traces.


[gRPC]: https://grpc.io/
[kind-projector]: https://github.com/typelevel/kind-projector
[Natchez]: https://github.com/tpolecat/natchez

