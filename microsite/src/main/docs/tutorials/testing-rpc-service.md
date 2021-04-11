---
layout: docs
title: Testing an RPC service
section: tutorials
permalink: tutorials/testing-rpc-service
---

# Tutorial: Testing an RPC service

This tutorial will show you how to write a unit test for an RPC service using an
in-memory channel and client.

This tutorial is aimed at developers who:

* are new to Mu-Scala
* have read the [Getting Started guide](../getting-started)
* have followed the [gRPC server and client tutorial](grpc-server-client)

Mu supports both Protobuf and Avro. For the purposes of this tutorial we will
assume you are using Protobuf, but it's possible to follow the tutorial even if
you are using Avro.

## Service definition

Let's use the following service definition. (For an explanation of how to create
service definition, check out the [RPC service definition with Protobuf tutorial](service-definition/protobuf).)

A client sends a `HelloRequest` containing a name, and the server responds with
a greeting and an indication of whether it is feeling happy or not.

```scala mdoc:silent
import higherkindness.mu.rpc.protocol._

object hello {
  case class HelloRequest(@pbdirect.pbIndex(1) name: String)
  case class HelloResponse(@pbdirect.pbIndex(1) greeting: String, @pbdirect.pbIndex(2) happy: Boolean)

  // Note: the @service annotation in your code might reference Avro instead of Protobuf
  @service(Protobuf, namespace = Some("com.example"))
  trait Greeter[F[_]] {
    def SayHello(req: HelloRequest): F[HelloResponse]
  }

}
```

## Service implementation

Here's the implementation we want to test.

```scala mdoc:silent
import cats.Applicative
import cats.syntax.applicative._
import hello._

class HappyGreeter[F[_]: Applicative] extends Greeter[F] {

  def SayHello(req: HelloRequest): F[HelloResponse] =
    HelloResponse(s"Hello, ${req.name}!", happy = true).pure[F]

}
```

We're going to write a test to check that the service is always happy.


## mu-rpc-testing

You'll need to add a dependency on the `mu-rpc-testing` module. This contains
some helpers for setting up an in-memory service for testing.

```sbt
libraryDependencies += "io.higherkindness" %% "mu-rpc-testing" % "@VERSION@" % Test
```

## Create the service and client

The test will instantiate a `HappyGreeter`, but instead of connecting it to a
real gRPC server and exposing it over HTTP, we'll connect it to an in-memory
channel.

We'll also create a client and connect it to the same in-memory channel, so it
can make requests to the service.

```scala mdoc:silent
import hello._
import cats.effect.{IO, Resource}
import cats.effect.std.Dispatcher
import higherkindness.mu.rpc.testing.servers.withServerChannel

trait ServiceAndClient {

  implicit val greeter: Greeter[IO] = new HappyGreeter[IO]

  /*
   * A cats-effect Resource that builds a gRPC server and client
   * connected to each other via an in-memory channel.
   */
  val clientResource: Resource[IO, Greeter[IO]] = for {
    dispatcher <- Dispatcher[IO]
    sc         <- withServerChannel(Greeter.bindService[IO](dispatcher))
    clientRes  <- Greeter.clientFromChannel[IO](IO.pure(sc.channel))
  } yield clientRes

}
```

The important part here is the use of `withServerChannel`. This is a helper method
provided by the `mu-rpc-testing` module that connects the service to an
in-memory channel so we don't need to start a real gRPC server.

## Write the test

Now we're ready to write our test.

With the service and client in place, the test consists of using the client to
make a request and then asserting that the response matches what we expect.

```scala mdoc:silent
import cats.effect.unsafe.implicits.global
import hello._
import org.scalatest.flatspec.AnyFlatSpec

class ServiceSpec extends AnyFlatSpec with ServiceAndClient {

  behavior of "Greeter service"

  it should "be happy" in {
    val response: HelloResponse = clientResource
      .use(client => client.SayHello(HelloRequest("somebody")))
      .unsafeRunSync()

    assert(response.happy === true)
  }

}
```

## Run the test

Let's see the test in action:

```scala mdoc
org.scalatest.nocolor.run(new ServiceSpec)
```

## Bonus points: property-based test

Since the `HappyGreeter` service is *always* happy, we could also write this as
a property-based test with ScalaCheck to verify that the service's happiness
does not depend on the incoming request.

```scala mdoc:silent
import org.scalatestplus.scalacheck.Checkers
import org.scalacheck.Gen
import org.scalacheck.Prop._

class PropertyBasedServiceSpec extends AnyFlatSpec with ServiceAndClient with Checkers {

  val requestGen: Gen[HelloRequest] = Gen.alphaStr.map(HelloRequest)

  behavior of "Greeter service"

  it should "be happy" in {
    clientResource.use { client =>
      IO {
        check {
          forAll(requestGen) { request =>
            val response: HelloResponse = client.SayHello(request).unsafeRunSync()
            response.happy :| "response should be happy"
          }
        }
      }
    }.unsafeRunSync()
  }

}
```

Let's run this test as well:

```scala mdoc
org.scalatest.nocolor.run(new PropertyBasedServiceSpec)
```
