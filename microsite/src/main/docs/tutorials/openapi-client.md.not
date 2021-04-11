---
layout: docs
title: OpenAPI REST client
section: tutorials
permalink: /tutorials/openapi-client
---

# Tutorial: OpenAPI REST client

This tutorial will show you how to build a REST client based on an [OpenAPI]
(YAML) specification.

This tutorial is aimed at developers who:

* are new to Mu-Scala
* have some understanding of REST APIs and OpenAPI
* have read the [Getting Started guide](../getting-started)

## Configure sbt

Create a new sbt project, and add the `sbt-mu-srcgen` plugin in
`project/plugins.sbt`.

This plugin is going to discover and parse your OpenAPI YAML
file and generate corresponding Scala code.

```sbt
addSbtPlugin("io.higherkindness" % "sbt-mu-srcgen" % "@VERSION@")
```

Then configure the plugin by adding a few lines to `build.sbt`:

```sbt
import higherkindness.mu.rpc.srcgen.Model._

// Look for OpenAPI YAML files
muSrcGenIdlType := IdlType.OpenAPI

// Generate code that is compatible with http4s v0.20.x
muSrcGenOpenApiHttpImpl := higherkindness.mu.rpc.srcgen.openapi.OpenApiSrcGenerator.HttpImpl.Http4sV20
```

The generated client will make use of

* [http4s] for the underlying HTTP client
* [Circe] for JSON serialization

So we need to add the appropriate dependencies to make it compile:

```sbt
libraryDependencies ++= Seq(
  "io.circe"   %% "circe-core"          % "0.12.3",
  "io.circe"   %% "circe-generic"       % "0.12.3",
  "org.http4s" %% "http4s-blaze-client" % "0.20.16",
  "org.http4s" %% "http4s-circe"        % "0.20.16"
)
```

## Add OpenAPI specification file

Suppose you want to generate Scala code for a REST service based on the
"Petstore" example OpenAPI IDL file (available for download 
[here](https://github.com/OAI/OpenAPI-Specification/blob/master/examples/v3.0/petstore.yaml)).

Download that file and save it as `src/main/resources/petstore/petstore.yaml`.

## Generate the code

You can run the source generator directly:

```shell script
sbt muSrcGen
```

or as part of compilation:

```shell script
sbt compile
```

Once the source generator has run, there should be a generated Scala file at
`target/scala-2.13/src_managed/main/petstore/petstore.scala`.

The file is very large so we won't show it here, but it contains:

* case classes for all the models
* Circe `Encoder`/`Decoder`s and http4s `EntityEncoder`/`EntityDecoder`s for all models
* An interface for a client for the REST API:
    ```scala
    trait PetstoreClient[F[_]] {
      import PetstoreClient._
      def getPets(limit: Option[Int], name: Option[String]): F[Pets]
      def createPet(newPet: NewPet): F[Either[CreatePetErrorResponse, Unit]]
      def getPet(petId: Int): F[Either[GetPetErrorResponse, Pet]]
      def deletePet(petId: Int): F[Unit]
      def updatePet(petId: Int, updatePet: UpdatePet): F[Unit]
    }
    ```
* An object containing factory methods to build an http4s-based client:
    ```scala
    object PetstoreHttpClient {
      def build[F[_]: Effect: Sync](client: Client[F], baseUrl: Uri)(implicit ...): PetstoreClient[F] = ...
      def apply[F[_]: ConcurrentEffect](baseUrl: Uri)(implicit ...): Resource[F, PetstoreClient[F]] = ...
    }
    ```

## Fix compilation errors

There is a known issue with name clashes in the generated source. If you see
compilation errors that look like this:

```
reference to Error is ambiguous;
 it is both defined in object models and imported subsequently by
 import io.circe._
```

you need to manually update the Circe import from `import io.circe._` to
`import.io.circe.{Error => _, _}`.

## Use the client

Here is an example showing how to use the generated REST client.

First some imports:

```scala
import petstore.models.Pets
import petstore.SwaggerPetstoreClient.ListPetsErrorResponse

import org.http4s._
import org.http4s.implicits._
import cats.effect.{IO, IOApp, ExitCode}
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import scala.concurrent.ExecutionContext.Implicits.global
```

Then we need to define some encoders to tell the client how it should encode
query parameters:

```scala
trait QueryParamEncoders {

  def localDateTimeQueryParamEncoder(formatter: DateTimeFormatter): QueryParamEncoder[LocalDateTime] =
    QueryParamEncoder[String].contramap[LocalDateTime](formatter.format)

  def localDateQueryParamEncoder(formatter: DateTimeFormatter): QueryParamEncoder[LocalDate] =
    QueryParamEncoder[String].contramap[LocalDate](formatter.format)

  implicit val isoLocalDateTimeEncoder: QueryParamEncoder[LocalDateTime] =
    localDateTimeQueryParamEncoder(DateTimeFormatter.ISO_LOCAL_DATE_TIME)

  implicit val isoLocalDateEncoder: QueryParamEncoder[LocalDate] =
    localDateQueryParamEncoder(DateTimeFormatter.ISO_LOCAL_DATE)

}
```

And finally an `IOApp` that builds a client, uses it to hit the "list pets"
endpoint and prints the response:

```scala
object ClientDemo extends IOApp with QueryParamEncoders {

  val baseUrl = uri"http://localhost:8080"

  val clientResource = SwaggerPetstoreHttpClient[IO](baseUrl)

  def run(args: List[String]): IO[ExitCode] = for {
    response <- clientResource.use(c => c.listPets(limit = Some(10)))
    _        <- printPets(response)
  } yield ExitCode.Success

  def printPets(response: Either[ListPetsErrorResponse, Pets]): IO[Unit] = response match {
    case Left(error) =>
      IO(println(s"Received an error response! $error"))
    case Right(pets) =>
      IO(println(s"Received a list of pets response! $pets"))
  }

}
```

## Working example

For a full working example project including both a server and a client, check out
[47deg/petstore4s](https://github.com/47deg/petstore4s).

[Circe]: https://circe.github.io/circe/
[http4s]: https://http4s.org/
[OpenAPI]: https://swagger.io/docs/specification/about/