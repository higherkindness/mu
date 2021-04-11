import microsites.MicrositesPlugin.autoImport._
import sbt.Keys._
import sbt._
import scoverage.ScoverageKeys._

import scala.language.reflectiveCalls
import mdoc.MdocPlugin.autoImport._
import ch.epfl.scala.sbtmissinglink.MissingLinkPlugin.autoImport._

object ProjectPlugin extends AutoPlugin {

  override def trigger: PluginTrigger = allRequirements

  object autoImport {

    lazy val V = new {
      val avro4s: String                = "3.1.0"
      val betterMonadicFor: String      = "0.3.1"
      val catsEffect: String            = "3.0.1"
      val circe: String                 = "0.13.0"
      val dockerItScala                 = "0.9.9"
      val dropwizard: String            = "4.1.19"
      val embeddedKafka: String         = "2.7.0"
      val enumeratum: String            = "1.6.1"
      val fs2: String                   = "3.0.1"
      val fs2Grpc: String               = "1.0.1"
      val fs2Kafka: String              = "2.0.0-RC1"
      val grpc: String                  = "1.36.1"
      val http4s: String                = "1.0.0-M20"
      val kindProjector: String         = "0.11.3"
      val log4cats: String              = "2.0.1"
      val log4s: String                 = "1.9.0"
      val logback: String               = "1.2.3"
      val scalalogging: String          = "3.9.3" // used in tests
      val monix: String                 = "3.3.0"
      val natchez: String               = "0.1.0"
      val nettySSL: String              = "2.0.34.Final"
      val paradise: String              = "2.1.1"
      val pbdirect: String              = "0.6.1"
      val prometheus: String            = "0.10.0"
      val pureconfig: String            = "0.14.1"
      val reactiveStreams: String       = "1.0.3"
      val scalaCollectionCompat: String = "2.4.3"
      val scalacheckToolbox: String     = "0.5.0"
      val scalamock: String             = "5.1.0"
      val scalatest: String             = "3.2.7"
      val scalatestplusScheck: String   = "3.2.2.0"
      val slf4j: String                 = "1.7.30"
    }

    lazy val rpcServiceSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "org.typelevel"          %% "cats-effect-std"         % V.catsEffect,
        "com.47deg"              %% "pbdirect"                % V.pbdirect,
        "com.beachape"           %% "enumeratum"              % V.enumeratum,
        "com.sksamuel.avro4s"    %% "avro4s-core"             % V.avro4s,
        "org.log4s"              %% "log4s"                   % V.log4s,
        "org.tpolecat"           %% "natchez-core"            % V.natchez,
        "org.scala-lang.modules" %% "scala-collection-compat" % V.scalaCollectionCompat,
        "io.grpc"                 % "grpc-stub"               % V.grpc
      ),
      scalacOptions --= on(2, 13)("-Wunused:patvars").value,
      scalacOptions --= on(2, 12)("-Ywarn-unused:patvars").value
    )

    lazy val macroSettings: Seq[Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        scalaOrganization.value % "scala-compiler" % scalaVersion.value % Provided
      ),
      libraryDependencies ++= on(2, 12)(
        compilerPlugin("org.scalamacros" %% "paradise" % V.paradise cross CrossVersion.full)
      ).value,
      scalacOptions ++= on(2, 13)("-Ymacro-annotations").value
    )

    lazy val monixSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "io.monix"           %% "monix"            % V.monix,
        "org.reactivestreams" % "reactive-streams" % V.reactiveStreams
      )
    )

    lazy val fs2Settings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "co.fs2"                %% "fs2-core"     % V.fs2,
        "org.lyranthe.fs2-grpc" %% "java-runtime" % V.fs2Grpc
      )
    )

    lazy val clientNettySettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "io.grpc" % "grpc-netty" % V.grpc
      )
    )

    lazy val clientOkHttpSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "io.grpc" % "grpc-okhttp" % V.grpc
      )
    )

    lazy val clientCacheSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "org.log4s"     %% "log4s"              % V.log4s,
        "co.fs2"        %% "fs2-core"           % V.fs2,
        "org.typelevel" %% "cats-effect-kernel" % V.catsEffect,
        compilerPlugin("com.olegpy" %% "better-monadic-for" % V.betterMonadicFor)
      )
    )

    lazy val healthCheckSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "org.typelevel" %% "cats-effect" % V.catsEffect
      )
    )

    lazy val serverSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "io.grpc" % "grpc-netty" % V.grpc
      )
    )

    lazy val httpSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "org.http4s"    %% "http4s-core"         % V.http4s exclude ("org.typelevel", "literally"),
        "org.http4s"    %% "http4s-dsl"          % V.http4s,
        "org.http4s"    %% "http4s-blaze-server" % V.http4s,
        "org.http4s"    %% "http4s-circe"        % V.http4s,
        "com.comcast"   %% "ip4s-core"           % "3.0.1", // issue with http4s version
        "org.typelevel" %% "literally"           % "1.0.0", // issue with http4s version
        "io.grpc"        % "grpc-stub"           % V.grpc
      )
    )

    lazy val configSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "com.github.pureconfig" %% "pureconfig" % V.pureconfig
      )
    )

    lazy val prometheusMetricsSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "io.prometheus" % "simpleclient" % V.prometheus
      )
    )

    lazy val dropwizardMetricsSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "io.dropwizard.metrics" % "metrics-core" % V.dropwizard
      )
    )

    lazy val testingSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "io.grpc"        % "grpc-testing" % V.grpc,
        "org.typelevel" %% "cats-effect"  % V.catsEffect
      )
    )

    lazy val nettySslSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "io.netty" % "netty-tcnative-boringssl-static" % V.nettySSL
      )
    )

    lazy val kafkaSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "com.github.fd4s"            %% "fs2-kafka"       % V.fs2Kafka,
        "org.typelevel"              %% "log4cats-slf4j"  % V.log4cats,
        "org.typelevel"              %% "log4cats-core"   % V.log4cats,
        "com.sksamuel.avro4s"        %% "avro4s-core"     % V.avro4s,
        "ch.qos.logback"              % "logback-classic" % V.logback,
        "io.github.embeddedkafka"    %% "embedded-kafka"  % V.embeddedKafka % Test,
        "com.typesafe.scala-logging" %% "scala-logging"   % V.scalalogging  % Test,
        compilerPlugin("com.olegpy" %% "better-monadic-for" % V.betterMonadicFor)
      )
    )

    lazy val benchmarksSettings: Seq[Def.Setting[_]] = Seq(
      Compile / unmanagedSourceDirectories += {
        baseDirectory.value.getParentFile / "shared" / "src" / "main" / "scala"
      },
      Compile / unmanagedResourceDirectories += {
        baseDirectory.value.getParentFile / "shared" / "src" / "main" / "resources"
      },
      Test / unmanagedSourceDirectories += {
        baseDirectory.value.getParentFile / "shared" / "src" / "test" / "scala"
      },
      libraryDependencies ++= Seq(
        "io.grpc"        % "grpc-all"         % V.grpc,
        "org.typelevel" %% "log4cats-slf4j"   % V.log4cats,
        "org.typelevel" %% "log4cats-core"    % V.log4cats,
        "org.slf4j"      % "log4j-over-slf4j" % V.slf4j,
        "org.slf4j"      % "jul-to-slf4j"     % V.slf4j,
        "org.slf4j"      % "jcl-over-slf4j"   % V.slf4j,
        "org.slf4j"      % "slf4j-api"        % V.slf4j,
        "ch.qos.logback" % "logback-core"     % V.logback,
        "ch.qos.logback" % "logback-classic"  % V.logback
      )
    )

    lazy val micrositeSettings: Seq[Def.Setting[_]] = Seq(
      micrositeName := "Mu-Scala",
      micrositeBaseUrl := "mu-scala",
      micrositeDescription := "A purely functional library for building RPC endpoint-based services",
      micrositeGithubOwner := "higherkindness",
      micrositeGithubRepo := "mu-scala",
      micrositeGitterChannelUrl := "47deg/mu",
      micrositeOrganizationHomepage := "https://www.47deg.com",
      micrositePushSiteWith := GitHub4s,
      mdocIn := (Compile / sourceDirectory).value / "docs",
      micrositeGithubToken := Option(System.getenv().get("GITHUB_TOKEN")),
      micrositePalette := Map(
        "brand-primary"   -> "#001e38",
        "brand-secondary" -> "#F44336",
        "white-color"     -> "#E6E7EC"
      ),
      micrositeHighlightTheme := "github-gist",
      micrositeHighlightLanguages += "protobuf"
    )

    lazy val docsSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "org.scalatest"        %% "scalatest"       % V.scalatest,
        "org.scalatestplus"    %% "scalacheck-1-14" % V.scalatestplusScheck,
        "io.dropwizard.metrics" % "metrics-jmx"     % V.dropwizard,
        "org.tpolecat"         %% "natchez-jaeger"  % V.natchez
      ),
      scalacOptions ~= (_ filterNot Set(
        "-Xfatal-warnings",
        "-Ywarn-unused-import",
        "-Xlint"
      ).contains)
    )

    lazy val testSettings = Seq(
      publishArtifact := false,
      libraryDependencies ++= Seq(
        "io.grpc"                  % "grpc-netty"                      % V.grpc                  % Test,
        "io.netty"                 % "netty-tcnative-boringssl-static" % V.nettySSL              % Test,
        "org.scalatest"           %% "scalatest"                       % V.scalatest             % Test,
        "org.scalatestplus"       %% "scalacheck-1-14"                 % V.scalatestplusScheck   % Test,
        "org.scalamock"           %% "scalamock"                       % V.scalamock             % Test,
        "com.47deg"               %% "scalacheck-toolbox-datetime"     % V.scalacheckToolbox     % Test,
        "org.scala-lang.modules"  %% "scala-collection-compat"         % V.scalaCollectionCompat % Test,
        "io.github.embeddedkafka" %% "embedded-kafka"                  % V.embeddedKafka         % Test,
        "org.http4s"              %% "http4s-blaze-client"             % V.http4s                % Test,
        "io.circe"                %% "circe-generic"                   % V.circe                 % Test,
        "ch.qos.logback"           % "logback-classic"                 % V.logback               % Test,
        "org.slf4j"                % "slf4j-nop"                       % V.slf4j                 % Test
      )
    )

    lazy val haskellIntegrationTestSettings = Seq(
      publishArtifact := false,
      Test / parallelExecution := false,
      libraryDependencies ++= Seq(
        "co.fs2"        %% "fs2-core"                    % V.fs2,
        "org.scalatest" %% "scalatest"                   % V.scalatest     % Test,
        "com.whisk"     %% "docker-testkit-scalatest"    % V.dockerItScala % Test,
        "com.whisk"     %% "docker-testkit-impl-spotify" % V.dockerItScala % Test
      )
    )

    def on[A](major: Int, minor: Int)(a: A): Def.Initialize[Seq[A]] =
      Def.setting {
        CrossVersion.partialVersion(scalaVersion.value) match {
          case Some(v) if v == (major, minor) => Seq(a)
          case _                              => Nil
        }
      }

  }

  lazy val missingLinkSettings: Seq[Def.Setting[_]] =
    Seq(
      missinglinkExcludedDependencies += moduleFilter(organization = "ch.qos.logback"),
      missinglinkExcludedDependencies += moduleFilter(
        organization = "org.slf4j",
        name = "slf4j-api"
      ),
      missinglinkExcludedDependencies += moduleFilter(
        organization = "org.apache.commons",
        name = "commons-compress"
      ),
      missinglinkIgnoreSourcePackages += IgnoredPackage("org.apache.avro.file"),
      missinglinkIgnoreSourcePackages += IgnoredPackage("io.netty.util.internal.logging"),
      missinglinkIgnoreSourcePackages += IgnoredPackage("io.netty.handler.ssl")
    )

  import autoImport._

  override def projectSettings: Seq[Def.Setting[_]] =
    Seq(
      Test / fork := true,
      Compile / compileOrder := CompileOrder.JavaThenScala,
      coverageFailOnMinimum := false,
      addCompilerPlugin(
        "org.typelevel" % "kind-projector" % V.kindProjector cross CrossVersion.full
      )
    ) ++ macroSettings ++ missingLinkSettings
}
