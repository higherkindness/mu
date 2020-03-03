import com.typesafe.sbt.site.jekyll.JekyllPlugin.autoImport._
import microsites.MicrositesPlugin.autoImport._
import microsites._
import sbt.Keys._
import sbt.ScriptedPlugin.autoImport._
import sbt._
import sbtorgpolicies.OrgPoliciesPlugin
import sbtorgpolicies.OrgPoliciesPlugin.autoImport._
import sbtorgpolicies.model._
import sbtorgpolicies.templates._
import sbtorgpolicies.templates.badges._
import sbtorgpolicies.runnable.syntax._
import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease.ReleaseStateTransformations._
import scoverage.ScoverageKeys._

import scala.language.reflectiveCalls
import mdoc.MdocPlugin.autoImport._

object ProjectPlugin extends AutoPlugin {

  override def requires: Plugins = OrgPoliciesPlugin

  override def trigger: PluginTrigger = allRequirements

  object autoImport {

    lazy val V = new {
      val avro4s: String              = "3.0.8"
      val avrohugger: String          = "1.0.0-RC22"
      val betterMonadicFor: String    = "0.3.1"
      val catsEffect: String          = "2.1.2"
      val circe: String               = "0.13.0"
      val dropwizard: String          = "4.1.4"
      val embeddedKafka: String       = "2.4.0"
      val enumeratum: String          = "1.5.15"
      val frees: String               = "0.8.2"
      val fs2: String                 = "2.2.2"
      val fs2Grpc: String             = "0.6.0"
      val fs2Kafka: String            = "0.20.2"
      val grpc: String                = "1.24.2"
      val jodaTime: String            = "2.10.5"
      val http4s: String              = "0.21.0-M6"
      val kindProjector: String       = "0.10.3"
      val log4cats: String            = "1.0.1"
      val log4s: String               = "1.8.2"
      val logback: String             = "1.2.3"
      val monix: String               = "3.1.0"
      val monocle: String             = "2.0.2"
      val nettySSL: String            = "2.0.25.Final"
      val paradise: String            = "2.1.1"
      val pbdirect: String            = "0.5.0"
      val prometheus: String          = "0.8.1"
      val pureconfig: String          = "0.12.3"
      val reactiveStreams: String     = "1.0.3"
      val scala: String               = "2.12.10"
      val scopt: String               = "3.7.1"
      val scalacheck: String          = "1.14.3"
      val scalacheckToolbox: String   = "0.3.2"
      val scalamock: String           = "4.4.0"
      val scalatest: String           = "3.1.1"
      val scalatestplusScheck: String = "3.1.0.0-RC2"
      val skeuomorph: String          = "0.0.22"
      val slf4j: String               = "1.7.30"
    }

    lazy val commonSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        %%("cats-effect", V.catsEffect)                  % Test,
        %%("scalamock", V.scalamock)                     % Test,
        %%("scheckToolboxDatetime", V.scalacheckToolbox) % Test,
        "org.scalatestplus"                              %% "scalatestplus-scalacheck" % V.scalatestplusScheck % Test
      )
    )

    lazy val internalSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        %%("cats-effect", V.catsEffect),
        %("grpc-stub", V.grpc),
        "com.47deg"    %% "pbdirect"   % V.pbdirect,
        "com.beachape" %% "enumeratum" % V.enumeratum,
        %%("avro4s", V.avro4s),
        %%("log4s", V.log4s),
        %%("scalamock", V.scalamock) % Test
      )
    )

    lazy val codegenSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "org.scala-lang" % "scala-compiler" % scalaVersion.value
      ),
      exportJars := true // for use in internal plugin dependencies
    )

    lazy val internalMonixSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        %%("monix", V.monix),
        "org.reactivestreams"        % "reactive-streams" % V.reactiveStreams,
        %%("scalamock", V.scalamock) % Test
      )
    )

    lazy val internalFs2Settings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        %%("fs2-core", V.fs2),
        "org.lyranthe.fs2-grpc"      %% "java-runtime" % V.fs2Grpc,
        %%("scalamock", V.scalamock) % Test
      )
    )

    lazy val clientCoreSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        %%("cats-effect", V.catsEffect),
        %%("scalamock", V.scalamock) % Test,
        %("grpc-netty", V.grpc)      % Test
      )
    )

    lazy val clientNettySettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        %("grpc-netty", V.grpc),
        "io.netty" % "netty-tcnative-boringssl-static" % V.nettySSL % Test
      )
    )

    lazy val clientOkHttpSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        %("grpc-okhttp", V.grpc)
      )
    )

    lazy val clientCacheSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        %%("log4s", V.log4s),
        %%("fs2-core", V.fs2),
        %%("cats-effect", V.catsEffect),
        compilerPlugin("com.olegpy" %% "better-monadic-for" % V.betterMonadicFor)
      )
    )

    lazy val healthCheckSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        %%("cats-effect", V.catsEffect)
      )
    )

    lazy val healthCheckSettingsFS2: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "io.chrisdavenport" %% "log4cats-core"  % V.log4cats,
        "io.chrisdavenport" %% "log4cats-slf4j" % V.log4cats,
        %%("fs2-core", V.fs2),
        %%("cats-effect", V.catsEffect)
      )
    )

    lazy val healthCheckSettingsMonix: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "io.chrisdavenport" %% "log4cats-core"  % V.log4cats,
        "io.chrisdavenport" %% "log4cats-slf4j" % V.log4cats,
        %%("monix", V.monix),
        %%("cats-effect", V.catsEffect)
      )
    )

    lazy val serverSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        %("grpc-netty", V.grpc),
        %%("scalamock", V.scalamock) % Test,
        "io.netty"                   % "netty-tcnative-boringssl-static" % V.nettySSL % Test
      )
    )

    lazy val httpSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        %%("http4s-dsl", V.http4s),
        %%("http4s-blaze-server", V.http4s),
        %%("http4s-circe", V.http4s),
        %%("http4s-blaze-client", V.http4s) % Test,
        %%("circe-generic", V.circe)        % Test,
        "ch.qos.logback"                    % "logback-classic" % V.logback % Test
      )
    )

    lazy val configSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        %%("pureconfig", V.pureconfig)
      )
    )

    lazy val prometheusMetricsSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        %("prometheus", V.prometheus)
      )
    )

    lazy val dropwizardMetricsSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "io.dropwizard.metrics" % "metrics-core" % V.dropwizard
      )
    )

    lazy val testingSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        %("grpc-testing", V.grpc),
        %%("cats-effect", V.catsEffect),
        %%("scalacheck", V.scalacheck) % Test,
        "org.scalatestplus"            %% "scalatestplus-scalacheck" % V.scalatestplusScheck % Test
      )
    )

    lazy val nettySslSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "io.netty" % "netty-tcnative-boringssl-static" % V.nettySSL
      )
    )

    lazy val srcGenSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        %%("monocle-core", V.monocle),
        "io.higherkindness" %% "skeuomorph"      % V.skeuomorph,
        "com.julianpeeters" %% "avrohugger-core" % V.avrohugger,
        %%("circe-generic", V.circe)
      )
    )

    lazy val kafkaSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "com.ovoenergy"           %% "fs2-kafka"      % V.fs2Kafka,
        "io.github.embeddedkafka" %% "embedded-kafka" % V.embeddedKafka % Test
      )
    )

    lazy val exampleRouteguideRuntimeSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        %%("monix", V.monix)
      )
    )

    lazy val marshallersJodatimeSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        %("joda-time", V.jodaTime),
        %%("scheckToolboxDatetime", V.scalacheckToolbox) % Test
      )
    )

    lazy val exampleRouteguideCommonSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        %%("circe-core", V.circe),
        %%("circe-generic", V.circe),
        %%("circe-parser", V.circe),
        %%("log4s", V.log4s),
        %("logback-classic", V.logback)
      )
    )

    lazy val exampleSeedLogSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        %("logback-classic", V.logback),
        "io.chrisdavenport" %% "log4cats-core"  % V.log4cats,
        "io.chrisdavenport" %% "log4cats-slf4j" % V.log4cats
      ))

    lazy val exampleSeedConfigSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "org.typelevel"         %% "cats-effect" % V.catsEffect,
        "com.github.pureconfig" %% "pureconfig"  % V.pureconfig))

    lazy val exampleSeedClientAppSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "com.github.scopt" %% "scopt" % V.scopt
      ))

    lazy val exampleTodolistCommonSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        "io.frees" %% "frees-todolist-lib" % V.frees,
        %%("log4s", V.log4s),
        %("logback-classic", V.logback)
      )
    )

    lazy val sbtPluginSettings: Seq[Def.Setting[_]] = Seq(
      sbtPlugin := true,
      scriptedLaunchOpts ++= Seq(
        "-Xmx2048M",
        "-XX:ReservedCodeCacheSize=256m",
        "-Dversion=" + version.value,
        // See https://github.com/sbt/sbt/issues/3469#issuecomment-521326813
        s"-Dsbt.boot.directory=${file(sys.props("user.home")) / ".sbt" / "boot"}"
      ),
      // Custom release process for the plugin:
      releaseProcess := Seq[ReleaseStep](
        releaseStepCommandAndRemaining("^ publishSigned"),
        releaseStepCommand("sonatypeBundleRelease")
      )
    )

    lazy val crossSettings: Seq[Def.Setting[_]] = Seq(
      unmanagedSourceDirectories in Compile += {
        baseDirectory.value.getParentFile / "shared" / "src" / "main" / "scala"
      },
      unmanagedSourceDirectories in Test += {
        baseDirectory.value.getParentFile / "shared" / "src" / "test" / "scala"
      }
    )

    lazy val micrositeSettings: Seq[Def.Setting[_]] = Seq(
      micrositeName := "Mu-Scala",
      micrositeBaseUrl := "mu-scala",
      micrositeDescription := "A purely functional library for building RPC endpoint-based services",
      micrositeGithubOwner := "higherkindness",
      micrositeGithubRepo := "mu-scala",
      micrositeGitterChannelUrl := "47deg/mu",
      micrositeOrganizationHomepage := "https://www.47deg.com",
      micrositeCompilingDocsTool := WithMdoc,
      micrositePushSiteWith := GitHub4s,
      mdocIn := (sourceDirectory in Compile).value / "docs",
      micrositeGithubToken := sys.env.get(orgGithubTokenSetting.value),
      micrositePalette := Map(
        "brand-primary"   -> "#001e38",
        "brand-secondary" -> "#F44336",
        "white-color"     -> "#E6E7EC"
      ),
      micrositeHighlightTheme := "github-gist",
      micrositeHighlightLanguages += "protobuf"
    )

    lazy val customScalacOptions: Seq[String] =
      scalacAdvancedOptions.filterNot(Set("-Yliteral-types", "-Xlint").contains)
    lazy val docsExclusions: Seq[String] => Seq[String] = (current: Seq[String]) =>
      current.filterNot(Set("-Xfatal-warnings", "-Ywarn-unused-import", "-Xlint").contains)

    lazy val docsSettings: Seq[Def.Setting[_]] = Seq(
      libraryDependencies ++= Seq(
        %%("scalatest", V.scalatest),
        "org.scalatestplus" %% "scalatestplus-scalacheck" % V.scalatestplusScheck
      ),
      scalacOptions ~= docsExclusions
    )

  }

  import autoImport._

  case class FixedCodecovBadge(info: BadgeInformation) extends Badge(info) {

    override def badgeIcon: Option[BadgeIcon] =
      BadgeIcon(
        title = "codecov.io",
        icon = s"http://codecov.io/gh/${info.owner}/${info.repo}/branch/master/graph/badge.svg",
        url = s"http://codecov.io/gh/${info.owner}/${info.repo}"
      ).some
  }

  override def projectSettings: Seq[Def.Setting[_]] =
    warnUnusedImport ++ Seq(
      description := "mu RPC is a purely functional library for " +
        "building RPC endpoint based services with support for RPC and HTTP/2",
      startYear := Some(2017),
      orgProjectName := "mu-scala",
      orgGithubSetting := GitHubSettings(
        organization = "higherkindness",
        project = (name in LocalRootProject).value,
        organizationName = "47 Degrees",
        groupId = "io.higherkindness",
        organizationHomePage = url("http://47deg.com"),
        organizationEmail = "hello@47deg.com"
      ),
      scalaVersion := V.scala,
      crossScalaVersions := Seq(V.scala),
      scalacOptions ++= customScalacOptions,
      Test / fork := true,
      compileOrder in Compile := CompileOrder.JavaThenScala,
      coverageFailOnMinimum := false,
      addCompilerPlugin(%%("paradise", V.paradise) cross CrossVersion.full),
      addCompilerPlugin(%%("kind-projector", V.kindProjector) cross CrossVersion.binary),
      libraryDependencies ++= Seq(
        %%("scalatest", V.scalatest) % Test,
        %("slf4j-nop", V.slf4j)      % Test
      ),
      releaseProcess := Seq[ReleaseStep](
        orgInitialVcsChecks,
        checkSnapshotDependencies,
        orgInquireVersions,
        orgTagRelease,
        orgUpdateChangeLog,
        releaseStepCommandAndRemaining("publishSigned"),
        releaseStepCommand("sonatypeBundleRelease"),
        setNextVersion,
        orgCommitNextVersion,
        orgPostRelease
      )
    ) ++ Seq(
      // sbt-org-policies settings:
      // format: OFF
      orgMaintainersSetting := List(Dev("developer47deg", Some("47 Degrees (twitter: @47deg)"), Some("hello@47deg.com"))),
      orgBadgeListSetting := List(
        TravisBadge.apply,
        FixedCodecovBadge.apply,
        { info => MavenCentralBadge.apply(info.copy(libName = "mu-scala")) },
        ScalaLangBadge.apply,
        LicenseBadge.apply,
        // Gitter badge (owner field) can be configured with default value if we migrate it to the higherkindness organization
        { info => GitterBadge.apply(info.copy(owner = "47deg", repo = "mu")) },
        GitHubIssuesBadge.apply
      ),
      orgEnforcedFilesSetting := List(
        LicenseFileType(orgGithubSetting.value, orgLicenseSetting.value, startYear.value),
        ContributingFileType(
          orgProjectName.value,
          // Organization field can be configured with default value if we migrate it to the higherkindness organization
          orgGithubSetting.value.copy(organization = "47deg", project = "mu-scala")
        ),
        AuthorsFileType(
          name.value,
          orgGithubSetting.value,
          orgMaintainersSetting.value,
          orgContributorsSetting.value),
        NoticeFileType(orgProjectName.value, orgGithubSetting.value, orgLicenseSetting.value, startYear.value),
        VersionSbtFileType,
        ChangelogFileType,
        ReadmeFileType(
          orgProjectName.value,
          orgGithubSetting.value,
          startYear.value,
          orgLicenseSetting.value,
          orgCommitBranchSetting.value,
          sbtPlugin.value,
          name.value,
          version.value,
          scalaBinaryVersion.value,
          sbtBinaryVersion.value,
          orgSupportedScalaJSVersion.value,
          orgBadgeListSetting.value
        ),
        ScalafmtFileType,
        TravisFileType(crossScalaVersions.value, orgScriptCICommandKey, orgAfterCISuccessCommandKey)
      )
    )
  // format: ON
}
