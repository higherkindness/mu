import sbtorgpolicies.model.scalac

pgpPassphrase := Some(getEnvVar("PGP_PASSPHRASE").getOrElse("").toCharArray)

////////////////
//// COMMON ////
////////////////

lazy val common = project
  .in(file("modules/common"))
  .settings(moduleName := "mu-common")
  .settings(commonSettings)
  .settings(codegenSettings)

lazy val `internal-core` = project
  .in(file("modules/internal"))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(testing % "test->test")
  .settings(moduleName := "mu-rpc-internal-core")
  .settings(internalSettings)

lazy val `internal-monix` = project
  .in(file("modules/internal/monix"))
  .dependsOn(`internal-core` % "compile->compile;test->test")
  .settings(moduleName := "mu-rpc-internal-monix")
  .settings(internalMonixSettings)

lazy val `internal-fs2` = project
  .in(file("modules/internal/fs2"))
  .dependsOn(`internal-core` % "compile->compile;test->test")
  .dependsOn(testing % "test->test")
  .settings(moduleName := "mu-rpc-internal-fs2")
  .settings(internalFs2Settings)

lazy val config = project
  .in(file("modules/config"))
  .dependsOn(common % "test->test")
  .dependsOn(channel % "compile->compile;test->test")
  .dependsOn(server % "compile->compile;test->test")
  .dependsOn(testing % "test->test")
  .settings(moduleName := "mu-config")
  .settings(configSettings)

lazy val testing = project
  .in(file("modules/testing"))
  .settings(moduleName := "mu-rpc-testing")
  .settings(testingSettings)

///////////////////////////////////
//// TRANSPORT LAYER - CHANNEL ////
///////////////////////////////////

lazy val channel = project
  .in(file("modules/channel"))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(`internal-core` % "compile->compile;test->test")
  .dependsOn(testing % "test->test")
  .settings(moduleName := "mu-rpc-channel")
  .settings(clientCoreSettings)

lazy val monix = project
  .in(file("modules/streaming/monix"))
  .dependsOn(channel)
  .dependsOn(`health-check-unary`)
  .dependsOn(`internal-monix`)
  .settings(moduleName := "mu-rpc-monix")

lazy val fs2 = project
  .in(file("modules/streaming/fs2"))
  .dependsOn(channel)
  .dependsOn(`health-check-unary`)
  .dependsOn(`internal-fs2`)
  .settings(moduleName := "mu-rpc-fs2")

lazy val netty = project
  .in(file("modules/channel/netty"))
  .dependsOn(channel % "compile->compile;test->test")
  .settings(moduleName := "mu-rpc-netty")
  .settings(clientNettySettings)

lazy val okhttp = project
  .in(file("modules/channel/okhttp"))
  .dependsOn(channel % "compile->compile;test->test")
  .settings(moduleName := "mu-rpc-okhttp")
  .settings(clientOkHttpSettings)

lazy val ssl = project
  .in(file("modules/channel/netty-ssl"))
  .dependsOn(server % "test->test")
  .dependsOn(`netty` % "compile->compile;test->test")
  .settings(moduleName := "mu-rpc-netty-ssl")
  .settings(nettySslSettings)

////////////////
//// CLIENT ////
////////////////

lazy val `client-cache` = project
  .in(file("modules/client-cache"))
  .dependsOn(common % "test->test")
  .settings(moduleName := "mu-rpc-client-cache")
  .settings(clientCacheSettings)

////////////////
//// SERVER ////
////////////////

lazy val server = project
  .in(file("modules/server"))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(`internal-core` % "compile->compile;test->test")
  .dependsOn(`internal-monix` % "test->test")
  .dependsOn(`internal-fs2` % "test->test")
  .dependsOn(channel % "test->test")
  .dependsOn(testing % "test->test")
  .settings(moduleName := "mu-rpc-server")
  .settings(serverSettings)

/////////////////////
//// HEALTHCHECK ////
/////////////////////

lazy val `health-check-unary` = project
  .in(file("modules/health-check-unary"))
  .dependsOn(channel)
  .settings(healthCheckSettings)
  .settings(moduleName := "mu-rpc-health-check-unary")

/////////HealthCheck Server Monix Example
lazy val `health-server-monix` = project
  .in(file("modules/examples/health-check/health-server-monix"))
  .dependsOn(monix)
  .dependsOn(`health-check-unary`)
  .dependsOn(server)
  .settings(healthCheckSettingsMonix)
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .settings(moduleName := "mu-rpc-example-health-check-server-monix")

/////////HealthCheck Server FS2 Example
lazy val `health-server-fs2` = project
  .in(file("modules/examples/health-check/health-server-fs2"))
  .dependsOn(fs2)
  .dependsOn(`health-check-unary`)
  .dependsOn(server)
  .settings(healthCheckSettingsFS2)
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .settings(moduleName := "mu-rpc-example-health-check-server-fs2")

/////////HealthCheck Client Example
lazy val `health-client` = project
  .in(file("modules/examples/health-check/health-client"))
  .dependsOn(`health-server-monix`)
  .dependsOn(`health-server-fs2`)
  .dependsOn(config)
  .dependsOn(netty)
  .settings(healthCheckSettingsMonix)
  .settings(healthCheckSettingsFS2)
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .settings(moduleName := "mu-rpc-example-health-check-client")

////////////////////
//// PROMETHEUS ////
////////////////////

lazy val prometheus = project
  .in(file("modules/metrics/prometheus"))
  .dependsOn(`internal-core` % "compile->compile;test->test")
  .dependsOn(testing % "test->test")
  .settings(moduleName := "mu-rpc-prometheus")
  .settings(prometheusMetricsSettings)

////////////////////
//// DROPWIZARD ////
////////////////////

lazy val dropwizard = project
  .in(file("modules/metrics/dropwizard"))
  .dependsOn(`internal-core` % "compile->compile;test->test")
  .dependsOn(testing % "test->test")
  .settings(moduleName := "mu-rpc-dropwizard")
  .settings(dropwizardMetricsSettings)

///////////////////
//// HTTP/REST ////
///////////////////

lazy val http = project
  .in(file("modules/http"))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(server % "compile->compile;test->test")
  .settings(moduleName := "mu-rpc-http")
  .settings(httpSettings)

///////////////
//// KAFKA ////
///////////////

lazy val kafka = project
  .in(file("modules/kafka"))
  .dependsOn(channel)
  .dependsOn(server % "test->test")
  .dependsOn(testing % "test->test")
  .settings(moduleName := "mu-rpc-kafka")
  .settings(kafkaSettings)

////////////////
//// SRCGEN ////
////////////////

lazy val `srcgen-core` = project
  .in(file("modules/srcgen/core"))
  .dependsOn(`internal-core` % "compile->compile;test->test")
  .dependsOn(channel % "test->test")
  .settings(moduleName := "mu-srcgen-core")
  .settings(srcGenSettings)

lazy val `srcgen-sbt` = project
  .in(file("modules/srcgen/plugin"))
  .dependsOn(`srcgen-core`)
  .settings(moduleName := "sbt-mu-srcgen")
  .settings(crossScalaVersions := Seq(V.scala))
  .settings(sbtPluginSettings: _*)
  .enablePlugins(BuildInfoPlugin)
  .settings(buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion))
  .settings(buildInfoPackage := "mu.rpc.srcgen")
  // See https://github.com/sbt/sbt/issues/3248
  .settings(publishLocal := publishLocal
    .dependsOn(
      common / publishLocal,
      `internal-core` / publishLocal,
      channel / publishLocal,
      server / publishLocal,
      `internal-fs2` / publishLocal,
      fs2 / publishLocal,
      `marshallers-jodatime` / publishLocal,
      `srcgen-core` / publishLocal
    )
    .value)
  .enablePlugins(SbtPlugin)

////////////////////
//// BENCHMARKS ////
////////////////////

lazy val lastReleasedV = "0.18.4"

lazy val `benchmarks-vprev` = project
  .in(file("benchmarks/vprev"))
  .settings(
    libraryDependencies ++= Seq(
      "io.higherkindness" %% "mu-rpc-channel" % lastReleasedV,
      "io.higherkindness" %% "mu-rpc-server"  % lastReleasedV,
      "io.higherkindness" %% "mu-rpc-testing" % lastReleasedV
    )
  )
  .settings(coverageEnabled := false)
  .settings(moduleName := "mu-benchmarks-vprev")
  .settings(crossSettings)
  .settings(noPublishSettings)
  .enablePlugins(JmhPlugin)

lazy val `benchmarks-vnext` = project
  .in(file("benchmarks/vnext"))
  .dependsOn(channel)
  .dependsOn(server)
  .dependsOn(testing)
  .settings(coverageEnabled := false)
  .settings(moduleName := "mu-benchmarks-vnext")
  .settings(crossSettings)
  .settings(noPublishSettings)
  .enablePlugins(JmhPlugin)

//////////////////
//// EXAMPLES ////
//////////////////

////////////////////
//// ROUTEGUIDE ////
////////////////////

lazy val `example-routeguide-protocol` = project
  .in(file("modules/examples/routeguide/protocol"))
  .dependsOn(monix)
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .settings(moduleName := "mu-rpc-example-routeguide-protocol")

lazy val `example-routeguide-runtime` = project
  .in(file("modules/examples/routeguide/runtime"))
  .settings(noPublishSettings)
  .settings(coverageEnabled := false)
  .settings(moduleName := "mu-rpc-example-routeguide-runtime")
  .settings(exampleRouteguideRuntimeSettings)

lazy val `example-routeguide-common` = project
  .in(file("modules/examples/routeguide/common"))
  .dependsOn(`example-routeguide-protocol`)
  .dependsOn(config)
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .settings(moduleName := "mu-rpc-example-routeguide-common")
  .settings(exampleRouteguideCommonSettings)

lazy val `example-routeguide-server` = project
  .in(file("modules/examples/routeguide/server"))
  .dependsOn(`example-routeguide-common`)
  .dependsOn(`example-routeguide-runtime`)
  .dependsOn(server)
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .settings(moduleName := "mu-rpc-example-routeguide-server")

lazy val `example-routeguide-client` = project
  .in(file("modules/examples/routeguide/client"))
  .dependsOn(`example-routeguide-common`)
  .dependsOn(`example-routeguide-runtime`)
  .dependsOn(`netty`)
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .settings(moduleName := "mu-rpc-example-routeguide-client")
  .settings(
    Compile / unmanagedSourceDirectories ++= Seq(
      baseDirectory.value / "src" / "main" / "scala-io",
      baseDirectory.value / "src" / "main" / "scala-task"
    )
  )
  .settings(addCommandAlias("runClientIO", "runMain example.routeguide.client.io.ClientAppIO"))
  .settings(
    addCommandAlias("runClientTask", "runMain example.routeguide.client.task.ClientAppTask"))

////////////////////
/////   SEED   /////
////////////////////

////////////////////////
//// Shared Modules ////
////////////////////////

lazy val `seed-config` = project
  .in(file("modules/examples/seed/shared/modules/config"))
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .settings(exampleSeedConfigSettings)

////////////////////////
////     Shared     ////
////////////////////////

lazy val allSharedModules: ProjectReference = `seed-config`

lazy val allSharedModulesDeps: ClasspathDependency =
  ClasspathDependency(allSharedModules, None)

lazy val `seed-shared` = project
  .in(file("modules/examples/seed/shared"))
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .aggregate(allSharedModules)
  .dependsOn(allSharedModulesDeps)

//////////////////////////
////  Server Modules  ////
//////////////////////////

lazy val `seed-server-common` = project
  .in(file("modules/examples/seed/server/modules/common"))
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)

lazy val `seed-server-protocol-avro` = project
  .in(file("modules/examples/seed/server/modules/protocol_avro"))
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .dependsOn(channel)

lazy val `seed-server-protocol-proto` = project
  .in(file("modules/examples/seed/server/modules/protocol_proto"))
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .dependsOn(fs2)

lazy val `seed-server-process` = project
  .in(file("modules/examples/seed/server/modules/process"))
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .settings(exampleSeedLogSettings)
  .dependsOn(`seed-server-common`, `seed-server-protocol-avro`, `seed-server-protocol-proto`)

lazy val `seed-server-app` = project
  .in(file("modules/examples/seed/server/modules/app"))
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .dependsOn(server, `seed-server-process`, `seed-config`)

//////////////////////////
////      Server      ////
//////////////////////////

lazy val allSeedServerModules: Seq[ProjectReference] = Seq(
  `seed-server-common`,
  `seed-server-protocol-avro`,
  `seed-server-protocol-proto`,
  `seed-server-process`,
  `seed-server-app`
)

lazy val allSeedServerModulesDeps: Seq[ClasspathDependency] =
  allSeedServerModules.map(ClasspathDependency(_, None))

lazy val `seed-server` = project
  .in(file("modules/examples/seed/server"))
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .aggregate(allSeedServerModules: _*)
  .dependsOn(allSeedServerModulesDeps: _*)
addCommandAlias("runAvroServer", "seed_server/runMain example.seed.server.app.AvroServerApp")
addCommandAlias("runProtoServer", "seed_server/runMain example.seed.server.app.ProtoServerApp")

//////////////////////////
////  Client Modules  ////
//////////////////////////

lazy val `seed-client-common` = project
  .in(file("modules/examples/seed/client/modules/common"))
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)

lazy val `seed-client-process` = project
  .in(file("modules/examples/seed/client/modules/process"))
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .settings(exampleSeedLogSettings)
  .dependsOn(
    netty,
    fs2,
    `seed-client-common`,
    `seed-server-protocol-avro`,
    `seed-server-protocol-proto`)

lazy val `seed-client-app` = project
  .in(file("modules/examples/seed/client/modules/app"))
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .settings(exampleSeedClientAppSettings)
  .dependsOn(`seed-client-process`, `seed-config`)

//////////////////////////
////      Client      ////
//////////////////////////

lazy val allSeedClientModules: Seq[ProjectReference] = Seq(
  `seed-client-common`,
  `seed-client-process`,
  `seed-client-app`
)

lazy val allSeedClientModulesDeps: Seq[ClasspathDependency] =
  allSeedClientModules.map(ClasspathDependency(_, None))

lazy val `seed-client` = project
  .in(file("modules/examples/seed/client"))
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .aggregate(allSeedClientModules: _*)
  .dependsOn(allSeedClientModulesDeps: _*)
addCommandAlias("runAvroClient", "seed_client/runMain example.seed.client.app.AvroClientApp")
addCommandAlias("runProtoClient", "seed_client/runMain example.seed.client.app.ProtoClientApp")

/////////////////////////
////       Root       ////
/////////////////////////

lazy val allSeedModules: Seq[ProjectReference] = Seq(
  `seed-shared`,
  `seed-client`,
  `seed-server`
)

lazy val allSeedModulesDeps: Seq[ClasspathDependency] =
  allSeedModules.map(ClasspathDependency(_, None))

lazy val seed = project
  .in(file("modules/examples/seed"))
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .settings(moduleName := "mu-rpc-example-seed")
  .aggregate(allSeedModules: _*)
  .dependsOn(allSeedModulesDeps: _*)

////////////////////
////  TODOLIST  ////
////////////////////

lazy val `example-todolist-protocol` = project
  .in(file("modules/examples/todolist/protocol"))
  .dependsOn(channel)
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .settings(moduleName := "mu-rpc-example-todolist-protocol")

lazy val `example-todolist-runtime` = project
  .in(file("modules/examples/todolist/runtime"))
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .settings(moduleName := "mu-rpc-example-todolist-runtime")

lazy val `example-todolist-server` = project
  .in(file("modules/examples/todolist/server"))
  .dependsOn(`example-todolist-protocol`)
  .dependsOn(`example-todolist-runtime`)
  .dependsOn(server)
  .dependsOn(config)
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .settings(moduleName := "mu-rpc-example-todolist-server")
  .settings(exampleTodolistCommonSettings)

lazy val `example-todolist-client` = project
  .in(file("modules/examples/todolist/client"))
  .dependsOn(`example-todolist-protocol`)
  .dependsOn(`example-todolist-runtime`)
  .dependsOn(`netty`)
  .dependsOn(config)
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .settings(moduleName := "mu-rpc-example-todolist-client")
  .settings(exampleTodolistCommonSettings)

/////////////////
//// CODEGEN ////
/////////////////

lazy val `example-codegen-protocol` = project
  .in(file("modules/examples/codegen/protocol"))
  .dependsOn(`channel`, `common` % Configurations.CompilerPlugin)
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .settings(moduleName := "mu-rpc-example-codegen-protocol")
  .settings(
    scalacOptions ++= Seq(
      // recompile whenever plugin changes (in IDEA this might require using sbt shell for imports & builds)
      "-Jdummy" + (`common`/Compile/packageBin).value.lastModified,
      "-P:service-generator:outputDir=" + (Compile/sourceManaged).value
    ),
    autoCompilerPlugins := true,
  )

lazy val `example-codegen-app` = project
  .in(file("modules/examples/codegen/app"))
  .dependsOn(`example-codegen-protocol`)
  .settings(coverageEnabled := false)
  .settings(noPublishSettings)
  .settings(moduleName := "mu-rpc-example-codegen-app")
  .settings(
    Compile/sourceGenerators += Def.task(((`example-codegen-protocol`/Compile/sourceManaged).value ** "*.scala").get)
  )

/////////////////////
//// MARSHALLERS ////
/////////////////////

lazy val `marshallers-jodatime` = project
  .in(file("modules/marshallers/jodatime"))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(channel % "compile->compile;test->test")
  .dependsOn(`internal-core` % "compile->compile;test->test")
  .dependsOn(testing % "test->test")
  .settings(moduleName := "mu-rpc-marshallers-jodatime")
  .settings(marshallersJodatimeSettings)

///////////////////////////
//// DECIMAL MIGRATION ////
///////////////////////////

lazy val `legacy-avro-decimal-compat-protocol` = project
  .in(file("modules/legacy-avro-decimal/procotol"))
  .settings(moduleName := "legacy-avro-decimal-compat-protocol")
  .disablePlugins(scoverage.ScoverageSbtPlugin)

lazy val `legacy-avro-decimal-compat-model` = project
  .in(file("modules/legacy-avro-decimal/model"))
  .settings(moduleName := "legacy-avro-decimal-compat-model")

lazy val `legacy-avro-decimal-compat-encoders` = project
  .in(file("modules/legacy-avro-decimal/encoders"))
  .settings(moduleName := "legacy-avro-decimal-compat-encoders")
  .dependsOn(`legacy-avro-decimal-compat-model` % "provided")
  .dependsOn(`internal-core`)

//////////////////////////
//// MODULES REGISTRY ////
//////////////////////////

lazy val allModules: Seq[ProjectReference] = Seq(
  common,
  `internal-core`,
  `internal-monix`,
  `internal-fs2`,
  channel,
  monix,
  fs2,
  `client-cache`,
  netty,
  okhttp,
  server,
  config,
  dropwizard,
  prometheus,
  testing,
  ssl,
  `srcgen-core`,
  http,
  kafka,
  `marshallers-jodatime`,
  `example-routeguide-protocol`,
  `example-routeguide-common`,
  `example-routeguide-runtime`,
  `example-routeguide-server`,
  `example-routeguide-client`,
  `example-todolist-protocol`,
  `example-todolist-runtime`,
  `example-todolist-server`,
  `example-todolist-client`,
  `example-codegen-protocol`,
  `example-codegen-app`,
  seed,
  `benchmarks-vprev`,
  `benchmarks-vnext`,
  `legacy-avro-decimal-compat-model`,
  `legacy-avro-decimal-compat-protocol`,
  `legacy-avro-decimal-compat-encoders`,
  `health-check-unary`,
  `health-client`,
  `health-server-monix`,
  `health-server-fs2`
)

lazy val allModulesDeps: Seq[ClasspathDependency] =
  allModules.map(ClasspathDependency(_, None))

lazy val root = project
  .in(file("."))
  .settings(name := "mu-scala")
  .settings(noPublishSettings)
  .aggregate(allModules: _*)
  .dependsOn(allModulesDeps: _*)

lazy val docs = project
  .in(file("docs"))
  .dependsOn(allModulesDeps.filterNot(_.project == LocalProject("benchmarks-vprev")): _*)
  .settings(name := "mu-docs")
  .settings(docsSettings: _*)
  .settings(micrositeSettings: _*)
  .settings(noPublishSettings: _*)
  .enablePlugins(MicrositesPlugin)
  .enablePlugins(MdocPlugin)
