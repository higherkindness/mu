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

import higherkindness.mu.rpc.protocol._
import _root_.fs2.Stream
// import monix.reactive.Observable

// This is not a normal "test" with assertions.
// It's just a check that valid code compiles as expected
// after the @service macro has been expanded.
object MacroAnnotationCompilationTest {

  final case class MyReq()
  final case class MyResp(a: Int)

  @service(Protobuf)
  trait MyFS2Service[F[_]] {

    def trivial(req: Empty.type): F[Empty.type]

    def unary(req: MyReq): F[MyResp]

    def clientStreaming(req: Stream[F, MyReq]): F[MyResp]

    def clientStreamingFullyQualifiedStream(req: _root_.fs2.Stream[F, MyReq]): F[MyResp]

    def serverStreaming(req: MyReq): F[Stream[F, MyResp]]

    def bidirectionalStreaming(req: Stream[F, MyReq]): F[Stream[F, MyResp]]

  }

  // @service(Protobuf)
  // trait MyMonixService[F[_]] {

  //   def trivial(req: Empty.type): F[Empty.type]

  //   def unary(req: MyReq): F[MyResp]

  //   def clientStreaming(req: Observable[MyReq]): F[MyResp]

  //   def clientStreamingFullyQualifiedStream(req: Observable[MyReq]): F[MyResp]

  //   def serverStreaming(req: MyReq): F[Observable[MyResp]]

  //   def bidirectionalStreaming(req: Observable[MyReq]): F[Observable[MyResp]]

  // }

}
