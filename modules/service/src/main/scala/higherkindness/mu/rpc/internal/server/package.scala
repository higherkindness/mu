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

package higherkindness.mu.rpc.internal

import cats.effect.kernel.Sync
import io.grpc.Metadata
import io.grpc.Metadata.{ASCII_STRING_MARSHALLER, BINARY_HEADER_SUFFIX, Key}
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import higherkindness.mu.rpc.protocol._
import natchez.Kernel
import scala.jdk.CollectionConverters._

package object server {

  private[internal] def addCompression[F[_]: Sync, A](
      observer: StreamObserver[A],
      compressionType: CompressionType
  ): F[Unit] =
    (observer, compressionType) match {
      case (o: ServerCallStreamObserver[_], Gzip) => Sync[F].delay(o.setCompression("gzip"))
      case _                                      => Sync[F].unit
    }

  private[internal] def extractTracingKernel(headers: Metadata): Kernel = {
    val asciiHeaders = headers
      .keys()
      .asScala
      .collect {
        case k if !k.endsWith(BINARY_HEADER_SUFFIX) =>
          k -> headers.get(Key.of(k, ASCII_STRING_MARSHALLER))
      }
      .toMap
    Kernel(asciiHeaders)
  }

}
