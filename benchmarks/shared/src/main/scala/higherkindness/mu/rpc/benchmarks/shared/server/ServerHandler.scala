/*
 * Copyright 2017-2020 47 Degrees, LLC. <http://www.47deg.com>
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

package higherkindness.mu.rpc.benchmarks
package shared
package server

import higherkindness.mu.rpc.benchmarks.shared.models._
import higherkindness.mu.rpc.benchmarks.shared.protocols._
import higherkindness.mu.rpc.protocol.Empty

abstract class HandlerImpl[F[_]](implicit persistenceService: PersistenceService[F]) {

  def listPersons(b: Empty.type): F[PersonList] = {
    val _ = b // makes compiler happy: parameter value b in method listPersons is never used
    persistenceService.listPersons
  }

  def getPerson(id: PersonId): F[Person] =
    persistenceService.getPerson(id)

  def getPersonLinks(id: PersonId): F[PersonLinkList] =
    persistenceService.getPersonLinks(id)

  def createPerson(person: Person): F[Person] =
    persistenceService.createPerson(person)
}

class ProtoHandler[F[_]](implicit PS: PersistenceService[F])
    extends HandlerImpl[F]
    with PersonServicePB[F]

class AvroHandler[F[_]](implicit PS: PersistenceService[F])
    extends HandlerImpl[F]
    with PersonServiceAvro[F]

class AvroWithSchemaHandler[F[_]](implicit PS: PersistenceService[F])
    extends HandlerImpl[F]
    with PersonServiceAvroWithSchema[F]
