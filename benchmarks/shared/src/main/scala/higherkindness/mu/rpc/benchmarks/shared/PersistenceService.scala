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

import cats.effect.Sync
import cats.syntax.applicative._
import higherkindness.mu.rpc.benchmarks.shared.models._

class PersistenceService[F[_]: Sync] {

  def listPersons: F[PersonList] = {
    val search = database.persons
    PersonList(search, search.size).pure[F]
  }

  def getPerson(pId: PersonId): F[Person] =
    Sync[F].delay(
      database.persons
        .find(_.id == pId.id)
        .getOrElse(throw DatabaseException(s"User $pId not found"))
    )

  def getPersonLinks(pId: PersonId): F[PersonLinkList] = {
    val search = database.personLinks.filter(link => link.p1.id == pId.id || link.p2.id == pId.id)
    PersonLinkList(search, search.size).pure[F]
  }

  def createPerson(person: Person): F[Person] = person.pure[F]
}

object PersistenceService {
  def apply[F[_]: Sync] = new PersistenceService[F]
}
