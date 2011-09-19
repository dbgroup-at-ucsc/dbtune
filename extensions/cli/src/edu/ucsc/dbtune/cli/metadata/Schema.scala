/* ************************************************************************** *
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ************************************************************************** */
package edu.ucsc.dbtune.cli.metadata

import scala.collection.mutable.Buffer
import scala.collection.JavaConversions._

/** This class provides a mirror for the core.metadata.Schema class
 */
class Schema(s:CoreSchema) extends CoreSchema(s) {
  var tables:List[Table] = Table.asScalaTable(s.getTables)
  var indexes:List[Index] = Index.asScalaIndex(s.getIndexes)
}

object Schema {
  /** Creates a list of schema objects
    *
    * @param javaSchemaList
    *    url containig the information about the host and database to connect to
    */
  def asScalaSchema( coreSchemas:java.util.List[CoreSchema] ) : List[Schema] =   {
    var cliSchemas = List[Schema]()

    for (x <- asScalaBuffer(coreSchemas)) {
      cliSchemas ::= new Schema(x)
    }

    return cliSchemas
  }
}
