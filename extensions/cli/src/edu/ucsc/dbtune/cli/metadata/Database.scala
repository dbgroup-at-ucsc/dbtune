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

import edu.ucsc.dbtune.DatabaseSystem._;
import java.util.Properties

/**
 * This class provides a hub for most of the operations that a user can execute through the CLI 
 */
class Database(s:CoreSchema) extends CoreSchema(s) {
  var tables:List[Table]              = Table.asScalaTable(s.getTables())
  var baseConfiguration:Configuration = new Configuration(s.getBaseConfiguration())
}

/**
 * Static methods for the Database class (a.k.a. Database's object companion)
 */
object Database
{
  /**
   * Creates a new Database containing the metadata information about a DB.
   *
   * @param url
   *    url containig the information about the host and database to connect to
   * @param usr
   *    username used to authenticate
   * @param pwd
   *    password used to authenticate
   */
  def connect(url:String, usr:String, pwd:String) : Database = {
    var cat = newDatabaseSystem.getCatalog
    var db  = new Database(cat.getSchemas.get(0))

    return db
  }
}
