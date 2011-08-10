/*
 ******************************************************************************
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
 ******************************************************************************/
package edu.ucsc.dbtune.cli.metadata

import edu.ucsc.dbtune.connectivity.DatabaseConnection
import edu.ucsc.dbtune.metadata.extraction.MetaDataExtractor
import edu.ucsc.dbtune.metadata.extraction.GenericJDBCExtractor
import edu.ucsc.dbtune.metadata.extraction.PGExtractor

import edu.ucsc.dbtune.connectivity.JdbcConnectionManager._

import java.util.Properties

/**
 * This class provides a hub for most of the operations that a user can execute through the CLI 
 */
class Database(s:CoreSchema) extends CoreSchema(s) {
  var connection:DatabaseConnection   = null
  var tables:List[Table]              = Table.asScalaTable(s.getTables())
  var baseConfiguration:Configuration = new Configuration(s.getBaseConfiguration())
}

/**
 * Static methods for the Database class (a.k.a. Database's object companion)
 */
object Database
{
  /**
   * Returns the appropriate driver class name based on the contents of the connection URL
   *
   * @param url
   *    String containing the URL to which a DB connection is open against
   */
  def getDriver(url:String):String = {
    url match {
      case x if x contains "postgres" => return "org.postgresql.Driver"
      case x if x contains "db2" => return "com.ibm.db2.jcc.DB2Driver"
      case _ => throw new IllegalArgumentException("unsupported url")
    }
  }

  /**
   * Returns the appropriate metadata extractor based on the contents of the connection URL
   *
   * @param url
   *    String containing the URL to which a DB connection is open against
   */
  private def getExtractor(url:String):MetaDataExtractor = {
    url match {
      case x if x contains "postgres" => return new PGExtractor()
      case x if x contains "db2" => return new GenericJDBCExtractor()
      case _ => throw new IllegalArgumentException("unsupported url")
    }
  }

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
    val props  = new Properties
    val driver = Database.getDriver(url)

    props.setProperty(URL,      url)
    props.setProperty(USERNAME, usr)
    props.setProperty(PASSWORD, pwd)
    props.setProperty(DRIVER,   driver)
    props.setProperty(DATABASE, "")

    var con = makeDatabaseConnectionManager(props).connect
    var db  = new Database(Database.getExtractor(url).extract(con).getSchemas.get(0))

    return db
  }
}
