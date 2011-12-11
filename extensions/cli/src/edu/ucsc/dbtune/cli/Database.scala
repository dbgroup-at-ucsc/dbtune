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
package edu.ucsc.dbtune.cli

import edu.ucsc.dbtune.DatabaseSystem
import edu.ucsc.dbtune.metadata.Configuration;
import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.util.Environment

import java.util.Properties

import edu.ucsc.dbtune.DatabaseSystem._
import edu.ucsc.dbtune.util.EnvironmentProperties.DBMS
import edu.ucsc.dbtune.util.EnvironmentProperties.JDBC_URL
import edu.ucsc.dbtune.util.EnvironmentProperties.USERNAME
import edu.ucsc.dbtune.util.EnvironmentProperties.OPTIMIZER
import edu.ucsc.dbtune.util.EnvironmentProperties.PASSWORD

/** This class provides a hub for most of the operations that a user can execute through the CLI 
  */
class Database(dbms:DatabaseSystem) extends Catalog(dbms.getCatalog) {

  /** Creates a configuration containing the given set of indexes. Index names are expected to be 
   * fully qualified, otherwise an exception will be thrown. The indexes are created as {@link 
   * Index.SECONDARY}, {@link Index.UNCLUSTERED} and {@link NON_UNIQUE}.
    *
    * @param indexIds
    *   sql statement
    * @return
    *   a configuration
    * @throw SQLException
    *   if one of the indexes isn't found in the system's catalog
    * @see edu.ucsc.dbtune.metadata
  def configuration(indexNames:String*) : Configuration =  {
    var conf = new Configuration("")

    for (indexName <- indexNames) {
      conf.add(dbms.getCatalog.findIndex(indexName))
    }
  }
    */
  
  /** Recommends indexes for the given SQL statement
    *
    * @param sql
    *   sql statement
    * @return
    *   a configuration
    */
  def recommend(sql:String) : Configuration =  {
    dbms.getOptimizer.recommendIndexes(sql)
  }
  
  /** Explains a SQL statement
    *
    * @param sql
    *   sql statement
    * @return
    *   a configuration
    */
  def explain(sql:String) : ExplainedSQLStatement =  {
    dbms.getOptimizer.explain(sql)
  }
  
  /** Explains a SQL statement
    *
    * @param sql
    *   sql statement
    * @param conf
    *   configuration to be used
    * @return
    *   a configuration
    */
  def explain(sql:String, conf:Configuration) : ExplainedSQLStatement =  {
    dbms.getOptimizer.explain(sql, conf)
  }
  
  /** Closes the connection to the DBMS
    */
  def close() =  {
    dbms.getConnection.close
  }
}

object Database
{
  /** Singleton */
  //var INSTANCE = None

  /** Creates to  Database containing the metadata information about a DB.
    *
    * @param url
    *   JDBC url
    * @param usr
    *   username used to authenticate
    * @param pwd
    *   password used to authenticate
    * @return
    *   a databse instance
    */
  def connect(url:String, usr:String, pwd:String) : Database = {

    var properties = new Properties()

    properties.setProperty(USERNAME,  usr)
    properties.setProperty(PASSWORD,  pwd)
    properties.setProperty(JDBC_URL,  url)
    properties.setProperty(OPTIMIZER, DBMS)

    new Database(newDatabaseSystem(properties))

    //Database.INSTANCE = new Database(newDatabaseSystem(properties))
  }

  /** Recommends indexes for the given SQL statement
    *
    * @param sql
    *   sql statement
    * @return
    *   a configuration
    */
  def recommend(sql:String) /*: Configuration*/ =  {
    //INSTANCE.recommend(sql)
  }
  
  /** Explains a SQL statement
    *
    * @param sql
    *   sql statement
    * @return
    *   a configuration
    */
  def explain(sql:String) /*: PreparedSQLStatement*/ =  {
    //INSTANCE.explain(sql)
  }
  
  /** Explains a SQL statement
    *
    * @param sql
    *   sql statement
    * @param conf
    *   configuration to be used
    * @return
    *   a configuration
    */
  def explain(sql:String, conf:Configuration) /*: PreparedSQLStatement */ =  {
    //INSTANCE.getOptimizer.explain(sql, conf)
  }
  
  /** Closes the connection to the DBMS
    */
  def close() =  {
    //INSTANCE.getConnection.close
    //INSTANCE = None
  }
}
