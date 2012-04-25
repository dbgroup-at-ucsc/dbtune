package edu.ucsc.dbtune.cli

import edu.ucsc.dbtune.DatabaseSystem
import edu.ucsc.dbtune.metadata.Catalog
import edu.ucsc.dbtune.metadata.Index
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement
import edu.ucsc.dbtune.optimizer.Optimizer
import edu.ucsc.dbtune.util.Environment

import java.util.HashSet
import java.util.Properties
import java.util.Set

import edu.ucsc.dbtune.DatabaseSystem._
import edu.ucsc.dbtune.util.EnvironmentProperties.IBG
import edu.ucsc.dbtune.util.EnvironmentProperties.JDBC_URL
import edu.ucsc.dbtune.util.EnvironmentProperties.USERNAME
import edu.ucsc.dbtune.util.EnvironmentProperties.OPTIMIZER
import edu.ucsc.dbtune.util.EnvironmentProperties.PASSWORD

/** This class provides a hub for most of the operations that a user can execute through the CLI 
  */
class Database(dbms: DatabaseSystem) extends Catalog(dbms.getCatalog) {
  val DBMS = dbms

  /** Recommends indexes for the given SQL statement
    *
    * @param sql
    *   sql statement
    * @return
    *   a configuration
    */
  def recommend(sql:String) : Set[Index] =  {
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
  def explain(sql:String, conf:Set[Index]) : ExplainedSQLStatement =  {
    dbms.getOptimizer.explain(sql, conf)
  }
  
  /** Closes the connection to the DBMS
    */
  def close() =  {
    dbms.getConnection.close
  }

  /** Explains a SQL statement
    *
    * @param sql
    *   sql statement
    * @return
    *   a configuration
    */
  def optimizer() : Optimizer =  {
    dbms.getOptimizer
  }
  
}

object Database
{  
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
    properties.setProperty(OPTIMIZER, IBG)

    new Database(newDatabaseSystem(properties))
  }
}
