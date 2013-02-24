package edu.ucsc.dbtune.cli

import java.sql.Connection

import edu.ucsc.dbtune.DatabaseSystem
import edu.ucsc.dbtune.metadata.Catalog
import edu.ucsc.dbtune.metadata.ColumnOrdering
import edu.ucsc.dbtune.metadata.Index
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement
import edu.ucsc.dbtune.optimizer.Optimizer
import edu.ucsc.dbtune.util.Environment
import edu.ucsc.dbtune.util.MetadataUtils

import com.google.common.collect.Sets

import java.util.HashSet
import java.util.Properties
import java.util.Set

import edu.ucsc.dbtune.DatabaseSystem._
import edu.ucsc.dbtune.util.Environment.getInstance
import edu.ucsc.dbtune.util.EnvironmentProperties.IBG
import edu.ucsc.dbtune.util.EnvironmentProperties.INUM
import edu.ucsc.dbtune.util.EnvironmentProperties.JDBC_URL
import edu.ucsc.dbtune.util.EnvironmentProperties.USERNAME
import edu.ucsc.dbtune.util.EnvironmentProperties.OPTIMIZER
import edu.ucsc.dbtune.util.EnvironmentProperties.PASSWORD

/** The Scala-fied interface to a DBMS.
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

  /** Returns the underlying JDBC connection
    *
    * @return
    *   the JDBC connection
    */
  def connection() : Connection =  {
    dbms.getConnection
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

  /** creates an index.
    *
    * @param orderingSpec
    *   specification of columns and their ordering
    * @return
    *   new index
    */
  def newIndex(orderingSpec: String) : Index = {
    DBMS.newIndex(ColumnOrdering.newOrdering(dbms.getCatalog, orderingSpec))
  }

  /** creates a set of indexes by reading their definition from a file.
    *
    * @param file
    *   absolute or relative path to a file containing the definition of a set of indexes to load
    * @return
    *   a set of indexes that are read from a file
    */
  def loadIndexes(fileName: String) : Set[Index] = {
    MetadataUtils.loadIndexes(DBMS, fileName)
  }
}

object Database
{
  /** connects to a database.
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

    var env = Environment.getInstance

    env.setProperty(USERNAME,  usr)
    env.setProperty(PASSWORD,  pwd)
    env.setProperty(JDBC_URL,  url)
    //env.setProperty(OPTIMIZER, INUM + "," + IBG)
    env.setProperty(OPTIMIZER, IBG)

    System.out.println("Extracting metadata...")

    new Database(newDatabaseSystem(env))
  }
}
