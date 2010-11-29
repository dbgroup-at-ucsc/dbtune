package edu.ucsc.dbtune.core

import edu.ucsc.dbtune.core.JdbcDatabaseConnectionManager._

import java.util.Properties

object SSchema
{
  def connect(url:String, usr:String, pwd:String):DatabaseConnection[_] = {

    val props = new Properties;

    props.setProperty(JdbcDatabaseConnectionManager.URL, url);
    props.setProperty(JdbcDatabaseConnectionManager.USERNAME, usr);
    props.setProperty(JdbcDatabaseConnectionManager.PASSWORD, pwd);

    return makeDatabaseConnectionManager(props).connect();
  }
}
