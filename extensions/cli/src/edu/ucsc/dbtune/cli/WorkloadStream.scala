package edu.ucsc.dbtune.cli

import scala.actors.Actor
import scala.actors.Actor._

import scala.collection.mutable.Set

import java.lang.Integer
import java.sql.Connection
import java.sql.SQLException
import java.util.Iterator

import edu.ucsc.dbtune.DatabaseSystem
import edu.ucsc.dbtune.advisor.Advisor
import edu.ucsc.dbtune.viz.AdvisorVisualizer
import edu.ucsc.dbtune.workload.FileWorkloadReader
import edu.ucsc.dbtune.workload.QueryLogReader
import edu.ucsc.dbtune.workload.SQLStatement
import edu.ucsc.dbtune.workload.WorkloadReader

/** Represents a stream of SQL statements that are executed in a DBMS. Through the use of the 
 * Registry pattern, the workload keeps a list of advisors and visualizers to whom it has to 
 * notificate about a new statement.
 *
 * @param db
 *    the database on which WFIT will be executed
 * @param wl
 *    workload that WFIT will be listening to
 * @param initialSet
 *    an (optional) initial candidate set
 */
class WorkloadStream(fileName: String, dbms: Option[Database]) {
  val workloadReader =
    dbms match {
      case None => new FileWorkloadReader(fileName, false)
      case Some(db) => QueryLogReader.newQueryLogReader(db.connection)
    }

  def this(fileName: String) {
    this(fileName, None)
  }

  def this(dbms: Database) {
    this("", Some(dbms))
  }

  def workloadName = { workloadReader.getWorkload.getWorkloadName }
}
