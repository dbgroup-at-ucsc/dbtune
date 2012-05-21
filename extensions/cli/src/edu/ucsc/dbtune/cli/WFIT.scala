package edu.ucsc.dbtune.cli

import scala.actors.Actor
import scala.actors.Actor._

import java.lang.Integer

import java.sql.SQLException

import java.util.HashSet
import java.util.Iterator
import java.util.Set

import edu.ucsc.dbtune.DatabaseSystem

import edu.ucsc.dbtune.metadata.Index

import edu.ucsc.dbtune.util.Environment
import edu.ucsc.dbtune.util.Environment._

import edu.ucsc.dbtune.workload.SQLStatement
import edu.ucsc.dbtune.workload.Workload

/**
 * The CLI interface to WFIT. Registers WFIT advisor to the given stream.
 *
 * @param db
 *    the database on which WFIT will be executed
 * @param wl
 *    workload that WFIT will be listening to
 * @param initialSet
 *    an (optional) initial candidate set
 * @author Ivo Jimenez
 */
class WFIT(
    db: Database,
    wl: WorkloadStream,
    initialSet: Set[Index] = new HashSet[Index],
    stateCnt: Integer = getInstance.getMaxNumStates,
    idxCnt: Integer = getInstance.getMaxNumIndexes,
    histSize: Integer = getInstance.getIndexStatisticsWindow,
    partitionIters: Integer = getInstance.getNumPartitionIterations)
  extends edu.ucsc.dbtune.advisor.wfit.WFIT(
      db.DBMS, initialSet, stateCnt, idxCnt, histSize, partitionIters)
{
  val workload = wl
  wl.register(this)

  def this(db: Database, workload: WorkloadStream) = {
    this(db, workload, new HashSet[Index], getInstance.getMaxNumStates)

    if (stateCnt == getInstance.getMaxNumStates)
      this.getRecommendationStatistics.setAlgorithmName("WFIT")
  }

  def this(db: Database, workload: WorkloadStream, initialSet: Set[Index]) = {
    this(db, workload, initialSet, getInstance.getMaxNumStates)

    this.getRecommendationStatistics.setAlgorithmName("WFIT")
  }

  def this(db: Database, workload: WorkloadStream, stateCnt: Integer) =
    this(db, workload, new HashSet[Index], stateCnt)

  def this(db: Database, workload: WorkloadStream, name: String) = {
    this(db, workload)

    this.getRecommendationStatistics.setAlgorithmName(name)
  }
}
