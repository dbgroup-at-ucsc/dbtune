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
    idxCnt: Integer = Environment.getInstance.getMaxNumIndexes(),
    stateCnt: Integer = Environment.getInstance.getMaxNumStates(),
    histSize: Integer = Environment.getInstance.getIndexStatisticsWindow(),
    partitionIters: Integer = Environment.getInstance.getNumPartitionIterations())
  extends edu.ucsc.dbtune.advisor.wfit.WFIT(
      db.DBMS, initialSet, idxCnt, stateCnt, histSize, partitionIters)
{
  wl.register(this)
}
