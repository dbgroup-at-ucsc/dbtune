package edu.ucsc.dbtune.cli

import scala.actors.Actor
import scala.actors.Actor._

import java.lang.Integer
import java.sql.SQLException
import java.util.Iterator
import java.util.Set

import edu.ucsc.dbtune.DatabaseSystem
import edu.ucsc.dbtune.metadata.Index
import edu.ucsc.dbtune.workload.SQLStatement
import edu.ucsc.dbtune.workload.Workload

class WFIT(db: Database) {
    var wfit = new edu.ucsc.dbtune.advisor.wfit.WFIT(db.DBMS)
    var wfitActor = new WFITActor(wfit, null)

  def processWorkload(workloadFile: String) = {
    wfitActor ! Stop
    wfit = new edu.ucsc.dbtune.advisor.wfit.WFIT(db.DBMS)
    wfitActor = new WFITActor(wfit, (new Workload(workloadFile)).iterator())
    wfitActor.start
  }

  def processWorkload(workloadFile: String, initialSet: Set[Index]) = {
    wfitActor ! Stop
    wfit = new edu.ucsc.dbtune.advisor.wfit.WFIT(db.DBMS, initialSet)
    wfitActor = new WFITActor(wfit, (new Workload(workloadFile)).iterator())
    wfitActor.start
  }

  def tick(steps: Integer) = {
    wfitActor ! Process(steps)
  }

  def tick = {
    wfitActor ! Process(1)
  }

  def recommendation = {
    wfit.getRecommendation
  }

  def stats = {
    wfit.getRecommendationStatistics
  }

  def stablePartitioning = {
    wfit.getStablePartitioning
  }

  def voteDown(id: java.lang.Integer) = {
    wfit.voteDown(id)
  }

  def voteUp(id: java.lang.Integer) = {
    wfit.voteUp(id)
  }

  def optimalStats = {
    wfit.getOptimalRecommendationStatistics
  }
}

case class Stop()
case class Process(steps: Integer)

class WFITActor(
    wfit: edu.ucsc.dbtune.advisor.wfit.WFIT,
    workload: Iterator[SQLStatement]) extends Actor {
  def act = {
    loop {
      react {
        case Process(steps) => process(steps)
        case Stop => exit
      }
    }
  }

  def process(steps: Integer) = {
    if (workload == null) throw new SQLException("WFIT not assigned with a workload yet")
    for (i <- 1 to steps) {
      if (workload.hasNext()) {
        wfit.process(workload.next())
      } else {
        this ! Stop
        throw new SQLException("Reached the end of the workload")
      }
    }
  }
}
