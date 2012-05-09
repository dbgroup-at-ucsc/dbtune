package edu.ucsc.dbtune.cli

import scala.actors.Actor
import scala.actors.Actor._

import edu.ucsc.dbtune.DatabaseSystem
import edu.ucsc.dbtune.workload.Workload

class WFIT(db: Database) extends edu.ucsc.dbtune.advisor.wfit.WFIT(db.DBMS) {

  def process(workloadFile: String) = {
    val wfitActor = new WFITActor(this, workloadFile)
    wfitActor.start
    wfitActor ! Process
  }
}

case class Stop()
case class Process()

class WFITActor(wfit: edu.ucsc.dbtune.advisor.wfit.WFIT, workloadFile: String) extends Actor {
  def act = {
    loop {
      react {
        case Process => process; exit
        case Stop => exit
      }
    }
  }

  def process = {
    wfit.process(new Workload(workloadFile))
  }
}
