package edu.ucsc.dbtune.cli

import scala.actors.Actor
import scala.actors.Actor._

import scala.collection.mutable.Set

import java.lang.Integer
import java.sql.SQLException
import java.util.Iterator

import edu.ucsc.dbtune.DatabaseSystem
import edu.ucsc.dbtune.advisor.Advisor
import edu.ucsc.dbtune.viz.AdvisorVisualizer
import edu.ucsc.dbtune.workload.SQLStatement
import edu.ucsc.dbtune.workload.Workload

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
class WorkloadStream(fileName: String) {
  val workload = new Workload(fileName)
  val workloadActor = new WorkloadStreamActor(workload.iterator)
  var advisors = Set[Advisor]()
  var visualizers = Set[AdvisorVisualizer]()

  workloadActor.start

  /** Registers the given advisor.
   *
   * @param advisor
   *    new advisor to be added to the registry
   */
  def register(advisor: Advisor) = advisors += advisor

  /** Registers the given visualizer.
   *
   * @param visualizer
   *    new visualizer to be added to the registry
   */
  def register(visualizer: AdvisorVisualizer) = {
    visualizers += visualizer; visualizer.setWorkload(workload)
  }

  /** Executes the next statement in the stream
   */
  def next = workloadActor ! Next(advisors, visualizers, 1)

  /** Executes a given number of statements.
   *
   * @param steps
   *    number of statements to be fetched from the stream and executed
   */
  def next(steps: Integer) = workloadActor ! Next(advisors, visualizers, steps)

  /** Executes the whole workload, i.e. until there are no more statements in the stream.
   */
  def play = workloadActor ! Play(advisors, visualizers)

  /** Checks if the given advisor is a member of the internal registry.
   *
   * @param advisor
   *    advisor being checked for membership in the registry
   */
  def isRegistered(advisor: Advisor) = advisors.contains(advisor)

  /** Checks if the given visualizer is a member of the internal registry.
   *
   * @param visualizer
   *    visualizer being checked for membership in the registry
   */
  def isRegistered(viz: AdvisorVisualizer) = visualizers.contains(viz)

  /** Checks if the given visualizer is a member of the internal registry.
   *
   * @param visualizer
   *    visualizer being checked for membership in the registry
   */
  def currentStatement() : SQLStatement = workloadActor.currentStmt
}

/** An actor to concurrently handle the processing and passing of statements contained in a 
 * workload.
 *
 * @param iterator
 *    a statement iterator
 */
class WorkloadStreamActor(iterator: Iterator[SQLStatement]) extends Actor {
  var currentStmt = new SQLStatement("select from empty")

  def act = {
    loop {
      react {
        case Next(advisors, visualizers, steps) => next(advisors, visualizers, steps)
        case Play(advisors, visualizers) => play(advisors, visualizers)
        case Stop => exit
      }
    }
  }

  /** Executes the whole workload, i.e. until there are no more statements in the stream, 
   * communicating each statement to every advisor and visualizer contained in the given sets
   */
  def play(advisors: Set[Advisor], visualizers: Set[AdvisorVisualizer]) = {
    while (iterator.hasNext()) {
      currentStmt = iterator.next
      advisors.foreach(_.process(currentStmt))
      visualizers.foreach(_.refresh)
    }

    currentStmt = null
    this ! Stop
  }

  /** Executes a specific number of statements from the workload, i.e. until there are no more 
   * statements in the stream, communicating each statement to every advisor and visualizer 
   * contained in the given sets. If the specified number of steps is greater than the remaining 
   * number of statements, the steps are just ignored.
   */
  def next(advisors: Set[Advisor], visualizers: Set[AdvisorVisualizer], steps: Integer) = {
    for (i <- 1 to steps) {
      if (iterator.hasNext()) {
        currentStmt = iterator.next
        advisors.foreach(_.process(currentStmt))
        visualizers.foreach(_.refresh)
      } else {
        currentStmt = null
        this ! Stop
      }
    }
  }

  def notify(visualizers: Set[AdvisorVisualizer]) = visualizers.foreach(_.refresh)
}

case class Stop()
case class Next(advisors: Set[Advisor], visualizers: Set[AdvisorVisualizer], steps: Integer)
case class Play(advisors: Set[Advisor], visualizers: Set[AdvisorVisualizer])
