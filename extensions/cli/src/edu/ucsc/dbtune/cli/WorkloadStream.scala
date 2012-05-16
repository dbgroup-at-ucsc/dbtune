package edu.ucsc.dbtune.cli

import scala.actors.Actor
import scala.actors.Actor._

import scala.collection.mutable.Set

import java.lang.Integer
import java.sql.SQLException
import java.util.Iterator

import edu.ucsc.dbtune.DatabaseSystem
import edu.ucsc.dbtune.advisor.Advisor
import edu.ucsc.dbtune.viz.Visualizer
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
  val workloadActor = new WorkloadStreamActor((new Workload(fileName)).iterator)
  var advisors = Set[Advisor]()
  var visualizers = Set[Visualizer]()

  workloadActor.start

  /** Registers the given advisor.
   *
   * @param advisor
   *    new advisor to be added to the registry
   */
  def register(advisor: Advisor) = { advisors += advisor }

  /** Registers the given visualizer.
   *
   * @param visualizer
   *    new visualizer to be added to the registry
   */
  def register(visualizer: Visualizer) = { visualizers += visualizer }

  /** Executes the next statement in the stream
   */
  def next = { workloadActor ! Next(advisors, visualizers, 1) }

  /** Executes a given number of statements.
   *
   * @param steps
   *    number of statements to be fetched from the stream and executed
   */
  def next(steps: Integer) = { workloadActor ! Next(advisors, visualizers, steps) }

  /** Executes the whole workload, i.e. until there are no more statements in the stream.
   */
  def play = { workloadActor ! Play(advisors, visualizers) }

  /** Checks if the given advisor is a member of the internal registry.
   *
   * @param advisor
   *    advisor being checked for membership in the registry
   */
  def isRegistered(advisor: Advisor) = { advisors.contains(advisor) }

  /** Checks if the given visualizer is a member of the internal registry.
   *
   * @param visualizer
   *    visualizer being checked for membership in the registry
   */
  def isRegistered(viz: Visualizer) = { visualizers.contains(viz) }
}

/** An actor to concurrently handle the processing and passing of statements contained in a 
 * workload.
 *
 * @param iterator
 *    a statement iterator
 */
class WorkloadStreamActor(iterator: Iterator[SQLStatement]) extends Actor {
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
  def play(advisors: Set[Advisor], visualizers: Set[Visualizer]) = {
    while (iterator.hasNext()) {
      advisors.foreach(_.process((iterator.next())))
      visualizers.foreach(_.refresh())
    }

    this ! Stop
  }

  /** Executes a specific number of statements from the workload, i.e. until there are no more 
   * statements in the stream, communicating each statement to every advisor and visualizer 
   * contained in the given sets. If the specified number of steps is greater than the remaining 
   * number of statements, the steps are just ignored.
   */
  def next(advisors: Set[Advisor], visualizers: Set[Visualizer], steps: Integer) = {
    for (i <- 1 to steps) {
      if (iterator.hasNext()) {
        advisors.foreach(_.process((iterator.next())))
        visualizers.foreach(_.refresh())
      } else {
        this ! Stop
      }
    }
  }
}

case class Stop()
case class Next(advisors: Set[Advisor], visualizers: Set[Visualizer], steps: Integer)
case class Play(advisors: Set[Advisor], visualizers: Set[Visualizer])
