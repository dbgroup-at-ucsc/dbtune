/*
 * ****************************************************************************
 *   Copyright 2010 University of California Santa Cruz                       *
 *                                                                            *
 *   Licensed under the Apache License, Version 2.0 (the "License");          *
 *   you may not use this file except in compliance with the License.         *
 *   You may obtain a copy of the License at                                  *
 *                                                                            *
 *       http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                            *
 *   Unless required by applicable law or agreed to in writing, software      *
 *   distributed under the License is distributed on an "AS IS" BASIS,        *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 *  ****************************************************************************
 */
package edu.ucsc.dbtune.util;

import com.google.caliper.Param;
import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import edu.ucsc.dbtune.ibg.IndexBenefitGraph;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedQueue;

import static edu.ucsc.dbtune.DBTuneInstances.makeIBGNode;
import static edu.ucsc.dbtune.DBTuneInstances.makeRandomIBGNode;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class OurVsTheirsBenchmark extends SimpleBenchmark {
  // created in advance so that the time of its construction (reflection is  is not taken into
  // consideration when updating the object containers in the below
  // benchmarks.
  private static final IndexBenefitGraph.IBGNode NODE;
  static {
     NODE = makeRandomIBGNode();
  }

  @Param({"1", "10", "100"}) private int length;

  public void timeOurStack(int reps) {
      for (int i = 0; i < reps; ++i) {
          DefaultStack<IndexBenefitGraph.IBGNode> our = new DefaultStack<IndexBenefitGraph.IBGNode>();
          for (int j = 0; j < length; ++j) {
              our.push(NODE);
              our.pop();
          }
      }
  }

  public void timeJdkStack(int reps) {
      for (int i = 0; i < reps; ++i) {
          Deque<IndexBenefitGraph.IBGNode> theirs = new ArrayDeque<IndexBenefitGraph.IBGNode>();
          for (int j = 0; j < length; ++j) {
              theirs.push(NODE);
              theirs.pop();
          }
      }
  }

  public void timeOurQueue(int reps) {
      for (int i = 0; i < reps; ++i) {
          DefaultQueue<IndexBenefitGraph.IBGNode> our = new DefaultQueue<IndexBenefitGraph.IBGNode>();
          for (int j = 0; j < length; ++j) {
              our.add(NODE);
              our.remove();
          }
      }
  }

  public void timeJdkQueue(int reps) {
      for (int i = 0; i < reps; ++i) {
          Deque<IndexBenefitGraph.IBGNode> theirs = new ArrayDeque<IndexBenefitGraph.IBGNode>();
          for (int j = 0; j < length; ++j) {
              theirs.add(NODE);
              theirs.remove();
          }
      }
  }

  public void timeOurConcurrentQuery(int reps){
      for(int i = 0; i <  reps; ++i){
          DefaultConcurrentQueue<IndexBenefitGraph.IBGNode> our = new DefaultConcurrentQueue<IndexBenefitGraph.IBGNode>(length);
          for(int j = 0; j < length; ++j){
              our.put(NODE, DefaultConcurrentQueue.PutOption.POP);
              our.get(DefaultConcurrentQueue.GetOption.THROW);
          }
      }
  }

  public void timeJdkConcurrentLinkedQueue(int reps){
      for(int i = 0; i <  reps; ++i){
          java.util.Queue<IndexBenefitGraph.IBGNode> theirs = new ConcurrentLinkedQueue<IndexBenefitGraph.IBGNode>();
          for(int j = 0; j < length; ++j){
              theirs.offer(NODE);
              theirs.poll();
          }
      }
  }

  public void timeJdkBlockingConcurrentQueue(int reps) {
      for(int i = 0; i <  reps; ++i){
          java.util.concurrent.BlockingQueue<IndexBenefitGraph.IBGNode> our = new java.util.concurrent.ArrayBlockingQueue<IndexBenefitGraph.IBGNode>(length);
          for(int j = 0; j < length; ++j){
              our.offer(NODE);
              our.poll();
          }
      }
  }

  public void timeOurBlockingConcurrentQueue(int reps) {
      for(int i = 0; i <  reps; ++i){
          DefaultBlockingQueue<IndexBenefitGraph.IBGNode> our = new DefaultBlockingQueue<IndexBenefitGraph.IBGNode>(length);
          for(int j = 0; j < length; ++j){
              our.put(makeIBGNode(j));
              our.get();
          }
      }
  }

  public static void main(String[] args) throws Exception {
      Runner.main(OurVsTheirsBenchmark.class, args);
  }
}
