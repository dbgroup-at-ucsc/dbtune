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

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
@SuppressWarnings({"UnusedDeclaration"})
public class OurVsTheirsBenchmark extends SimpleBenchmark {
  @Param({"1", "10", "100"}) private int length;

  public void timeOurStack(int reps) {
      for (int i = 0; i < reps; ++i) {
          Stack<String> our = new Stack<String>();
          for (int j = 0; j < length; ++j) {
              our.push("B");
              our.pop();
          }
      }
  }

  public void timeJdkStack(int reps) {
      for (int i = 0; i < reps; ++i) {
          Deque<String> theirs = new ArrayDeque<String>();
          for (int j = 0; j < length; ++j) {
              theirs.push("B");
              theirs.pop();
          }
      }
  }

  public void timeOurQueue(int reps) {
      for (int i = 0; i < reps; ++i) {
          Queue<String> our = new Queue<String>();
          for (int j = 0; j < length; ++j) {
              our.add("B");
              our.remove();
          }
      }
  }

  public void timeJdkQueue(int reps) {
      for (int i = 0; i < reps; ++i) {
          @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
          Deque<String> theirs = new ArrayDeque<String>();
          for (int j = 0; j < length; ++j) {
              theirs.add("B");
              theirs.remove();
          }
      }
  }

  public static void main(String[] args) throws Exception {
      Runner.main(OurVsTheirsBenchmark.class, args);
  }
}
