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

package edu.ucsc.dbtune.advisor;

import edu.ucsc.dbtune.spi.core.Supplier;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.ToStringBuilder;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class InteractionSelection {
    private final IndexPartitions    oldPartitions;
    private final StatisticsFunction doiFunc;
    private final int                   maxNumStates;
    private final StaticIndexSet     newHotSet;

    /**
     * Construct a {code selection variable} which will be utilized by {@link InteractionSelector}.
     * @param builder
     *      a {@link InteractionSelection}'s builder.
     */
    private InteractionSelection(StrictBuilder builder){
        this.oldPartitions = builder.oldPartitions;
        this.doiFunc       = builder.doiFunc;
        this.maxNumStates  = builder.maxNumStates;
        this.newHotSet     = builder.newHotSet;
    }

    public InteractionSelection(
        IndexPartitions    oldPartitions,
        StatisticsFunction doiFunc,
        int                   maxNumStates,
        StaticIndexSet     newHotSet ) {

        this.oldPartitions = oldPartitions;
        this.doiFunc       = doiFunc;
        this.maxNumStates  = maxNumStates;
        this.newHotSet     = newHotSet;
    }


    public StaticIndexSet getNewHotSet(){
        return Checks.checkNotNull(newHotSet);
    }


    public IndexPartitions getOldPartitions(){
        return Checks.checkNotNull(oldPartitions);
    }


    public StatisticsFunction getDoiFunction(){
        return Checks.checkNotNull(doiFunc);
    }

    public int getMaxNumStates(){
        return maxNumStates;
    }


    @Override
    public String toString() {
        return new ToStringBuilder<InteractionSelection>(this)
               .add("newHotSet", getNewHotSet())
               .add("oldPartitions", getOldPartitions())
               .add("doiFunction", getDoiFunction())
               .add("maxNumStates", getMaxNumStates())
               .toString();
    }

    /**
     * A builder of {@link InteractionSelection}s. This partition builder is strict in the sense of not
     * taking optional values. In other words, all arguments values should not be null.
     */
    public static class StrictBuilder implements Supplier<InteractionSelection> {
        private IndexPartitions    oldPartitions;
        private StatisticsFunction doiFunc;
        private int                   maxNumStates;
        private StaticIndexSet     newHotSet;

        public StrictBuilder(){}

        public StrictBuilder newHotSet(StaticIndexSet value){
            this.newHotSet = value;
            return this;
        }

        public StrictBuilder oldPartitions(IndexPartitions value){
            this.oldPartitions = Checks.checkNotNull(value);
            return this;
        }

        public StrictBuilder doiFunction(StatisticsFunction value){
            this.doiFunc = Checks.checkNotNull(value);
            return this;
        }

        public StrictBuilder maxNumStates(int value){
            this.maxNumStates = Checks.checkNotNull(value);
            return this;
        }

        
        @Override
        public InteractionSelection get() {
            return new InteractionSelection(this);
        }
    }
}
