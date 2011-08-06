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

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.ibg.CandidatePool;
import edu.ucsc.dbtune.spi.core.Supplier;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.ToStringBuilder;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class HotsetSelection {
    private final CandidatePool.Snapshot candSet;
    private final StaticIndexSet         oldHotSet;
    private final DynamicIndexSet        requiredIndexSet;
    private final StatisticsFunction     benefitFunc;
    private final int                       maxSize;
    private final boolean                   debugOutput;

    /**
     * Construct a {@code selection variable} which will be utilized by {@link HotSetSelector}.
     *
     * @param builder
     *      a {@link HotsetSelection}'s builder.
     */
    private HotsetSelection(StrictBuilder builder){
        this.candSet            = builder.candSet;
        this.oldHotSet          = builder.oldHotSet;
        this.requiredIndexSet   = builder.requiredIndexSet;
        this.benefitFunc        = builder.benefitFunc;
        this.maxSize            = builder.maxSize;
        this.debugOutput        = builder.debugOutput;
    }

    /**
     * Construct a {@code selection variable} which will be utilized by {@link HotSetSelector}.
     */
    public HotsetSelection(
        CandidatePool.Snapshot candSet,
        StaticIndexSet         oldHotSet,
        DynamicIndexSet        requiredIndexSet,
        StatisticsFunction     benefitFunc,
        int                       maxSize,
        boolean                   debugOutput )
    {
        this.candSet            = candSet;
        this.oldHotSet          = oldHotSet;
        this.requiredIndexSet   = requiredIndexSet;
        this.benefitFunc        = benefitFunc;
        this.maxSize            = maxSize;
        this.debugOutput        = debugOutput;
    }


    public CandidatePool.Snapshot getCandidateSet(){
        return Checks.checkNotNull(candSet);
    }

    public StaticIndexSet getOldHotSet(){
        return Checks.checkNotNull(oldHotSet);
    }

    public DynamicIndexSet getRequiredIndexSet(){
        return Checks.checkNotNull(requiredIndexSet);
    }

    public StatisticsFunction getBenefitFunction(){
        return Checks.checkNotNull(benefitFunc);
    }

    public int getMaxSize(){
        return maxSize;
    }

    public boolean isDebugOutputEnabled(){
        return debugOutput;
    }


    @Override
    public String toString() {
        return new ToStringBuilder<HotsetSelection>(this)
               .add("candSet", getCandidateSet())
               .add("oldHotSet", getOldHotSet())
               .add("requiredIndexSet", getRequiredIndexSet())
               .add("benefitFunc", getBenefitFunction()).add("maxSize", getMaxSize())
               .add("debugOutput", (isDebugOutputEnabled() ? "Y" : "N"))
               .toString();
    }

    /**
     * A builder of {@link HotsetSelection}s. This builder is strict in the sense that it does
     * not take optional values. In other words, all arguments values should not be null.
     *
     * @param 
     *      the {@link DBIndex index type}.
     */
    public static class StrictBuilder implements Supplier<HotsetSelection> {
        private CandidatePool.Snapshot candSet;
        private StaticIndexSet         oldHotSet;
        private DynamicIndexSet        requiredIndexSet;
        private StatisticsFunction     benefitFunc;
        private int                       maxSize;
        private boolean                   debugOutput;

        public StrictBuilder(boolean debugOutput){
            this.debugOutput = debugOutput;
        }

        public StrictBuilder candidateSet(CandidatePool.Snapshot snapshot){
            candSet = Checks.checkNotNull(snapshot);
            return this;
        }

        public StrictBuilder oldHotSet(StaticIndexSet value){
            this.oldHotSet = Checks.checkNotNull(value);
            return this;
        }

        public StrictBuilder requiredIndexSet(DynamicIndexSet value){
            this.requiredIndexSet = Checks.checkNotNull(value);
            return this;
        }

        public StrictBuilder benefitFunction(StatisticsFunction value){
            this.benefitFunc = Checks.checkNotNull(value);
            return this;
        }

        public StrictBuilder maxSize(int value){
            this.maxSize = Checks.checkNotNull(value);
            return this;
        }

        @Override
        public HotsetSelection get() {
            return new HotsetSelection(this);
        }
    }
}
