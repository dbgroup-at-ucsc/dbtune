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
import edu.ucsc.dbtune.util.PreConditions;
import edu.ucsc.dbtune.util.ToStringBuilder;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class HotsetSelection<I extends DBIndex<I>> {
    private final CandidatePool.Snapshot<I>   candSet;
    private final StaticIndexSet<I>           oldHotSet;
    private final DynamicIndexSet<I>          requiredIndexSet;
    private final StatisticsFunction<I>       benefitFunc;
    private final int                         maxSize;
    private final boolean                     debugOutput;

    /**
     * Construct a {code selection variable} which will be utilized by {@link HotSetSelector}.
     * @param builder
     *      a {@link HotsetSelection}'s builder.
     */
    private HotsetSelection(StrictBuilder<I> builder){
        this.candSet            = builder.candSet;
        this.oldHotSet          = builder.oldHotSet;
        this.requiredIndexSet   = builder.requiredIndexSet;
        this.benefitFunc        = builder.benefitFunc;
        this.maxSize            = builder.maxSize;
        this.debugOutput        = builder.debugOutput;
    }

    public CandidatePool.Snapshot<I> getCandidateSet(){
        return PreConditions.checkNotNull(candSet);
    }

    public StaticIndexSet<I> getOldHotSet(){
        return PreConditions.checkNotNull(oldHotSet);
    }

    public DynamicIndexSet<I> getRequiredIndexSet(){
        return PreConditions.checkNotNull(requiredIndexSet);
    }

    public StatisticsFunction<I> getBenefitFunction(){
        return PreConditions.checkNotNull(benefitFunc);
    }

    public int getMaxSize(){
        return maxSize;
    }

    public boolean isDebugOutputEnabled(){
        return debugOutput;
    }


    @Override
    public String toString() {
        return new ToStringBuilder<HotsetSelection<I>>(this)
               .add("candSet", getCandidateSet())
               .add("oldHotSet", getOldHotSet())
               .add("requiredIndexSet", getRequiredIndexSet())
               .add("benefitFunc", getBenefitFunction()).add("maxSize", getMaxSize())
               .add("debugOutput", (isDebugOutputEnabled() ? "Y" : "N"))
               .toString();
    }

    /**
     * A builder of {@link HotsetSelection}s. This builder is strict in the sense of it does
     * not take optional values. In other words, all arguments values should not be null.
     * @param <I>
     *      the {@link DBIndex index type}.
     */
    public static class StrictBuilder<I extends DBIndex<I>> implements Supplier<HotsetSelection<I>> {
        private CandidatePool.Snapshot<I>   candSet;
	    private StaticIndexSet<I>           oldHotSet;
	    private DynamicIndexSet<I>          requiredIndexSet;
	    private StatisticsFunction<I>       benefitFunc;
	    private int                         maxSize;
	    private boolean                     debugOutput;

        public StrictBuilder(boolean debugOutput){
            this.debugOutput = debugOutput;
        }

        public StrictBuilder<I> candidateSet(CandidatePool.Snapshot<I> snapshot){
            candSet = PreConditions.checkNotNull(snapshot);
            return this;
        }

        public StrictBuilder<I> oldHotSet(StaticIndexSet<I> value){
            this.oldHotSet = PreConditions.checkNotNull(value);
            return this;
        }

        public StrictBuilder<I> requiredIndexSet(DynamicIndexSet<I> value){
            this.requiredIndexSet = PreConditions.checkNotNull(value);
            return this;
        }

        public StrictBuilder<I> benefitFunction(StatisticsFunction<I> value){
            this.benefitFunc = PreConditions.checkNotNull(value);
            return this;
        }

        public StrictBuilder<I> maxSize(int value){
            this.maxSize = PreConditions.checkNotNull(value);
            return this;
        }

        
        @Override
        public HotsetSelection<I> get() {
            return new HotsetSelection<I>(this);
        }
    }
}
