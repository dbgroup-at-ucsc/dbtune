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

package edu.ucsc.dbtune.core.optimizer;

import edu.ucsc.dbtune.core.metadata.Index;
import edu.ucsc.dbtune.spi.core.Console;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.ToStringBuilder;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static edu.ucsc.dbtune.util.Instances.newAtomicReference;
import static edu.ucsc.dbtune.util.Instances.newFalseBoolean;

/**
 * default implementation of {@link edu.ucsc.dbtune.core.optimizer.WhatIfOptimizationBuilder} type.
 */
// todo(Huascar) remove asap
class WhatIfOptimizationBuilderImpl implements WhatIfOptimizationBuilder {

    private final String sql;
    private final AtomicReference<Double> cost;

    // optional variables
    private IndexBitSet configuration;
    private IndexBitSet usedSet;
    private Index         profiledIndex;
    private IndexBitSet usedColumns;

    private final AbstractIBGWhatIfOptimizer whatIfOptimizer;
    private final AtomicBoolean              withProfiledIndex;

    public WhatIfOptimizationBuilderImpl(
            AbstractIBGWhatIfOptimizer whatIfOptimizer,
            String sql
    ){
        this.whatIfOptimizer    = whatIfOptimizer;
        this.cost               = newAtomicReference(0.0);
        this.sql                = sql;
        this.withProfiledIndex  = newFalseBoolean();
    }

    /**
     * updates the optimization cost's value.
     * @param value
     *      new cost value.
     */
    void addCost(double value){
        Console.streaming().info("WhatIfOptimizationBuilderImpl#addCost(double) has " +
                "an actual cost="
                + cost.get() + ", and will take a new cost="
                + value
        );
        cost.compareAndSet(cost.get(), value);
    }

    /**
     * @return
     *      the configuration to be used.
     */
    public IndexBitSet getConfiguration(){
        return configuration == null ? null : configuration.clone();
    }

    /**
     * @return
     *      a profiled index.
     */
    public Index getProfiledIndex(){
        return profiledIndex;
    }

    /**
     * @return {@code true} if we are dealing with profiled indexes, {@code false}
     *      otherwise.
     */
    public boolean withProfiledIndex(){
        return withProfiledIndex.get();
    }

    /**
     * @return
     *      the sql to be used as workload.
     */
    public String getSQL(){
        return sql;
    }
    
    public IndexBitSet getUsedSet(){
        return usedSet == null ? null : usedSet.clone();
    }

    /**
     * @return
     *      the db columns used in the optimization.
     */
    public IndexBitSet getUsedColumns(){
        return usedColumns == null ? null : usedColumns.clone();
    }

    @Override
    public WhatIfOptimizationCostBuilder using(IndexBitSet config, IndexBitSet usedSet) {
        this.withProfiledIndex.set(false);
        this.configuration = config;
        this.usedSet       = usedSet;
        return this;
    }

    @Override
    public WhatIfOptimizationCostBuilder using(IndexBitSet config,
           Index profiledIndex, IndexBitSet usedColumns
    ) {
        this.withProfiledIndex.set(true);
        this.configuration = config;
        this.profiledIndex = profiledIndex;
        this.usedColumns   = usedColumns;
        return this;
    }


    @Override
    public Double toGetCost() throws SQLException{
        if(Double.compare(0.0, cost.get()) == 0){
            //whatIfOptimizer.calculateOptimizationCost();
        }
        Console.streaming().info("WhatIfOptimizationBuilderImpl#toGetCost() will return a worload cost=" + cost.get());
        return cost.get();
    }

    @Override
    public String toString() {
        return new ToStringBuilder<WhatIfOptimizationBuilderImpl>(this)
                .add("sql", getSQL())
                .add("configuration", getConfiguration())
                .add("usedSet", getUsedSet())
                .add("usedColumns", getUsedColumns())
                .add("profiledIndex", getProfiledIndex())
                .toString();
    }
}
