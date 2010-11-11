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
package edu.ucsc.satuning.db;

import edu.ucsc.satuning.util.BitSet;
import edu.ucsc.satuning.util.ToStringBuilder;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static edu.ucsc.satuning.util.Util.newAtomicReference;
import static edu.ucsc.satuning.util.Util.newFalseBoolean;

/**
 * default implementation of {@link WhatIfOptimizationBuilder} type.
 * @param <I>
 *      the type of {@link DBIndex} class.
 */
public class WhatIfOptimizationBuilderImpl<I extends DBIndex<I>>
implements WhatIfOptimizationBuilder<I>{

    private final String sql;
    private final AtomicReference<Double> cost;

    // optional variables
    private BitSet configuration;
    private BitSet usedSet;
    private I      profiledIndex;
    private BitSet usedColumns;
    private final AbstractDatabaseWhatIfOptimizer<I> whatIfOptimizer;
    private final AtomicBoolean withProfiledIndex;

    public WhatIfOptimizationBuilderImpl(
            AbstractDatabaseWhatIfOptimizer<I> whatIfOptimizer,
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
        cost.compareAndSet(cost.get(), value);
    }

    /**
     * @return
     *      the configuration to be used.
     */
    public BitSet getConfiguration(){
        return configuration == null ? null : configuration.clone();
    }

    /**
     * @return
     *      a profiled index.
     */
    public I getProfiledIndex(){
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
    
    public BitSet getUsedSet(){
        return usedSet == null ? null : usedSet.clone();
    }

    /**
     * @return
     *      the db columns used in the optimization.
     */
    public BitSet getUsedColumns(){
        return usedColumns == null ? null : usedColumns.clone();
    }

    @Override
    public WhatIfOptimizationCostBuilder using(BitSet config, BitSet usedSet) {
        this.withProfiledIndex.set(false);
        this.configuration = config;
        this.usedSet       = usedSet;
        return this;
    }

    @Override
    public WhatIfOptimizationCostBuilder using(BitSet config,
           I profiledIndex, BitSet usedColumns
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
            whatIfOptimizer.calculateOptimizationCost();
        }
        return cost.get();
    }

    @Override
    public String toString() {
        return new ToStringBuilder<WhatIfOptimizationBuilderImpl<?>>(this)
                .add("sql", getSQL())
                .add("configuration", getConfiguration())
                .add("usedSet", getUsedSet())
                .add("usedColumns", getUsedColumns())
                .add("profiledIndex", getProfiledIndex())
                .toString();
    }
}
