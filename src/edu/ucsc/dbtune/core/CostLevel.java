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

package edu.ucsc.dbtune.core;

import edu.ucsc.dbtune.util.ToStringBuilder;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class CostLevel {
    private final double    totalCost;
    private final int       count;
    private final boolean   initialized;

    private CostLevel(double totalCost, int count, boolean initialized){
        this.totalCost      = totalCost;
        this.count          = count;
        this.initialized    = initialized;
    }

    /**
     * create an immutable {@code CostOfLevel} instance.
     * @param totalCost
     *      total cost of level.
     * @param count
     *      # of levels
     * @return
     *      a new immutable {@link CostLevel} object.
     */
    public static CostLevel valueOf(double totalCost, int count){
        return new CostLevel(totalCost, count, (!(totalCost == 0.0 || count == 0)));
    }
    
    public CostLevel close(){
        return new CostLevel(0.0, 0, false);
    }

    void ensureInitialization(){
        if(!isInitialized()){
            throw new IllegalStateException();
        }
    }

    public double getTotalCost() {
        ensureInitialization();
        return totalCost;
    }

    public int getCount() {
        ensureInitialization();
        return count;
    }

    public boolean isInitialized(){
        return initialized;
    }

    @Override
    public String toString() {
        if(!isInitialized()) return "CostLevel(...)";
        return new ToStringBuilder<CostLevel>(this)
               .add("total cost", getTotalCost())
               .add("count", getCount())
               .toString();
    }
}
