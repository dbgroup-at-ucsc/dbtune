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
import edu.ucsc.dbtune.core.DatabaseConnection;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.spi.core.Supplier;
import edu.ucsc.dbtune.spi.ibg.ProfiledQuery;
import edu.ucsc.dbtune.util.BitSet;
import edu.ucsc.dbtune.util.PreConditions;
import edu.ucsc.dbtune.util.ToStringBuilder;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class BenefitInfoInput <I extends DBIndex<I>> {
    private final DatabaseConnection<I>   connection;
    private final Snapshot<I>             snapshot;
    private final StaticIndexSet<I>       hotSet;
    private final BitSet                  config;
    private final ProfiledQuery<I>        profiledQ;

    private BenefitInfoInput(StrictBuilder<I> builder){
        connection = builder.conn;
        snapshot   = builder.snapshot;
        hotSet     = builder.hotSet;
        config     = builder.config;
        profiledQ  = builder.profiledQuery;
    }

    public DatabaseConnection<I> getDatabaseConnection() {
        return connection;
    }

    public Snapshot<I> getSnapshot() {
        return snapshot;
    }

    public StaticIndexSet<I> getHotSet() {
        return hotSet;
    }

    public BitSet getRecommendedIndexes() {
        return config;
    }

    public ProfiledQuery<I> getProfiledQuery() {
        return profiledQ;
    }

    @Override
    public String toString() {
        return new ToStringBuilder<BenefitInfoInput<I>>(this)
               .add("connection", getDatabaseConnection())
               .add("snapshot", getSnapshot())
               .add("hotSet", getHotSet())
               .add("recommendedIndexes", getRecommendedIndexes())
               .add("profiledQuery", getProfiledQuery())
             .toString();
    }

    public static class StrictBuilder <I extends DBIndex<I>> implements Supplier<BenefitInfoInput<I>> {
        private DatabaseConnection<I>   conn;
        private Snapshot<I>             snapshot;
        private StaticIndexSet<I>       hotSet;
        private BitSet                  config;
        private ProfiledQuery<I>        profiledQuery;
        public StrictBuilder(DatabaseConnection<I> connection){
            this.conn = connection;
        }

        public StrictBuilder<I> snapshot(Snapshot<I> value){
            snapshot = PreConditions.checkNotNull(value);
            return this;
        }

        public StrictBuilder<I> hotSet(StaticIndexSet<I> value){
            hotSet = PreConditions.checkNotNull(value);
            return this;
        }

        public StrictBuilder<I> recommendedIndexes(BitSet value){
            config = PreConditions.checkNotNull(value);
            return this;
        }

        public StrictBuilder<I> profiledQuery(ProfiledQuery<I> value){
            profiledQuery =  PreConditions.checkNotNull(value);
            return this;
        }

        @Override
        public BenefitInfoInput<I> get() {
            return new BenefitInfoInput<I>(this);
        }
    }
}
