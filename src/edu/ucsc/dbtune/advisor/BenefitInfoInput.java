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

import edu.ucsc.dbtune.connectivity.DatabaseConnection;
import edu.ucsc.dbtune.ibg.CandidatePool.Snapshot;
import edu.ucsc.dbtune.spi.core.Supplier;
import edu.ucsc.dbtune.util.Checks;
import edu.ucsc.dbtune.util.IndexBitSet;
import edu.ucsc.dbtune.util.ToStringBuilder;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class BenefitInfoInput {
    private final DatabaseConnection connection;
    private final Snapshot             snapshot;
    private final StaticIndexSet       hotSet;
    private final IndexBitSet config;
    private final ProfiledQuery        profiledQ;

    private BenefitInfoInput(StrictBuilder builder){
        connection = builder.conn;
        snapshot   = builder.snapshot;
        hotSet     = builder.hotSet;
        config     = builder.config;
        profiledQ  = builder.profiledQuery;
    }

    public DatabaseConnection getDatabaseConnection() {
        return connection;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

    public StaticIndexSet getHotSet() {
        return hotSet;
    }

    public IndexBitSet getRecommendedIndexes() {
        return config;
    }

    public ProfiledQuery getProfiledQuery() {
        return profiledQ;
    }

    @Override
    public String toString() {
        return new ToStringBuilder<BenefitInfoInput>(this)
               .add("connection", getDatabaseConnection())
               .add("snapshot", getSnapshot())
               .add("hotSet", getHotSet())
               .add("recommendedIndexes", getRecommendedIndexes())
               .add("profiledQuery", getProfiledQuery())
             .toString();
    }

    public static class StrictBuilder implements Supplier<BenefitInfoInput> {
        private DatabaseConnection conn;
        private Snapshot             snapshot;
        private StaticIndexSet       hotSet;
        private IndexBitSet config;
        private ProfiledQuery        profiledQuery;
        public StrictBuilder(DatabaseConnection connection){
            this.conn = connection;
        }

        public StrictBuilder snapshot(Snapshot value){
            snapshot = Checks.checkNotNull(value);
            return this;
        }

        public StrictBuilder hotSet(StaticIndexSet value){
            hotSet = Checks.checkNotNull(value);
            return this;
        }

        public StrictBuilder recommendedIndexes(IndexBitSet value){
            config = Checks.checkNotNull(value);
            return this;
        }

        public StrictBuilder profiledQuery(ProfiledQuery value){
            profiledQuery =  Checks.checkNotNull(value);
            return this;
        }

        @Override
        public BenefitInfoInput get() {
            return new BenefitInfoInput(this);
        }
    }
}
