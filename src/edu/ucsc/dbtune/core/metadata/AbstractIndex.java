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

package edu.ucsc.dbtune.core.metadata;

import edu.ucsc.dbtune.core.DBIndex;
import edu.ucsc.dbtune.core.DatabaseColumn;
import edu.ucsc.dbtune.core.DatabaseTable;
import java.util.Collections;
import java.util.List;

/**
 * This class provides a skeletal implementation of the {@link edu.ucsc.dbtune.core.DBIndex}
 * interface to minimize the effort required to implement this interface.
 *
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
abstract class AbstractIndex implements DBIndex {
    protected final int       internalId;
    protected final String    creationText;
    protected final double    creationCost;
    protected final double    size;

  /**
     * construct {@code AbstractDBIndex} given an internalId, creationText,
     * and creationCost.
     * @param internalId
     *      {@link edu.ucsc.dbtune.core.DBIndex}'s internal id.
     * @param creationText
     *      {@link edu.ucsc.dbtune.core.DBIndex}'s creation text.
     * @param size
     *      {@link edu.ucsc.dbtune.core.DBIndex}'s size (in megabytes).
     * @param creationCost
     *      {@link edu.ucsc.dbtune.core.DBIndex}'s creation cost.
     */
    protected AbstractIndex(
            int internalId,
            String creationText,
            double size,
            double creationCost
    ){
        this.internalId     = internalId;
        this.creationText   = creationText;
        this.size           = size;
        this.creationCost   = creationCost;
    }

    @Override
    public double creationCost() {
        return creationCost;
    }

    @Override
    public String creationText() {
        return creationText;
    }

    @Override
    public int internalId() {
        return internalId;
    }

  @Override public List<DatabaseColumn> getColumns() {
    return Collections.emptyList();
  }

  @Override
    public boolean isOn(DatabaseTable name) {
        return false;
    }

    @Override
    public double megabytes() {
        return size;
    }
}
