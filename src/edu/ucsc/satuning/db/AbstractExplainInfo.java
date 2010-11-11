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

import edu.ucsc.satuning.workload.SQLStatement;
import edu.ucsc.satuning.workload.SQLStatement.SQLCategory;
import java.io.Serializable;

/**
 * This class provides a skeletal implementation of the {@link ExplainInfo}
 * interface to minimize the effort required to implement this interface.
 *
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public abstract class AbstractExplainInfo<I extends DBIndex<I>> implements ExplainInfo<I> {
    protected final SQLCategory category;

    /**
     * construct an {@code AbstractExplainInfo} object.
     * @param category
     *      a {@link SQLCategory} object.
     */
    protected AbstractExplainInfo(SQLStatement.SQLCategory category){
        this.category = category;
    }

    /**
     * @return
     *      a {@link SQLCategory} object.
     */
    public SQLCategory getSQLCategory(){
        return category;
    }

    @Override
    public boolean isDML() {
        return getSQLCategory() == SQLCategory.DML;
    }
}
