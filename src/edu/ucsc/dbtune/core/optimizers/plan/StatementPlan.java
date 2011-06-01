/* ************************************************************************** *
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
 * ************************************************************************** */
package edu.ucsc.dbtune.core.optimizers.plan;

import edu.ucsc.dbtune.spi.Tree;

/**
 * Represents a plan for SQL statements of a RDBMS.
 */
public class StatementPlan extends Tree<Operator> {

    /**
     * Creates a SQL statement plan with one (given root) node.
     *
     * @param root
     *     root of the plan
     */
    public StatementPlan(Operator root) {
        super(root);

        root.setId(1);
    }

    /**
     * Returns the operator at the root of the plan.
     *
     * @return root node of the plan
     */
    public Operator getRootOperator() {
        return super.getRootElement();
    }

    /**
     * {@inheritDoc}
     */
	@Override
    public Entry<Operator> setChild(Operator parentValue, Operator childValue) {
        Entry<Operator> e;
        
        e = super.setChild(parentValue, childValue);

        childValue.setId(parentValue.getId() + 1);

        return e;
    }
}
