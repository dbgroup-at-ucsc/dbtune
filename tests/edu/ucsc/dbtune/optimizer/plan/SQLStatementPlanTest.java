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
package edu.ucsc.dbtune.optimizer.plan;

import org.junit.Test;

import edu.ucsc.dbtune.optimizer.plan.Operator;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Ivo Jimenez (ivo@cs.ucsc.edu.com)
 */
public class SQLStatementPlanTest {

    @Test
    public void testBasicUsage() {
        Operator         root = new Operator("Root",20.67,2);
        SQLStatementPlan plan = new SQLStatementPlan(root);

        assertThat(plan.size(), is(1));

        Operator left = new Operator("SeqScan",19.50,2202);
        Operator right = new Operator("SeqScan",1.50,88);

        plan.setChild(root, left);
        plan.setChild(root, right);

        assertThat(plan.size(), is(3));

        assertThat(plan.getRootOperator().getId(), is(1));
        assertThat(plan.getChildren(plan.getRootOperator()).size(), is(2));
    }
}
