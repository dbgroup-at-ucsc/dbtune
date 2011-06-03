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
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied  *
 *   See the License for the specific language governing permissions and      *
 *   limitations under the License.                                           *
 * ************************************************************************** */
package edu.ucsc.dbtune.core.optimizers.plan;

/**
 * Represents an operator of a SQL statement plan
 */
public class Operator implements Comparable<Operator>
{
    /** ID used to identify an operator within a plan */
    protected int id;

    /** Name of operator */
    protected String name;

    /** Accumulated cost of the plan up to this operator */
    protected double accumulatedCost;

    /** Cost of the operator (not accumulated cost) */
    protected double cost;

    /** Number of tuples that the operator produces */
    protected long cardinality;

    /** When the operator is applied to a base table */
    protected String tableName;

    /** When the operator is filtering tuples */
    protected String condition;

    /**
     * creates an empty operator, its name is "EMPTY". This can be used to represent empty plans.
     */
    public Operator() {
        this("EMPTY", 0.0, 0);
    }

    /**
     * creates an operator of the given name
     *
     * @param name
     *     name of the operator
     * @param accumulated cost
     *     cost of the plan up to this operator
     * @param cardinality
     *     number of rows produced by the operator
     */
    public Operator(String name, double accumulatedCost, long cardinality) {
        this.id              = 0;
        this.name            = name;
        this.cost            = 0.0;
        this.accumulatedCost = accumulatedCost;
        this.cardinality     = cardinality;
        this.tableName       = null;
        this.condition       = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return 1 * 31 + (new Long(id)).hashCode();
    }

    /**
     * returns the operator id
     *
     * @return
     *     operator id
     */
    public int getId() {
        return id;
    }

    /**
     * assigns the value of the operator id
     *
     * @param id
     *     operator id
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * returns the accumulated cost of the operator
     *
     * @return
     *     the value corresponding to the accumulated cost of the operator
     */
    public double getAccumulatedCost() {
        return accumulatedCost;
    }

    /**
     * returns the cost of the operator
     *
     * @return
     *     the value corresponding to the cost of the operator
     */
    public void setCost(double cost) {
        this.cost = cost;
    }

    /**
     * returns the cost of the operator
     *
     * @return
     *     the value corresponding to the cost of the operator
     */
    public double getCost() {
        return cost;
    }

    /**
     * returns the number of rows produced by the operator
     *
     * @return
     *     cardinality of the operator
     */
    public long getCardinality() {
        return cardinality;
    }

    /**
     * returns the name of the table that the operator reads from
     *
     * @return
     *     name of table the operator is applied to
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * returns the operator name
     *
     * @return
     *     name of operator
     */
    public String getName() {
        return name;
    }

    /**
     * {@inheritDoc}
     */
	@Override
    public int compareTo(Operator operator)
    {
        return new Integer(this.id).compareTo(operator.id);
    }

    /**
     * {@inheritDoc}
     */
	@Override
    public String toString() {
        return
           "id: " + id + "; operator: " + name + "; cost: " + cost + "; accCost: " + accumulatedCost + "; cardinality: " + cardinality + " \n";

    }
}
