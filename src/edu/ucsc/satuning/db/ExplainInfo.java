package edu.ucsc.satuning.db;

/**
 * todo(Huascar) check the real purpose of this type.
 * represents an explained index.
 * @param <I> the type of index.
 */
public interface ExplainInfo<I extends DBIndex<I>> {
    /**
     * gets the maintenance cost of an index.
     * @param index
     *      a {@link DBIndex} object.
     * @return
     *      maintenance cost.
     */
	double maintenanceCost(I index);

    /**
     * @return {@code true} if it's
     *      {@link edu.ucsc.satuning.workload.SQLStatement.SQLCategory#DML},
     *      {@code false} otherwise.
     */
    boolean isDML();
}
