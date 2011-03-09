package edu.ucsc.satuning.db;

import java.sql.SQLException;

/**
 * 
 * @param <I>
 */
public interface DBIndex<I extends DBIndex<I>> {
    /**
     * 
     * @return
     *    a
     */
    double creationCost();

    /**
     * 
     * @return
     *    a
     */
	String creationText();

    /**
     * 
     * @return
     *    a
     */
	DatabaseTable baseTable();

    /**
     * 
     * @return
     *    a
     */
	int columnCount();

    /**
     * 
     * @param id
     * @return
     *    a
     * @throws SQLException
     */
	I consDuplicate(int id) throws SQLException;

    @Override
    boolean equals(Object obj);

    /**
     * 
     * @param i
     * @return
     *    a
     */
	// we only need to use the equals() method of a column
    DatabaseIndexColumn getColumn(int i);
    
    @Override
	int hashCode();

    /**
     * 
     * @return
     *    a
     */
	int internalId();

    /**
     * 
     * @return
     *    a
     */
	double megabytes();

    /**
     * 
     * @return
     *    a
     */
	String toString();
}
