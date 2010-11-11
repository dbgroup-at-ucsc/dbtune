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
     */
    double creationCost();

    /**
     * 
     * @return
     */
	String creationText();

    /**
     * 
     * @return
     */
	DatabaseTable baseTable();

    /**
     * 
     * @return
     */
	int columnCount();

    /**
     * 
     * @param id
     * @return
     * @throws SQLException
     */
	I consDuplicate(int id) throws SQLException;

    @Override
    boolean equals(Object obj);

    /**
     * 
     * @param i
     * @return
     */
	// we only need to use the equals() method of a column
    DatabaseIndexColumn getColumn(int i);
    
    @Override
	int hashCode();

    /**
     * 
     * @return
     */
	int internalId();

    /**
     * 
     * @return
     */
	double megabytes();

    /**
     * 
     * @return
     */
	String toString();
}
