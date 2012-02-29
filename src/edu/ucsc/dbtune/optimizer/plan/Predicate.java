package edu.ucsc.dbtune.optimizer.plan;

import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;

/**
 * Represents a predicate.
 * 
 * @author Quoc Trung Tran
 * @author Ivo Jimenez
 */
public class Predicate 
{
    private Column column;
    private String predicateText;
    
    /**
     * Creates a selection predicate.
     *
     * @param column
     *      column for which the predicate is defined on
     * @param text
     *      text of the predicate
     */
    public Predicate(Column column, String text)
    {
        this.column = column;

        if (text == null)
            throw new RuntimeException("text can't be null");

        predicateText = text;
    }
    
    /**
     * Returns the text of the predicate.
     *
     * @return
     *      the predicate text
     */
    public String getText()
    {
        return predicateText;
    }
    
    /**
     * Returns the column over which the selection predicate is defined on.
     *
     * @return
     *      the column
     */
    public Column getColumn()
    {
        return column;
    }

    /**
     * @param index
     *      index that is checked for coverage
     * @return
     *      {@code true} if the index covers the predicate; {@code false} otherwise
     */
    public boolean isCoveredBy(Index index)
    {
        if (column != null)
            return index.contains(column);

        return predicateText.contains(index.getName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
    
        if (!(obj instanceof Predicate))
            return false;
    
        Predicate o = (Predicate) obj;
    
        if (column != null && column.equals(o.column))
            return true;

        if (predicateText.equals(o.predicateText))
            return true;

        return false;
    }
     
    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        int code = 1;

        if (column != null)
            code = 37 * code + column.hashCode();

        code = 37 * code + predicateText.hashCode();

        return code;
    }
}
