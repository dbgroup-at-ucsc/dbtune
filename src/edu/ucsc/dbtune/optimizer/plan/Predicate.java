package edu.ucsc.dbtune.optimizer.plan;

import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Rx;
import edu.ucsc.dbtune.util.Tree.Entry;

/**
 * Represents a predicate.
 * 
 * @author Quoc Trung Tran
 * @author Ivo Jimenez
 */
public class Predicate 
{
    private Column columnA;
    private Column columnB;
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
        this.columnA = column;

        if (text == null)
            throw new RuntimeException("text can't be null");

        predicateText = text;
    }
    
    /**
     * save everything to a xml node
     * @param rx
     */
    public void save(Rx rx) {
        if (columnA!=null)
            rx.createChild("columnA",columnA.toString());
        if (columnB!=null)
            rx.createChild("columnB",columnB.toString());
        if (predicateText!=null)
            rx.createChild("predicateText",predicateText.toString());
    }
    
    /**
     * Creates a join predicate.
     *
     * @param columnA
     *      column for which the predicate is defined on
     * @param columnB
     *      column for which the predicate is defined on
     */
    public Predicate(Column columnA, Column columnB)
    {
        this.columnA = columnA;
        this.columnB = columnB;
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
        return columnA;
    }

    /**
     * Returns the column over which the join predicate is defined on.
     *
     * @return
     *      the column
     */
    public Column getLeftColumn()
    {
        return columnA;
    }

    /**
     * Returns the column over which the join predicate is defined on.
     *
     * @return
     *      the column
     */
    public Column getRightColumn()
    {
        return columnB;
    }

    /**
     * A predicate is covered by an index if all the columns referenced by it are contained in the 
     * given index.
     *
     * @param index
     *      index that is checked for coverage
     * @return
     *      {@code true} if the index covers the predicate; {@code false} otherwise
     */
    public boolean isCoveredBy(Index index)
    {
        if (columnA != null)
            return index.contains(columnA);

        if (columnB != null)
            return index.contains(columnB);

        if (predicateText != null)
            for (Column c : index)
                if (predicateText.contains(c.getName()))
                    return true;

        return false;
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
    
        if (columnA != null && columnA.equals(o.columnA))
            return true;

        if (columnB != null && columnB.equals(o.columnB))
            return true;

        if (predicateText != null)
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

        if (columnA != null)
            code = 37 * code + columnA.hashCode();
        if (columnB != null)
            code = 37 * code + columnB.hashCode();

        if (predicateText != null)
            code = 37 * code + predicateText.hashCode();

        return code;
    }
    
     @Override
    public String toString() {
        return predicateText;
    }
}
