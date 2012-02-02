package edu.ucsc.dbtune.optimizer.plan;

import edu.ucsc.dbtune.metadata.Column;

/**
 * Represents a selection predicate
 * 
 * @author Quoc Trung Tran
 *
 */
public class Predicate 
{
    private Column columnA;
    private Column columnB;
    private String predicateText;
    
    public Predicate(Column _column, String _text)
    {
        columnA = _column;
        predicateText = _text;
    }
    
    
    public String getText()
    {
        return predicateText;
    }
    
    public Column getColumn()
    {
        return columnA;
    }

}
