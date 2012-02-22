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

     @Override
    public boolean equals(Object obj) {
        if (obj instanceof Predicate) {
            Predicate p = (Predicate) obj;
            if (this.columnA != p.columnA)
                return false;
            if (this.columnB != p.columnB)
                return false;
            if (predicateText == null) {
                if (p.predicateText != null)
                    return false;
                return true;
            } else {
                String s1 = predicateText;
                int t1 = s1.lastIndexOf("(id=");
                if (t1 > 0)
                    s1 = s1.substring(0, t1);
                String s2 = predicateText;
                int t2 = s1.lastIndexOf("(id=");
                if (t2 > 0)
                    s2 = s2.substring(0, t2);
                return s1.equals(s2);
            }
        }
        // TODO Auto-generated method stub
        return super.equals(obj);
    }
     
    @Override
    public int hashCode() {
        int hashCode = 1;
        if (columnA != null)
            hashCode += columnA.hashCode();
        if (columnB != null)
            hashCode += columnB.hashCode();
        String s1 = predicateText;
        int t1 = s1.lastIndexOf("(id=");
        if (t1 > 0)
            s1 = s1.substring(0, t1);
        hashCode += s1.hashCode();
        return hashCode;
    }
}
