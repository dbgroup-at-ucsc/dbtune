package edu.ucsc.dbtune.bip.div;


public class SortableObject implements Comparable<SortableObject>
{
    private int id;
    private double val;
    
    public SortableObject(int id, double val)
    {
        this.id = id;
        this.val = val;
    }
    
    @Override
    public int compareTo(SortableObject o) 
    {   
        return Double.compare(val, o.val);
    }
    
    public int getID()
    {
        return id;
    }
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(" id = " + id).append(" val = " + val);
        
        return sb.toString();
    }
}