package edu.ucsc.dbtune.bip.div;


public class SortableObject implements Comparable<SortableObject>
{
    private int id;
    private int val;
    
    public SortableObject(int id, int val)
    {
        this.id = id;
        this.val = val;
    }
    
    @Override
    public int compareTo(SortableObject o) 
    {   
        int objVal = o.val;
         
        if (val < objVal)
            return -1;
        else if (val == objVal)
            return 0;
        else 
            return 1;
    }
    
    public int getID()
    {
        return id;
    }
}