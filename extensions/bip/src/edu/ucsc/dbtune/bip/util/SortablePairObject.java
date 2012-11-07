package edu.ucsc.dbtune.bip.util;


public class SortablePairObject  implements Comparable<SortablePairObject>
{
    private double cost;
    private int id;
    
    /**
     * Constructor
     */
    public SortablePairObject(int id, double cost)
    {
        this.id = id;
        this.cost = cost;   
    }
    
    
    public int getId()
    {
        return id;
    }
    
    public double getCost()
    {
        return cost;
    }
   
    @Override
    public int compareTo(SortablePairObject o) 
    {
        double objCost = o.getCost(); 
        if (cost < objCost)
            return -1;
        else if (cost == objCost)
            return 0;
        else 
            return 1;
    }
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(" id = " + id + " cost = " + cost);
        return sb.toString();
    }
}
