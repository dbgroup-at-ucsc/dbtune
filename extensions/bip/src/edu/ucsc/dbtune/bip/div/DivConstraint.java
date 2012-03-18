package edu.ucsc.dbtune.bip.div;

public class DivConstraint 
{
    private int    type;
    private double factor;
    
    public DivConstraint(int t, double f)
    {
        type = t;
        factor = f;
    }
            
    public int getType()
    {        
        return type;
    }
        
    public double getFactor()
    {
        return factor;
    }
}


