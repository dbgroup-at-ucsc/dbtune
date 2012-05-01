package edu.ucsc.dbtune.bip.div;

public class DivConstraint 
{
    private String   type;
    private double factor;
    
    public DivConstraint(String t, double f)
    {
        type = t;
        factor = f;
    }
            
    public String getType()
    {        
        return type;
    }
        
    public double getFactor()
    {
        return factor;
    }
}


