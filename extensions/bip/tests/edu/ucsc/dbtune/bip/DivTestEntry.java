package edu.ucsc.dbtune.bip;

import java.util.List;

/**
 * Keep the result of DIVBIP
 * @author Quoc Trung Tran
 *
 */
public class DivTestEntry
{
    public static final String NUMBER_REPLICA = "number_replica";
    public static final String SPACE_BUDGET = "space_budget";
    
    private List<DivParameter> parameters;
    private double inumCost;
    
    public DivTestEntry(List<DivParameter> parameters, double cost)
    {
        this.parameters = parameters;
        this.inumCost = cost;
    }
    
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        
        sb.append(" Parameters: \n");
        for (DivParameter par : parameters)
            sb.append(par);
        
        sb.append(" INUM cost: " + Math.round(inumCost) + "\n");
        
        return sb.toString();
    }
    
    /**
     * Check if the entry contains the given parameter
     * @param par
     * @return
     */
    public boolean containParameter(DivParameter objPar)
    {   
        for (DivParameter par : parameters) {
            if (par.getName().equals(objPar.getName())
                && par.getVal() == objPar.getVal())
                return true;
        }
        
        return false;
    }
    
    public double getCost()
    {
        return inumCost;
    }
    
    static public class DivParameter
    {
        private String name;
        private double value;
        
        public DivParameter(String _name, double _value)
        {
            name = _name;
            value = _value;
        }
        
        public double getVal()
        {
            return value;
        }
        
        public String getName()
        {
            return name;
        }
        
        public String toString()
        {
            StringBuffer sb = new StringBuffer();
            sb.append(" name = " + name + ", ")
              .append(" value = " + value + "\n");
            
            return sb.toString();
        }
    }
}
