package edu.ucsc.dbtune.bip.div;

import edu.ucsc.dbtune.util.HashCodeUtil;

public class DivVariableIndicator
{
    private int typeVariable;
    private int replica;
    private int q;
    private int k;
    private int a;
    private int fHashCode;
    
    /**
     * This class maps the indicator to a variable used in DivBIP. The parameters indicate
     * the following place where the variables are defined on.
     * 
     * @param _type
     *      The type of variable (e.g., X, Y, S)
     * @param _replica
     *      The replica
     * @param _q
     *      The query statement 
     * @param _k
     *      The plan ID           
     * @param _a
     *      The index ID
     */
    public DivVariableIndicator(int _type, int _replica, int _q, int _k, int _a)
    {
        this.typeVariable = _type;
        this.replica = _replica;
        this.q = _q;
        this.k = _k;
        this.a = _a;
        fHashCode = 0;
    }
    

    @Override
    public boolean equals(Object obj) 
    {
        if (!(obj instanceof DivVariableIndicator))
            return false;

        DivVariableIndicator var = (DivVariableIndicator) obj;
        
        if ( (this.typeVariable != var.typeVariable) ||   
             (this.replica != var.replica) ||
             (this.q != var.q) ||  
             (this.k != var.k) ||
             (this.a != var.a) ) 
            return false;
        
        return true;
    }

    @Override
    public int hashCode() 
    {
        if (fHashCode == 0) {
            int result = HashCodeUtil.SEED;
            result = HashCodeUtil.hash(result, this.typeVariable);
            result = HashCodeUtil.hash(result, this.replica);
            result = HashCodeUtil.hash(result, this.q);
            result = HashCodeUtil.hash(result, this.k);
            result = HashCodeUtil.hash(result, this.a);
            fHashCode = result;
        }
        
        return fHashCode;
    }
}
