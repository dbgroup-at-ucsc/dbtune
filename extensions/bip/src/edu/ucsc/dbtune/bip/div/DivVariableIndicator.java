package edu.ucsc.dbtune.bip.div;

import edu.ucsc.dbtune.util.HashCodeUtil;

public class DivVariableIndicator
{
    private int typeVariable;
    private int replica;
    private int q;
    private int planId;
    private int slotId;
    private int idx;
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
    public DivVariableIndicator(int _type, int _replica, int _q, 
                                int _planId, int _slotId, int _idx)
    {
        this.typeVariable = _type;
        this.replica = _replica;
        this.q = _q;
        this.planId = _planId;
        this.slotId = _slotId;
        this.idx = _idx;
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
             (this.planId != var.planId) ||
             (this.slotId != var.slotId) ||
             (this.idx != var.idx) ) 
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
            result = HashCodeUtil.hash(result, this.planId);
            result = HashCodeUtil.hash(result, this.slotId);
            result = HashCodeUtil.hash(result, this.idx);
            fHashCode = result;
        }
        
        return fHashCode;
    }
}
