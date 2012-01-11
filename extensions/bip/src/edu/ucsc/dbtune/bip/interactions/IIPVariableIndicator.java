package edu.ucsc.dbtune.bip.interactions;

import edu.ucsc.dbtune.bip.util.HashCodeUtil;


public class IIPVariableIndicator 
{
    private int theta, typeVariable, k, i, a;
    private int fHashCode;
    
    /**
     * A {@code SimVarible} is defined based on the following
     * five parameters
     * 
     * @param _theta
     *      Variables defined on either VAR_EMPTY, VAR_C, VAR_D, VAR_CD           
     * @param _typeVariable
     *      The type of variable (e.g., VAR_X, VAR_Y)
     * @param _k
     *      The query plan ID
     * @param _i
     *      The slot ID
     * @param _a
     *      The position of an index in this slot
     */
    public IIPVariableIndicator(int _theta, int _typeVariable, int _k, int _i, int _a)
    {
        this.theta = _theta;
        this.typeVariable = _typeVariable;
        this.k = _k;
        this.i = _i;
        this.a = _a;
        fHashCode = 0;
    }
    

    @Override
    public boolean equals(Object obj) 
    {
        if (!(obj instanceof IIPVariableIndicator))
            return false;

        IIPVariableIndicator var = (IIPVariableIndicator) obj;
        
        if ( (this.theta != var.theta) ||
             (this.typeVariable != var.typeVariable) ||   
             (this.k != var.k) ||
             (this.i != var.i) ||
             (this.a != var.a) ) {
            return false;
        }
        
        return true;
    }

    @Override
    public int hashCode() 
    {
        if (fHashCode == 0) {
            int result = HashCodeUtil.SEED;
            result = HashCodeUtil.hash(result, this.theta);
            result = HashCodeUtil.hash(result, this.typeVariable);
            result = HashCodeUtil.hash(result, this.k);
            result = HashCodeUtil.hash(result, this.i);
            result = HashCodeUtil.hash(result, this.a);
            fHashCode = result;
        }
        
        return fHashCode;
    }


    @Override
    public String toString() {
        return "IIPVariableIndex [a=" + a + ", fHashCode=" + fHashCode + ", i="
                + i + ", k=" + k + ", theta=" + theta + ", typeVariable="
                + typeVariable + "]";
    }    
}
