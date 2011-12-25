package edu.ucsc.dbtune.bip.interactions;

import edu.ucsc.dbtune.bip.util.HashCodeUtil;


public class IIPVariableIndex 
{
    private int theta, typeVariable, k, i, a;
    private int fHashCode;
    
    public IIPVariableIndex(int _theta, int _typeVariable, int _k, int _i, int _a)
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
        if (!(obj instanceof IIPVariableIndex))
            return false;

        IIPVariableIndex var = (IIPVariableIndex) obj;
        
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
