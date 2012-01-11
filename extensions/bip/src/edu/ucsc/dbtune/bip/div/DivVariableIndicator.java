package edu.ucsc.dbtune.bip.div;

import edu.ucsc.dbtune.bip.util.HashCodeUtil;

public class DivVariableIndicator
{
    private int typeVariable, replica, q, k, i, a;
    private int fHashCode;
    
    public DivVariableIndicator(int _type, int _replica, int _q, int _k, int _i, int _a)
    {
        this.typeVariable = _type;
        this.replica = _replica;
        this.q = _q;
        this.k = _k;
        this.i = _i;
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
            result = HashCodeUtil.hash(result, this.typeVariable);
            result = HashCodeUtil.hash(result, this.replica);
            result = HashCodeUtil.hash(result, this.q);
            result = HashCodeUtil.hash(result, this.k);
            result = HashCodeUtil.hash(result, this.i);
            result = HashCodeUtil.hash(result, this.a);
            fHashCode = result;
        }
        
        return fHashCode;
    }
}
