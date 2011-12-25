package edu.ucsc.dbtune.bip.sim;


import edu.ucsc.dbtune.bip.util.HashCodeUtil;

public class SimVariableIndex 
{
    private int typeVariable, window, q, k, i, a;
    private int fHashCode;
    
    public SimVariableIndex(int _type, int _window, int _q, int _k, int _i, int _a)
    {
        this.typeVariable = _type;
        this.window = _window;
        this.q = _q;
        this.k = _k;
        this.i = _i;
        this.a = _a;
        fHashCode = 0;
    }
    

    @Override
    public boolean equals(Object obj) 
    {
        if (!(obj instanceof SimVariableIndex))
            return false;

        SimVariableIndex var = (SimVariableIndex) obj;
        
        if ( (this.typeVariable != var.typeVariable) ||   
             (this.window != var.window) ||
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
            result = HashCodeUtil.hash(result, this.window);
            result = HashCodeUtil.hash(result, this.q);
            result = HashCodeUtil.hash(result, this.k);
            result = HashCodeUtil.hash(result, this.i);
            result = HashCodeUtil.hash(result, this.a);
            fHashCode = result;
        }
        
        return fHashCode;
    }

}
