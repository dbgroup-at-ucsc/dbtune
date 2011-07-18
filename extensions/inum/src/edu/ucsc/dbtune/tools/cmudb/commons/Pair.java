package edu.ucsc.dbtune.tools.cmudb.commons;

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: May 12, 2008
 * Time: 5:05:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class Pair<L,R> {
    private R r;
    private L l;

    public Pair(L l, R r) {
        this.l = l;
        this.r = r;
    }

    public R getR() {
        return r;
    }

    public void setR(R r) {
        this.r = r;
    }

    public L getL() {
        return l;
    }

    public void setL(L l) {
        this.l = l;
    }
}
