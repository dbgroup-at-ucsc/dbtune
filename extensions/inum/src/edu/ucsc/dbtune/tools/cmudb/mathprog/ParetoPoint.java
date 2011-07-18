package edu.ucsc.dbtune.tools.cmudb.mathprog;

/**
 * (C) DIAS Lab and LMC Lab., Ecole Polytechnic Federale du Lausanne
 * All rights reserved. Do not distribute the source code without explicit permission of the copyright owners.
 * <p/>
 * User: dash
 * Date: May 21, 2010
 * Time: 3:41:22 PM
 */
public class ParetoPoint {
    private double cost;
    private double storage;
    private long time;
    private double cCoeff;
    private double sCoeff;

    public ParetoPoint(double cost, double storage, long time, double cCoeff, double sCoeff) {
        this.cost = cost;
        this.storage = storage;
        this.time = time;
        this.cCoeff = cCoeff;
        this.sCoeff = sCoeff;
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    public double getStorage() {
        return storage;
    }

    public void setStorage(double storage) {
        this.storage = storage;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public double getcCoeff() {
        return cCoeff;
    }

    public void setcCoeff(double cCoeff) {
        this.cCoeff = cCoeff;
    }

    public double getsCoeff() {
        return sCoeff;
    }

    public void setsCoeff(double sCoeff) {
        this.sCoeff = sCoeff;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ParetoPoint that = (ParetoPoint) o;

        if (Double.compare(that.storage, storage) != 0) return false;

        return true;
    }

    @Override
    public int hashCode() {
        long temp = storage != +0.0d ? Double.doubleToLongBits(storage) : 0L;
        return (int) (temp ^ (temp >>> 32));
    }

    @Override
    public String toString() {
        return "ParetoPoint{" +
                "cost=" + cost +
                ", storage=" + storage +
                ", time=" + time +
                ", cCoeff=" + cCoeff +
                ", sCoeff=" + sCoeff +
                '}';
    }
}