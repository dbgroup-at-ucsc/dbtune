package edu.ucsc.dbtune.seq.utils;

import edu.ucsc.dbtune.util.Rt;

public class RRange<T> {
    public double min = Double.MAX_VALUE;
    public double max = -Double.MAX_VALUE;
    public T minObject;
    public T maxObject;
    public Object minObject2;
    public Object maxObject2;
    public Object minObject3;
    public Object maxObject3;

    public void clear() {
        min = Double.MAX_VALUE;
        max = -Double.MAX_VALUE;
        minObject = null;
        maxObject = null;
        minObject2 = null;
        maxObject2 = null;
    }

    public void add(double d) {
        if (d < min)
            min = d;
        if (d > max)
            max = d;
    }

    public void add(double d, T object) {
        if (d < min) {
            min = d;
            minObject = object;
        }
        if (d > max) {
            max = d;
            maxObject = object;
        }
    }

    public void add(double d, T object, Object object2) {
        if (d < min) {
            min = d;
            minObject = object;
            minObject2 = object2;
        }
        if (d > max) {
            max = d;
            maxObject = object;
            maxObject2 = object2;
        }
    }
    public void add(double d, T object, Object object2, Object object3) {
        if (d < min) {
            min = d;
            minObject = object;
            minObject2 = object2;
            minObject3 = object3;
        }
        if (d > max) {
            max = d;
            maxObject = object;
            maxObject2 = object2;
            maxObject3 = object3;
        }
    }

    public void print() {
        Rt.p("min=%f max=%f range=%f", min, max, max - min);
    }
}