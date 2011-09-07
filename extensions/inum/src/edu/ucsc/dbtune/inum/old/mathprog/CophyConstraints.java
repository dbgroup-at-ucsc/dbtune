package edu.ucsc.dbtune.inum.old.mathprog;

/**
 * (C) DIAS Lab and LMC Lab., Ecole Polytechnic Federale du Lausanne
 * All rights reserved. Do not distribute the source code without explicit permission of the copyright owners.
 * <p/>
 * User: dash
 * Date: May 24, 2010
 * Time: 5:10:52 PM
 */
public class CophyConstraints {
    public double maxTime;
    public int maxIndexWidth;
    public int accuracy;
    public double indexSize;

    public static CophyConstraints getDefaultConstraints() {
        CophyConstraints constraints = new CophyConstraints();
        constraints.maxTime = 1E5;
        constraints.accuracy = 0;
        constraints.indexSize = 1024;
        constraints.maxIndexWidth = 15;

        return constraints;
    }

    public void setAll(CophyConstraints constraints) {
        maxTime = constraints.maxTime;
        maxIndexWidth = constraints.maxIndexWidth;
        accuracy = constraints.accuracy;
        indexSize = constraints.indexSize;
    }
}
