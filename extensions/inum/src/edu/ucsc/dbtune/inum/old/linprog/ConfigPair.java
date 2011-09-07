package edu.ucsc.dbtune.inum.old.linprog;

import edu.ucsc.dbtune.inum.old.model.PhysicalConfiguration;

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: May 1, 2008
 * Time: 3:08:38 PM
 * To change this template use File | Settings | File Templates.
 */
class ConfigPair {
    PhysicalConfiguration config;
    float cost;


    public ConfigPair(PhysicalConfiguration config, float cost) {
        this.config = config;
        this.cost = cost;
    }


    public PhysicalConfiguration getConfig() {
        return config;
    }

    public float getCost() {
        return cost;
    }

    public void setCost(float cost) {
        this.cost = cost;
    }
}
