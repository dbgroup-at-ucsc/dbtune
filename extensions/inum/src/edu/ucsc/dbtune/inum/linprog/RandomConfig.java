package edu.ucsc.dbtune.inum.linprog;

import edu.ucsc.dbtune.inum.model.Configuration;
import java.util.List;
import java.util.Random;

/**
 * Created by IntelliJ IDEA.
 * User: Dash
 * Date: Jun 8, 2007
 * Time: 5:49:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class RandomConfig {
    private Random rand;
    private String[] tables;
    private List configs[];
    private int total;

    public RandomConfig(Random rand, List configs[]) {
        this.rand = rand;
        this.configs = configs;
        this.total = 1;
        for (int i = 0; i < this.configs.length; i++) {
            List list = this.configs[i];
            total *= (list.size() + 1);
        }
    }

    public Configuration next() {
        int val = rand.nextInt(total);
        Configuration ret = new Configuration();

        // ok find the next random number.
        for (int i = 0; i < configs.length; i++) {
            int length = configs[i].size()+1;
            int idx = val % length;
            val = val/ length;
            if(idx != 0) {
                ret.addConfig((Configuration) configs[i].get(idx - 1));
            }
        }

        return ret;
    }
}
