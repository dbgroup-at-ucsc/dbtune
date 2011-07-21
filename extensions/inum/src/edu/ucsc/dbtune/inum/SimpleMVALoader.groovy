package edu.ucsc.dbtune.inum

import edu.ucsc.dbtune.inum.commons.Persistence

/**
 * Created by IntelliJ IDEA.
 * User: ddash
 * Date: Mar 5, 2008
 * Time: 4:46:11 PM
 * To change this template use File | Settings | File Templates.
 */

def list = [];
def file = new File (args[0]);
println file.absolutePath

file.eachLine { line ->
    def matcher = line =~ /(\d+):(\d+(.\d+)?):(.*)/;

    if(matcher.matches()) {
        println line
        def id = Integer.parseInt(matcher[0][1]);
        def cost = Float.parseFloat(matcher[0][2]);
        def key = matcher[0][4];

        if (list[id] == null) {
            list[id] = [:];
        }
        
        Map cost1 = list[id];
        cost1[key] = cost;

    }
}

Persistence.saveToXML(InumUtils.getMatViewAccessCostFile("test.sql"), list);
