package edu.ucsc.dbtune;

import java.io.File;
import java.io.IOException;

import org.xml.sax.SAXException;

import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;

public class AnalyzeInumSpace {
    static int nljCount(Rx rx, boolean effective) {
        int count = 0;
        if (rx.getAttribute("name").equals("NESTED.LOOP.JOIN")) {
            double rows = rx.findChild("parameters").getDoubleAttribute(
                    "cardinality");
            if (!effective || rows > 1)
                count++;
        }
        for (Rx rx2 : rx.findChilds("operator"))
            count += nljCount(rx2, effective);
        return count;
    }

    public static void main(String[] args) throws Exception {
        File dir = new File(
                "/home/wangrui/dbtune/inum/tpch_inumspace_recommend");
        for (int i = 1; i <= 22; i++) {
            Rx rx = Rx.findRoot(Rt.readFile(new File(dir, i + ".xml")));
            Rx[] plans = rx.findChilds("plan");
            int count1 = 0;
            int count2 = 0;
            int ecount1 = 0;
            int ecount2 = 0;
            for (Rx plan : plans) {
                int count = nljCount(plan.findChild("operator"), false);
                if (count == 1)
                    count1++;
                if (count > 1)
                    count2++;
                count = nljCount(plan.findChild("operator"), true);
                if (count == 1)
                    ecount1++;
                if (count > 1)
                    ecount2++;
            }
            Rt.np(rx.getAttribute("id") + "\t" + plans.length + "\t" + count1
                    + "\t" + count2 + "\t" + ecount1 + "\t" + ecount2);
        }
    }
}
