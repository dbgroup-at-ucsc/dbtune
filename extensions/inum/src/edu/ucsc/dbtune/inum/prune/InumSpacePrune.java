package edu.ucsc.dbtune.inum.prune;

import static edu.ucsc.dbtune.inum.FullTableScanIndex.getFullTableScanIndexInstance;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import javax.xml.transform.TransformerException;

import org.xml.sax.SAXException;

import edu.ucsc.dbtune.metadata.Catalog;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.plan.InumPlan;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;
import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * Prune INUM space if candidate index set is given
 * 
 * @author wangrui
 * 
 */
public class InumSpacePrune {
    public static PrunableInumPlan[] getPrunableInumSpace(Set<InumPlan> space,
            Set<Index> candidates) throws SQLException {
        InumPlan[] plans = space.toArray(new InumPlan[space.size()]);
        // if (plans[0] instanceof PrunableInumPlan)
        // return (PrunableInumPlan[]) plans;
        Vector<PrunableInumPlan> v = new Vector<PrunableInumPlan>();
        for (InumPlan plan : plans) {
            PrunableInumPlan p = new PrunableInumPlan(plan, candidates);
            if (p.usable)
                v.add(p);
        }
        return v.toArray(new PrunableInumPlan[v.size()]);
    }

    public static void save(File file, PrunableInumPlan[] plans)
            throws IOException, TransformerException {
        Rx rx = new Rx("inumSpace");
        for (PrunableInumPlan plan : plans) {
            plan.save(rx.createChild("plan"));
        }
        Rt.write(file, rx.getXml().getBytes());
    }

    public static PrunableInumPlan[] load(Catalog catalog,File file, Map<String, Index> map)
            throws SAXException, IOException, SQLException {
        Rx rx = Rx.findRoot(Rt.readFile(file));
        Rx[] rs = rx.findChilds("plan");
        Vector<PrunableInumPlan> vs = new Vector<PrunableInumPlan>();
        for (int i = 0; i < rs.length; i++) {
            PrunableInumPlan plan = new PrunableInumPlan(catalog,rs[i], map);
            vs.add(plan);
        }
        return vs.toArray(new PrunableInumPlan[vs.size()]);
    }

    public static int[] statistics(PrunableInumPlan[] plans) {
        int a = 0, b = 0;
        for (PrunableInumPlan plan : plans) {
            a += plan.numOfSlots();
            b += plan.numOfIndex();
        }
        return new int[] { plans.length, a, b };
    }

    /**
     * Prune INUM space if candidate index set is given
     * 
     * @param space
     *            INUM space
     * @param candidates
     *            candidate index set
     * @return
     * @throws SQLException
     */
    public static Set<InumPlan> prune(Set<InumPlan> space, Set<Index> candidates)
            throws SQLException {
        PrunableInumPlan[] ps = getPrunableInumSpace(space, candidates);
        ps = prune(ps, candidates);
        space.clear();
        for (PrunableInumPlan p : ps)
            space.add(p);
        return space;
    }

    public static PrunableInumPlan[] prune(PrunableInumPlan[] ps,
            Set<Index> candidates) throws SQLException {
        ps = prune1(ps, candidates);
        ps = prune2(ps, candidates);
        ps = prune3(ps, candidates);
        return ps;
    }

    public static PrunableInumPlan[] prune1(PrunableInumPlan[] ps,
            Set<Index> candidates) throws SQLException {
        double maxCost=Double.POSITIVE_INFINITY;
        for (PrunableInumPlan p1 : ps) {
            if (p1.maxCost< maxCost)
                maxCost=p1.maxCost;
        }
        Vector<PrunableInumPlan> v = new Vector<PrunableInumPlan>();
        for (PrunableInumPlan p : ps) {
            if (p.minCost< maxCost)
                v.add(p);
        }
        return v.toArray(new PrunableInumPlan[v.size()]);
    }

    public static PrunableInumPlan[] prune2(PrunableInumPlan[] ps,
            Set<Index> candidates) throws SQLException {
        Vector<PrunableInumPlan> v = new Vector<PrunableInumPlan>();
        for (PrunableInumPlan p : ps) {
            p.removeDupIndexes();
            v.add(p);
        }
        return v.toArray(new PrunableInumPlan[v.size()]);
    }

    public static PrunableInumPlan[] prune3(PrunableInumPlan[] ps,
            Set<Index> candidates) throws SQLException {
        Vector<PrunableInumPlan> v = new Vector<PrunableInumPlan>();
        for (int i = 0; i < ps.length; i++) {
            boolean remove = false;
            for (int j = i + 1; j < ps.length; j++) {
                if (ps[i].isCoveredBy(ps[j])) {
                    remove = true;
                    break;
                }
            }
            if (!remove)
                v.add(ps[i]);
        }
        return v.toArray(new PrunableInumPlan[v.size()]);
    }
}
