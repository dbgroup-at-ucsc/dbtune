package edu.ucsc.dbtune.seq.bip.def;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import edu.ucsc.dbtune.DatabaseSystem;
import edu.ucsc.dbtune.metadata.Column;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.metadata.Schema;
import edu.ucsc.dbtune.metadata.Table;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Rx;

public class SeqInumIndex implements Serializable {
    public int id;
    public String name;
    public Index index;
    private Rx indexRx; // if index is not loaded
    public double createCost, dropCost;
    public double createCostDB2;
    public String createCostSQL;
    public double storageCost;

    public double indexBenefit; // with this index - with no index
    public double indexBenefit2; // with all index - without this index

    public SeqInumIndex(int id) {
        this.id = id;
    }

    public void save(Rx rx) {
        rx.setAttribute("id", id);
        rx.setAttribute("name", name);
        rx.createChild("createCost", createCost);
        rx.createChild("createCostDB2", createCostDB2);
        if (createCostSQL != null)
            rx.createChild("createCostSQL", createCostSQL);
        rx.createChild("dropCost", dropCost);
        rx.createChild("storageCost", storageCost);
        rx.createChild("indexBenefit", indexBenefit);
        rx.createChild("indexBenefit2", indexBenefit2);
        if (index != null) {
            Rx rx2 = rx.createChild("index");
            // rx2.createChild("name", index.getFullyQualifiedName());
            // rx2.createChild("table",
            // index.getTable().getFullyQualifiedName());
            for (Column col : index) {
                rx2.createChild("column", col
                        + (index.isAscending(col) ? "(A)" : "(D)"));
            }
        } else if (indexRx != null) {
            Rx rx2 = rx.createChild("index");
            Rx[] columns = indexRx.findChilds("column");
            for (int i = 0; i < columns.length; i++) {
                rx2.createChild("column", columns[i].getText());
            }
        }
    }

    public String getColumnNames() {
        StringBuilder sb = new StringBuilder();
        if (index != null) {
            for (Column col : index) {
                if (sb.length() == 0)
                    sb.append(col.getTable().getName());
                sb.append("," + col.getName());
            }
        } else if (indexRx != null) {
            Rx[] columns = indexRx.findChilds("column");
            for (int i = 0; i < columns.length; i++) {
                String s = columns[i].getText();
                int t = s.lastIndexOf('.');
                String table = s.substring(0, t);
                String name = s.substring(t + 1);
                if (sb.length() == 0)
                    sb.append(table);
                sb.append("," + name);
            }
        }
        return sb.toString();
    }

    public SeqInumIndex(Rx rx, DatabaseSystem db) throws SQLException {
        id = rx.getIntAttribute("id");
        name = rx.getAttribute("name");
        createCost = rx.getChildDoubleContent("createCost");
        createCostDB2 = rx.getChildDoubleContent("createCostDB2");
        createCostSQL = rx.getChildText("createCostSQL");
        dropCost = rx.getChildDoubleContent("dropCost");
        storageCost = rx.getChildDoubleContent("storageCost");
        indexBenefit = rx.getChildDoubleContent("indexBenefit");
        indexBenefit2 = rx.getChildDoubleContent("indexBenefit2");
        Rx rx2 = rx.findChild("index");
        indexRx = rx2;
        if (rx2 != null && db != null) {
            // String name=rx2.getChildText("name");
            // String tableName=rx2.getChildText("table");
            // String[] st=tableName.ssplit("\\.");
            // Schema schema=(Schema)db.getCatalog().find(st[0]);
            // Table table= schema.findTable(st[1]);
            loadIndex(db);
        } else {
            indexRx = rx2;
        }
    }

    public Index loadIndex(DatabaseSystem db) throws SQLException {
        if (index != null)
            return index;
        Rx[] columns = indexRx.findChilds("column");
        Vector<Column> v = new Vector<Column>();
        HashMap<Column, Boolean> map = new HashMap<Column, Boolean>();
        for (int i = 0; i < columns.length; i++) {
            String s = columns[i].getText();
            String cname = s.substring(0, s.indexOf('('));
            Column c = (Column) db.getCatalog().findByQualifiedName(cname);
            if (c == null)
                throw new Error(cname);
            v.add(c);
            map.put(c, "(A)".equals(s.substring(s.indexOf('('))));
        }
        index = new Index(v, map);
        return index;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("[%d\tcreate=%,.0f", id, createCost));
        sb.append(String.format("\tbenefit=%,.0f ", this.indexBenefit));
        if (indexRx != null) {
            for (Rx a : indexRx.findChilds("column")) {
                sb.append("," + a.getText());
            }
        }
        sb.append("]");
        return sb.toString();
    }
}
