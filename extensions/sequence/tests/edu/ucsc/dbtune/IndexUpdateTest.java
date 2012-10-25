package edu.ucsc.dbtune;

import static edu.ucsc.dbtune.DatabaseSystem.newDatabaseSystem;

import java.io.File;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import edu.ucsc.dbtune.advisor.db2.DB2IndexInfo;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.optimizer.DB2Optimizer;
import edu.ucsc.dbtune.optimizer.ExplainTables;
import edu.ucsc.dbtune.optimizer.ExplainedSQLStatement;
import edu.ucsc.dbtune.optimizer.InumOptimizer;
import edu.ucsc.dbtune.optimizer.plan.Operator;
import edu.ucsc.dbtune.optimizer.plan.SQLStatementPlan;
import edu.ucsc.dbtune.seq.utils.RTimer;
import edu.ucsc.dbtune.seq.utils.RTimerN;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;

public class IndexUpdateTest {

    void indexUpdateCostTest(DatabaseSystem db) throws Exception {
        InumOptimizer optimizer = (InumOptimizer) db.getOptimizer();
        DB2Optimizer db2optimizer = (DB2Optimizer) optimizer.getDelegate();
        String[] columns = ("TPCDS.STORE_SALES.SS_SOLD_DATE_SK(A)"
                + "+TPCDS.STORE_SALES.SS_SOLD_TIME_SK(A)"
                + "+TPCDS.STORE_SALES.SS_ITEM_SK(A)"
                + "+TPCDS.STORE_SALES.SS_CUSTOMER_SK(A)"
                + "+TPCDS.STORE_SALES.SS_CDEMO_SK(A)"
                + "+TPCDS.STORE_SALES.SS_HDEMO_SK(A)"
                + "+TPCDS.STORE_SALES.SS_ADDR_SK(A)"
                + "+TPCDS.STORE_SALES.SS_STORE_SK(A)"
                + "+TPCDS.STORE_SALES.SS_PROMO_SK(A)"
                + "+TPCDS.STORE_SALES.SS_TICKET_NUMBER(A)"
                + "+TPCDS.STORE_SALES.SS_QUANTITY(A)"
                + "+TPCDS.STORE_SALES.SS_WHOLESALE_COST(A)"
                + "+TPCDS.STORE_SALES.SS_LIST_PRICE(A)"
                + "+TPCDS.STORE_SALES.SS_SALES_PRICE(A)"
                + "+TPCDS.STORE_SALES.SS_EXT_DISCOUNT_AMT(A)"
                + "+TPCDS.STORE_SALES.SS_EXT_SALES_PRICE(A)"
                + "+TPCDS.STORE_SALES.SS_EXT_WHOLESALE_COST(A)"
                + "+TPCDS.STORE_SALES.SS_EXT_LIST_PRICE(A)"
                + "+TPCDS.STORE_SALES.SS_EXT_TAX(A)"
                + "+TPCDS.STORE_SALES.SS_COUPON_AMT(A)"
                + "+TPCDS.STORE_SALES.SS_NET_PAID(A)"
                + "+TPCDS.STORE_SALES.SS_NET_PAID_INC_TAX(A)"
                + "+TPCDS.STORE_SALES.SS_NET_PROFIT(A)").split("\\+");
        Index[] testSet = new Index[columns.length];
        for (int i = 1; i <= columns.length; i++) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < i; j++) {
                sb.append("+" + columns[j]);
            }
            testSet[i - 1] = InumTest2.createIndex(db, "[" + sb + "]");
        }
        Rt
                .np("columns\tbaseTableUpdateCost\trows\tindex overhead\tindex size\twhatIfTime\tdesignAdvisorTime\tindex");
        for (Index index : testSet) {
            Set<Index> indexes = new HashSet<Index>();
            indexes.add(index);
            SQLStatement sql = new SQLStatement(
                    "delete from tpcds.store_sales where\n"
                            + "    SS_SOLD_DATE_SK in (select d_date_sk from tpcds.date_dim where\n"
                            + "     d_year<2000)");
            // ExplainTables.dump = true;
            RTimer timer = new RTimer();

            timer.reset();
            DB2IndexInfo info = DB2IndexInfo.getInfo(db, index, sql);
            double designAdvisorTime = timer.getSecondElapse();

            timer.reset();
            ExplainedSQLStatement db2plan = db2optimizer.explain(sql, indexes);
            double whatIfTime = timer.getSecondElapse();

            SQLStatementPlan plan = db2plan.getPlan();
            Operator update = plan.getChildren(plan.getRootOperator()).get(0);
            Rt.np(index.columns().size() + "\t"
                    + db2plan.getBaseTableUpdateCost() + "\t"
                    + update.cardinality + "\t" + info.updateCost + "\t"
                    + info.sizeMB + "\t" + whatIfTime + "\t"
                    + designAdvisorTime + "\t" + index.toString());
        }
        System.exit(0);
    }

    void realIndexUpdateCostTest(DatabaseSystem db) throws Exception {
        InumOptimizer optimizer = (InumOptimizer) db.getOptimizer();
        DB2Optimizer db2optimizer = (DB2Optimizer) optimizer.getDelegate();
        String table = "tpcds.customer_address";
        String[] columns = { "CA_ADDRESS_SK", "CA_ADDRESS_ID",
                "CA_STREET_NUMBER", "CA_STREET_NAME", "CA_STREET_TYPE",
                "CA_SUITE_NUMBER", "CA_CITY", "CA_COUNTY", "CA_STATE",
                "CA_ZIP", "CA_COUNTRY", "CA_GMT_OFFSET", "CA_LOCATION_TYPE" };
        Statement st = db.getConnection().createStatement();

        table = "TPCDS.STORE_SALES";
        columns = new String[] { "SS_SOLD_DATE_SK", "SS_SOLD_TIME_SK",
                "SS_ITEM_SK", "SS_CUSTOMER_SK", "SS_CDEMO_SK", "SS_HDEMO_SK",
                "SS_ADDR_SK", "SS_STORE_SK", "SS_PROMO_SK", "SS_TICKET_NUMBER",
                "SS_QUANTITY", "SS_WHOLESALE_COST", "SS_LIST_PRICE",
                "SS_SALES_PRICE", "SS_EXT_DISCOUNT_AMT", "SS_EXT_SALES_PRICE",
                "SS_EXT_WHOLESALE_COST", "SS_EXT_LIST_PRICE", "SS_EXT_TAX",
                "SS_COUPON_AMT", "SS_NET_PAID", "SS_NET_PAID_INC_TAX",
                "SS_NET_PROFIT", };

        // st.execute("select max(" + columns[0] + ") from " + table);
        // ExplainTables.dumpResult(st.getResultSet());
        // System.exit(0);
        // st.execute("drop index tpcds.indexr1");
        // st.execute("drop index tpcds.indexr2");
        // st.execute("drop index tpcds.indexr3");
        st
                .execute("delete from " + table + " where " + columns[0]
                        + ">2500000");

        Rt
                .np("columns\tcreateIndexTime\tinsertTime\tselectTime\tdeleteTime\tselectTime\tdropIndexTime\tindexColumns");
        for (int n = 0; n <= columns.length; n++) {
            RTimerN timer = new RTimerN();
            StringBuilder sb = new StringBuilder();
            if (n > 0) {
                for (int j = 0; j < n; j++) {
                    if (j > 0)
                        sb.append(",");
                    sb.append(columns[j]);
                }
                st.execute("create index tpcds.indexr1 on " + table + " (" + sb
                        + ")");
            }
            double createIndexTime = timer.getSecondElapse();
            // ExplainTables.dump = true;

            timer.reset();
            int batchSize = 1000;
            for (int i = 0; i < batchSize; i++) {
                // st.execute("INSERT INTO tpcds.customer_address \n" + "(\n"
                // + "    CA_ADDRESS_SK,\n" + "    CA_ADDRESS_ID,\n"
                // + "    CA_STREET_NUMBER,\n" + "    CA_STREET_NAME,\n"
                // + "    CA_STREET_TYPE,\n" + "    CA_SUITE_NUMBER,\n"
                // + "    CA_CITY,\n" + "    CA_COUNTY,\n"
                // + "    CA_STATE,\n" + "    CA_ZIP,\n"
                // + "    CA_COUNTRY,\n" + "    CA_GMT_OFFSET,\n"
                // + "    CA_LOCATION_TYPE\n" + ") values ("
                // + (2500000 + i) + "," + "    'AAAAAAAACLAJAAAA',\n"
                // + "    '326       ',\n" + "    'Chestnut Main',\n"
                // + "    'Ln             ',\n" + "    'Suite I   ',\n"
                // + "    'Spring Hill',\n" + "    'Leflore County',\n"
                // + "    'MS',\n" + "    '56787     ',\n"
                // + "    'United States',\n" + "    -6.00,\n"
                // + "    'apartment           '\n" + ")");
                st.execute("insert into tpcds.store_sales\n" + "(\n"
                        + "    SS_SOLD_DATE_SK,\n" + "    SS_SOLD_TIME_SK,\n"
                        + "    SS_ITEM_SK,\n" + "    SS_CUSTOMER_SK,\n"
                        + "    SS_CDEMO_SK,\n" + "    SS_HDEMO_SK,\n"
                        + "    SS_ADDR_SK,\n" + "    SS_STORE_SK,\n"
                        + "    SS_PROMO_SK,\n" + "    SS_TICKET_NUMBER,\n"
                        + "    SS_QUANTITY,\n" + "    SS_WHOLESALE_COST,\n"
                        + "    SS_LIST_PRICE,\n" + "    SS_SALES_PRICE,\n"
                        + "    SS_EXT_DISCOUNT_AMT,\n"
                        + "    SS_EXT_SALES_PRICE,\n"
                        + "    SS_EXT_WHOLESALE_COST,\n"
                        + "    SS_EXT_LIST_PRICE,\n" + "    SS_EXT_TAX,\n"
                        + "    SS_COUPON_AMT,\n" + "    SS_NET_PAID,\n"
                        + "    SS_NET_PAID_INC_TAX,\n" + "    SS_NET_PROFIT\n"
                        + ") values (\n" + "    "
                        + (2500000 + i)
                        + ",\n"
                        + "    44911,\n"
                        + "    97607,\n"
                        + "    292215,\n"
                        + "    1517913,\n"
                        + "    1564,\n"
                        + "    147662,\n"
                        + "    16,\n"
                        + "    40,\n"
                        + "    2400001,\n"
                        + "    29,\n"
                        + "    87.52,\n"
                        + "    134.78,\n"
                        + "    52.56,\n"
                        + "    0.00,\n"
                        + "    1524.24,\n"
                        + "    2538.08,\n"
                        + "    3908.62,\n"
                        + "    15.24,\n"
                        + "    0.00,\n"
                        + "    1524.24,\n"
                        + "    1539.48,\n"
                        + "    -1013.84\n" + ")");
            }
            // st.executeBatch();
            double insertTime = timer.getSecondElapse();

            timer.reset();
            st.execute("select * from " + table + " where " + columns[0] + "="
                    + (2500000 + batchSize - 1));
            double insertSelectTime = timer.getSecondElapse();

            timer.reset();
            st.execute("delete from " + table + " where " + columns[0]
                    + ">=2500000");
            double deleteTime = timer.getSecondElapse();

            timer.reset();
            st.execute("select * from " + table + " where " + columns[0] + "="
                    + 2500000);
            double deleteSelectTime = timer.getSecondElapse();

            timer.reset();
            if (n > 0)
                st.execute("drop index tpcds.indexr1");
            double dropIndexTime = timer.getSecondElapse();

            Rt.np(n + "\t" + createIndexTime + "\t" + insertTime + "\t"
                    + insertSelectTime + "\t" + deleteTime + "\t"
                    + deleteSelectTime + "\t" + dropIndexTime + "\t" + sb);
        }
        System.exit(0);
    }

    public IndexUpdateTest() throws Exception {
        Environment en = Environment.getInstance();
        en.setProperty("username", "db2inst1");
        en.setProperty("password", "db2inst1admin");
        en.setProperty("workloads.dir", "resources/workloads/db2");
        DatabaseSystem test = null;
        String dbName = "test";
        en.setProperty("jdbc.url", "jdbc:db2://localhost:50000/" + dbName);
        test = newDatabaseSystem(en);
        // indexUpdateCostTest(test);
        realIndexUpdateCostTest(test);
        if (test != null)
            test.getConnection().close();
    }

    public static void main(String[] args) throws Exception {
        new IndexUpdateTest();
    }

}
