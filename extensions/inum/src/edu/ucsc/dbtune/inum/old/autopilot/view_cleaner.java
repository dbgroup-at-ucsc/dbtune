package edu.ucsc.dbtune.inum.old.autopilot;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Debabrata Dash
 * Date: Mar 15, 2006
 * Time: 4:10:15 PM
 * The file is a copyright of CMU, all rights reserved.
 */
public class view_cleaner {
    public static void main(String[] args) {
        String prefix = args.length >= 1 ? args[0] : "idxdesigner_mv";

        autopilot ap = new autopilot();

        try {
            Connection conn;
            ap.init_database();
            conn = ap.getConnection();
            try {
                cleanViews(prefix, conn);
            } finally {
                conn.close();
            }
        }
        catch (Exception E) {
            System.out.println(E.getMessage());
            E.printStackTrace();

        }
    }

    private static void cleanViews(String prefix, Connection conn) throws SQLException {
        List idxes = new ArrayList();
        String query = "select o.name from sysobjects o where name like '" + prefix + "%' and type = 'V'";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
            String idxName = rs.getString(1);
            idxes.add(idxName);
        }
        rs.close();

        for (int i = 0; i < idxes.size(); i++) {
            String idxName = (String) idxes.get(i);
            String update = "drop view " + idxName;

            System.out.println(update);
            Statement stmt1 = conn.createStatement();
            stmt1.execute(update);
            stmt1.close();
        }
    }
}