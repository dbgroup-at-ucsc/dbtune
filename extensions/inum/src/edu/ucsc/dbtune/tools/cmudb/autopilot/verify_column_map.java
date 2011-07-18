package edu.ucsc.dbtune.tools.cmudb.autopilot;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Iterator;
import java.io.IOException;
import java.io.FileInputStream;

/**
 * Created by IntelliJ IDEA.
 * User: Dash
 * Date: Apr 27, 2007
 * Time: 2:18:43 PM
 * To change this template use File | Settings | File Templates.
 */
public class verify_column_map {
    public static void main(String[] args) {
        String prefix = args.length >= 1 ? args[0] : "column.properties";

        autopilot ap = new autopilot();

        try {
            Connection conn;
            ap.init_database();
            conn = ap.getConnection();
            try {
                verifyColumnMap(prefix, conn);
            } finally {
                conn.close();
            }
        }
        catch (Exception E) {
            System.out.println(E.getMessage());
            E.printStackTrace();
        }
    }

    private static void verifyColumnMap(String prefix, Connection conn) throws IOException, SQLException {
        Properties props = new Properties();
        props.load(new FileInputStream(prefix));

        Statement stmt = conn.createStatement();
        for (Iterator<Object> iterator = props.keySet().iterator(); iterator.hasNext();) {
            String col = (String) iterator.next();
            String table = ((String) props.get(col)).toUpperCase();

            String sql = "select c.name, o.name from sysobjects o, syscolumns c where c.id = o.id and upper(c.name) = '" + col + "' and upper(o.name) = '" + table +"'";
            ResultSet rs = stmt.executeQuery(sql);
            if(!rs.next()) {
                System.err.println("invalid mapping: " + col + "->" + table);
            }
        }
    }

}
