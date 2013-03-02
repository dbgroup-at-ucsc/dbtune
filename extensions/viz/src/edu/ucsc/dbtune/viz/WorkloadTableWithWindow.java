package edu.ucsc.dbtune.viz;

import java.awt.Color;
import java.awt.Component;

import java.sql.SQLException;

import java.util.List;
import java.util.Vector;

import javax.swing.BoxLayout;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;

import edu.ucsc.dbtune.advisor.RecommendationStatistics;
import edu.ucsc.dbtune.advisor.WindowingAdvisor;
import edu.ucsc.dbtune.advisor.WorkloadObserverAdvisor;

import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * A JFrame that displays the statements contained in a workload and marks the one that are being 
 * considered by the advisor.
 *
 * @author Ivo Jimenez
 */
public class WorkloadTableWithWindow extends SwingVisualizer
{
    static final long serialVersionUID = 0;
    private Vector<Object> columns;
    private JTable table;

    /**
     * Constructs a workload table.
     *
     * @param advisor
     *      advisor being observed
     * @throws SQLException
     *      if the given advisor doesn't implement the {@link WindowingAdvisor} interface.
     */
    public WorkloadTableWithWindow(WorkloadObserverAdvisor advisor)
        throws SQLException
    {
        super(advisor);

        if (!(advisor instanceof WindowingAdvisor))
            throw new SQLException("Advisor should implement the WindowingAdvisor interface");

        setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);

        getContentPane().setLayout(new BoxLayout(getContentPane(), BoxLayout.Y_AXIS));

        setTitle("   Workload");
        setBackground(Color.gray);
        setSize(512, 372);
        setLocation(512, 24);
        pack();

        columns = new Vector<Object>();

        columns.add("");
        columns.add("SQL");

        table = new JTable(new DefaultTableModel(new Object[]{"", "SQL"}, 1000));
        table.setDefaultRenderer(Object.class, new CustomRenderer());
        table.getColumnModel().getColumn(0).setMinWidth(8);
        table.getColumnModel().getColumn(0).setMaxWidth(8);
        table.getColumnModel().getColumn(0).setPreferredWidth(8);
        getContentPane().add(new JScrollPane(table));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateContent() throws Exception
    {
        RecommendationStatistics stats = advisor.getRecommendationStatistics();
        List<SQLStatement> window = ((WindowingAdvisor) advisor).getWindow();

        DefaultTableModel model = (DefaultTableModel) table.getModel();

        //Object[][] dataVector = new Object[stats.size()][];
        @SuppressWarnings("unchecked")
        Vector<Vector<Object>> dataVector = (Vector<Vector<Object>>) model.getDataVector();

        dataVector.clear();

        for (RecommendationStatistics.Entry e : stats)
            dataVector.add(newRow(e.getSql(), window.contains(e.getSql())));
            //dataVector[e.getSql().getPosition() - 1] =
                //newRow(e.getSql(), window.contains(e.getSql()));

        table.getColumnModel().getColumn(0).setMinWidth(8);
        table.getColumnModel().getColumn(0).setMaxWidth(8);
        table.getColumnModel().getColumn(0).setPreferredWidth(8);

        //model.setDataVector(dataVector, columns);
        //model.setDataVector(dataVector, new Object[]{"", "SQL"});

        table.getColumnModel().getColumn(0).setMinWidth(8);
        table.getColumnModel().getColumn(0).setMaxWidth(8);
        table.getColumnModel().getColumn(0).setPreferredWidth(8);
    }

    /**
     * @param sql
     *      SQL object
     * @param isInWindow
     *      whether the statement is in the window or not
     * @return
     *      an array of strings
     */
    //private Object[] newRow(SQLStatement sql, boolean isInWindow)
    private Vector<Object> newRow(SQLStatement sql, boolean isInWindow)
    {
        //Object[] row = new Object[2];
        Vector<Object> row = new Vector<Object>();

        if (isInWindow)
            row.add("_");
        else
            row.add("");

        row.add(sql.getSQL());

        /*
        if (isInWindow)
            row[0] = "_";
        else
            row[0] = "";

        row[1] = sql.getSQL();
        */

        return row;
    }

    //CHECKSTYLE:OFF
    private class CustomRenderer extends DefaultTableCellRenderer
    {
        static final long serialVersionUID = 0;
        public Component getTableCellRendererComponent(JTable table, Object value, boolean 
                isSelected, boolean hasFocus, int row, int column)
        {
            Component c = super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);

            if (value == null)
                return c;

            if (value.toString().startsWith("_"))
                c.setBackground(new java.awt.Color(180, 55, 55));
            else
                c.setBackground(java.awt.Color.white);
            return c;
        }
    }
    //CHECKSTYLE:ON
}
