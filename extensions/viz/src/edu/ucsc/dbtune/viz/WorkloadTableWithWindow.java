package edu.ucsc.dbtune.viz;

import java.awt.Color;
import java.awt.Component;

import java.sql.SQLException;

import java.util.List;

import javax.swing.BoxLayout;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import javax.swing.table.DefaultTableCellRenderer;

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
    private String[] columnNames;
    private String[][] dataValues;

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

        columnNames = new String[2];

        columnNames[0] = "";
        columnNames[1] = "SQL";

        setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);

        getContentPane().setLayout(new BoxLayout(getContentPane(), BoxLayout.Y_AXIS));

        setTitle("   Workload");
        setBackground(Color.gray);
        setSize(512, 372);
        setLocation(512, 24);
        pack();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateContent() throws Exception
    {
        RecommendationStatistics stats = advisor.getRecommendationStatistics();
        List<SQLStatement> window = ((WindowingAdvisor) advisor).getWindow();

        dataValues = new String[stats.size()][];

        for (RecommendationStatistics.Entry e : stats)
            dataValues[e.getSql().getPosition() - 1] =
                newRow(e.getSql(), window.contains(e.getSql()));

        getContentPane().removeAll();
        JTable table = new JTable(dataValues, columnNames);
        table.setDefaultRenderer(Object.class, new CustomRenderer());
        table.getColumnModel().getColumn(0).setMinWidth(8);
        table.getColumnModel().getColumn(0).setMaxWidth(8);
        table.getColumnModel().getColumn(0).setPreferredWidth(8);
        getContentPane().add(new JScrollPane(table));
    }

    /**
     * @param sql
     *      SQL object
     * @param isInWindow
     *      whether the statement is in the window or not
     * @return
     *      an array of strings
     */
    private String[] newRow(SQLStatement sql, boolean isInWindow)
    {
        String[] row = new String[2];

        if (isInWindow)
            row[0] = "_";
        else
            row[0] = "";

        row[1] = sql.getSQL();

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

            if (value.toString().startsWith("_"))
                c.setBackground(new java.awt.Color(180, 55, 55));
            else
                c.setBackground(java.awt.Color.white);
            return c;
        }
    }
    //CHECKSTYLE:ON
}
