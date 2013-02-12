package edu.ucsc.dbtune.viz;

import java.awt.Color;

import javax.swing.BoxLayout;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import edu.ucsc.dbtune.advisor.RecommendationStatistics;
import edu.ucsc.dbtune.advisor.WorkloadObserverAdvisor;

import edu.ucsc.dbtune.workload.SQLStatement;

/**
 * A JFrame that displays the statements contained in a workload.
 *
 * @author Ivo Jimenez
 */
public class WorkloadTable extends SwingVisualizer
{
    static final long serialVersionUID = 0;
    private String[] columnNames;
    private String[][] dataValues;

    /**
     * Constructs a workload table.
     *
     * @param advisor
     *      advisor being observed
     */
    public WorkloadTable(WorkloadObserverAdvisor advisor)
    {
        super(advisor);

        columnNames = new String[1];

        columnNames[0] = "SQL";
        //columnNames[1] = "SQL";

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

        dataValues = new String[stats.size()][];

        for (RecommendationStatistics.Entry e : stats)
            dataValues[e.getSql().getPosition() - 1] = newRow(e.getSql());

        getContentPane().removeAll();
        JTable table = new JTable(dataValues, columnNames);
        table.getColumnModel().getColumn(0).setPreferredWidth(30);
        getContentPane().add(new JScrollPane(table));
    }

    /**
     * @param sql
     *      SQL object
     * @return
     *      an array of strings
     */
    private String[] newRow(SQLStatement sql)
    {
        String[] row = new String[1];

        //row[0] = sql.getPosition() + "";
        row[0] = sql.getSQL();

        return row;
    }
}
