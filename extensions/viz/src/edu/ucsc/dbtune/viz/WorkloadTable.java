package edu.ucsc.dbtune.viz;

import java.awt.Color;

import javax.swing.BoxLayout;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import edu.ucsc.dbtune.advisor.RecommendationStatistics;

import edu.ucsc.dbtune.advisor.wfit.WFITRecommendationStatistics;

import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.workload.Workload;

/**
 * A JFrame that displays the partitions of a set of indexes.
 *
 * @author Ivo Jimenez
 */
public class WorkloadTable extends AbstractVisualizer
{
    private String[] columnNames;
    private String[][] dataValues;

    /**
     */
    public WorkloadTable()
    {
        columnNames = new String[2];

        columnNames[0] = "ID";
        columnNames[1] = "SQL";

        frame = new JFrame();

        frame.setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);

        frame.getContentPane().setLayout(new BoxLayout(frame.getContentPane(), BoxLayout.Y_AXIS));

        frame.setTitle("DBTune");
        frame.setBackground(Color.gray);
        frame.setSize(600, 400);
        frame.pack();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateContent()
    {
        if (stats.size() < 1)
            return;

        if (stats.size() > 1)
            throw new RuntimeException(
                    "Can only display partition for one instance of an algorithm");

        if (!(stats.get(0) instanceof WFITRecommendationStatistics))
            throw new RuntimeException(
                    "Expecting WFIT-specific recommendation statistics");

        if (stats.get(0).size() == 0)
            return;

        RecommendationStatistics.Entry e = stats.get(0).getLastEntry();

        if (e.getSql().getPosition() > dataValues.length)
            throw new RuntimeException("Wrong number of statement for the given workload");

        if (e.getSql().getPosition() > 1) {
            // remove mark from previous statement
            String previous = dataValues[e.getSql().getPosition() - 2][0];
            dataValues[e.getSql().getPosition() - 2][0] = previous.substring(3, previous.length());
        }

        String sqlText = dataValues[e.getSql().getPosition() - 1][0];
        dataValues[e.getSql().getPosition() - 1][0] = ">> " + sqlText;

        frame.getContentPane().removeAll();
        frame.getContentPane().add(new JScrollPane(new JTable(dataValues, columnNames)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setWorkload(Workload wl)
    {
        dataValues = new String[wl.size()][];

        for (SQLStatement sql : wl)
            dataValues[sql.getPosition() - 1] = newRow(sql);

        frame.getContentPane().add(new JScrollPane(new JTable(dataValues, columnNames)));
    }

    /**
     * @param sql
     *      SQL object
     * @return
     *      an array of strings
     */
    private String[] newRow(SQLStatement sql)
    {
        String[] row = new String[2];

        row[0] = sql.getPosition() + "";
        row[1] = sql.getSQL();

        return row;
    }
}
