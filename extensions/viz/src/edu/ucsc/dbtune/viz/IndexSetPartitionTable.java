package edu.ucsc.dbtune.viz;

import java.awt.Color;

import java.util.ArrayList;
import java.util.Set;

import javax.swing.BoxLayout;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import edu.ucsc.dbtune.advisor.RecommendationStatistics;

import edu.ucsc.dbtune.metadata.Index;

import static edu.ucsc.dbtune.util.MetadataUtils.getColumnListString;

/**
 * A JFrame that displays the partitions of a set of indexes.
 *
 * @author Ivo Jimenez
 */
public class IndexSetPartitionTable extends AbstractVisualizer
{
    private String[] columnNames;
    private JFrame frame;

    /**
     */
    public IndexSetPartitionTable()
    {
        stats = new ArrayList<RecommendationStatistics>();
        frame = new JFrame();
        columnNames = new String[5];

        columnNames[0] = "NAME";
        columnNames[1] = "TABLE";
        columnNames[2] = "COLUMNS";
        columnNames[3] = "BENEFIT";
        columnNames[4] = "OPTIMAL RECOMMENDED";

        frame.setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);
        frame.getContentPane().setLayout(new BoxLayout(frame.getContentPane(), BoxLayout.Y_AXIS));
        frame.setTitle("DBTune");
        frame.setBackground(Color.gray);
    }

    /**
     */
    @Override
    public void refresh()
    {
        frame.getContentPane().removeAll();

        if (stats.size() < 1)
            return;

        if (stats.size() > 1)
            throw new RuntimeException(
                "Can only display partition for one instance of an algorithm");

        RecommendationStatistics.Entry e = stats.get(0).getLastEntry();
        Set<Set<Index>> partitions = e.getCandidatePartitioning();

        for (Set<Index> partition : partitions)
            frame.getContentPane().add(new JScrollPane(newTable(e, partition)));

        frame.repaint();
        frame.pack();
        frame.setSize(600, 400);
        frame.setVisible(true);
    }

    /**
     * Creates a table containing the given indexes.
     *
     * @param indexes
     *      set to be displayed in the table
     * @param e
     *      entry
     * @return
     *      the table
     */
    private JTable newTable(RecommendationStatistics.Entry e, Set<Index> indexes)
    {
        String[][] dataValues = new String[indexes.size()][4];

        int i = 0;

        for (Index index : indexes)
            dataValues[i++] = newRow(e, index);
            
        return new JTable(dataValues, columnNames);
    }

    /**
     * @param index
     *      index for which a new row is created
     * @param e
     *      entry
     * @return
     *      an array of strings, where each corresponds to an attribute of the index
     */
    protected String[] newRow(RecommendationStatistics.Entry e, Index index)
    {
        String[] row = new String[5];

        row[0] = index.getName();
        row[1] = index.getTable() + "";
        row[2] = getColumnListString(index);
        row[3] = e.getBenefits().get(index) + "";
        row[4] = e.getRecommendation().contains(index) ? "Y" : "N";

        return row;
    }
}
