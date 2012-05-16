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
        columnNames = new String[4];

        columnNames[0] = "ID";
        columnNames[1] = "NAME";
        columnNames[2] = "TABLE";
        columnNames[3] = "COLUMNS";

        frame = new JFrame();

        frame.setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);

        frame.getContentPane().setLayout(new BoxLayout(frame.getContentPane(), BoxLayout.Y_AXIS));

        frame.setTitle("DBTune");
        frame.setSize(300, 200);
        frame.setBackground(Color.gray);

        stats = new ArrayList<RecommendationStatistics>();
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

        Set<Set<Index>> partitions = stats.get(0).getLastEntry().getCandidatePartitioning();

        for (Set<Index> partition : partitions)
            frame.getContentPane().add(new JScrollPane(newTable(partition)));

        frame.repaint();
        frame.pack();
        frame.setSize(300, 200);
        frame.setVisible(true);
    }

    /**
     * Creates a table containing the given indexes.
     *
     * @param indexes
     *      set to be displayed in the table
     * @return
     *      the table
     */
    private JTable newTable(Set<Index> indexes)
    {
        String[][] dataValues = new String[indexes.size()][4];

        int i = 0;

        for (Index index : indexes)
            dataValues[i++] = newRow(index);
            
        return new JTable(dataValues, columnNames);
    }

    /**
     * @param index
     *      index for which a new row is created
     * @return
     *      an array of strings, where each corresponds to an attribute of the index
     */
    private String[] newRow(Index index)
    {
        String[] row = new String[4];

        row[0] = index.getId() + "";
        row[1] = index.getName();
        row[2] = index.getTable() + "";
        row[3] = getColumnListString(index);

        return row;
    }
}
