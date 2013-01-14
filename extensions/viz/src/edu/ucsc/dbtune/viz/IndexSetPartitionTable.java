package edu.ucsc.dbtune.viz;

import java.awt.Color;

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
public class IndexSetPartitionTable extends SwingVisualizer
{
    protected String[] columnNames;

    /**
     * constructor.
     */
    public IndexSetPartitionTable()
    {
        columnNames = new String[6];

        columnNames[0] = "NAME";
        columnNames[1] = "TABLE";
        columnNames[2] = "COLUMNS";
        //columnNames[3] = "SIZE";
        columnNames[3] = "CREATION COST";
        columnNames[4] = "BENEFIT";
        columnNames[5] = "RECOMMENDED";

        setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);
        getContentPane().setLayout(new BoxLayout(getContentPane(), BoxLayout.Y_AXIS));
        setTitle("   Indexes");
        setBackground(Color.gray);
        pack();
        setSize(511, 375);
        setLocation(0, 375);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateContent()
    {
        getContentPane().removeAll();

        if (stats.size() < 1)
            return;

        if (stats.size() > 1)
            throw new RuntimeException(
                "Can only display partition for one instance of an algorithm");

        if (stats.get(0).size() == 0)
            return;

        RecommendationStatistics.Entry e = stats.get(0).getLastEntry();
        Set<Set<Index>> partitions = e.getCandidatePartitioning();

        for (Set<Index> partition : partitions)
            getContentPane().add(new JScrollPane(newTable(e, partition)));
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
        String[][] dataValues = new String[indexes.size()][];

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
        String[] row = new String[6];

        row[0] = index.getName();
        row[1] = index.getTable() + "";
        row[2] = getColumnListString(index);
        //row[3] = index.getBytes() / (1024 * 1024) + "";
        row[3] = index.getCreationCost() + "";
        row[4] = e.getBenefits().get(index) == null ? "0.0" : (e.getBenefits().get(index) + "");
        row[5] = e.getRecommendation().contains(index) ? "Y" : "N";

        return row;
    }
}
