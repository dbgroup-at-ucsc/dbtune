package edu.ucsc.dbtune.viz;

import java.awt.Color;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import javax.swing.BoxLayout;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import com.google.common.collect.Sets;

import edu.ucsc.dbtune.advisor.RecommendationStatistics;
import edu.ucsc.dbtune.advisor.wfit.WFITRecommendationStatistics;
import edu.ucsc.dbtune.metadata.Index;

import static edu.ucsc.dbtune.util.MetadataUtils.transitionCost;

/**
 * A JFrame that displays the partitions of a set of indexes.
 *
 * @author Ivo Jimenez
 */
public class WFITStatisticsTable extends AbstractVisualizer
{
    private String[] columnNames;
    private JFrame frame;

    /**
     */
    public WFITStatisticsTable()
    {
        columnNames = new String[5];

        columnNames[0] = "PARTITION";
        columnNames[1] = "STATE";
        columnNames[2] = "INDEXES";
        columnNames[3] = "SCORE";
        columnNames[4] = "TRANSITION COST";

        frame = new JFrame();

        frame.setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);

        frame.getContentPane().setLayout(new BoxLayout(frame.getContentPane(), BoxLayout.Y_AXIS));

        frame.setTitle("DBTune");
        frame.setBackground(Color.gray);
        frame.setSize(600, 400);
        frame.pack();

        stats = new ArrayList<RecommendationStatistics>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void show()
    {
        frame.setVisible(true);
    }

    /**
     * {@inheritDoc}
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

        if (!(stats.get(0) instanceof WFITRecommendationStatistics))
            throw new RuntimeException(
                    "Expecting WFIT-specific recommendation statistics");

        RecommendationStatistics.Entry e = stats.get(0).getLastEntry();
        Set<Set<Index>> partitions = e.getCandidatePartitioning();
        Set<Index> previousState = e.getPreviousRecommendation();
        Map<Set<Index>, Double> wfScores =
            ((WFITRecommendationStatistics.Entry) e).getWorkFunctionScores();

        int partitionNumber = 1;
        for (Set<Index> partition : partitions)
            frame.getContentPane().add(
                    new JScrollPane(
                        newTable(partitionNumber++, partition, wfScores, previousState)));

        frame.repaint();
    }

    /**
     * Creates a table containing the given indexes.
     *
     * @param partitionNumber
     *      the partition being displayed
     * @param indexes
     *      set of indexes inside the partition
     * @param wf
     *      work function scores
     * @param previousState
     *      previous state
     * @return
     *      the table
     */
    private JTable newTable(
            int partitionNumber,
            Set<Index> indexes,
            Map<Set<Index>, Double> wf,
            Set<Index> previousState)
    {
        String[][] dataValues = new String[Sets.powerSet(indexes).size()][];

        int state = 0;

        for (Set<Index> subset : Sets.powerSet(indexes))
            dataValues[state++] =
                newRow(partitionNumber, state, subset, wf.get(subset), previousState);
            
        return new JTable(dataValues, columnNames);
    }

    /**
     * @param partitionNumber
     *      the partition being displayed
     * @param stateNumber
     *      the partition being displayed
     * @param subset
     *      subset that the row corresponds to
     * @param wfScore
     *      score of the subset
     * @param previousRecommendation
     *      previous state
     * @return
     *      an array of strings, where each corresponds to an attribute of the index
     */
    private String[] newRow(
            int partitionNumber,
            int stateNumber,
            Set<Index> subset,
            double wfScore,
            Set<Index> previousRecommendation)
    {
        String[] row = new String[5];

        row[0] = partitionNumber + "";
        row[1] = stateNumber + "";
        row[2] = subset + "";
        row[3] = wfScore + "";
        row[4] = transitionCost(previousRecommendation, subset) + "";

        columnNames[0] = "PARTITION";
        columnNames[1] = "STATE";
        columnNames[2] = "INDEXES";
        columnNames[3] = "SCORE";
        columnNames[4] = "TRANSITION COST";

        return row;
    }
}
