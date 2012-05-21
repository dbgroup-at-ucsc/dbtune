package edu.ucsc.dbtune.viz;

import java.awt.Color;

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

    /**
     */
    public WFITStatisticsTable()
    {
        columnNames = new String[6];

        columnNames[0] = "PARTITION";
        columnNames[1] = "STATE";
        columnNames[2] = "INDEXES";
        columnNames[3] = "WORK-FUNCTION VALUE";
        columnNames[4] = "UNDO COST";
        columnNames[5] = "SCORE";

        frame = new JFrame();

        frame.setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);

        frame.getContentPane().setLayout(new BoxLayout(frame.getContentPane(), BoxLayout.Y_AXIS));

        frame.setTitle("   WFIT Internal Statistics");
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
        frame.getContentPane().removeAll();

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
        Set<Set<Index>> partitions = e.getCandidatePartitioning();
        Set<Index> previousState = e.getPreviousRecommendation();
        Map<Set<Index>, Double> wfScores =
            ((WFITRecommendationStatistics.Entry) e).getWorkFunctionScores();

        int partitionNumber = 1;
        for (Set<Index> partition : partitions)
            frame.getContentPane().add(
                    new JScrollPane(
                        newTable(partitionNumber++, partition, wfScores, previousState)));
    }

    /**
     * Creates a table containing the given indexes.
     *
     * @param partitionNumber
     *      the partition being displayed
     * @param partition
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
            Set<Index> partition,
            Map<Set<Index>, Double> wf,
            Set<Index> previousState)
    {
        String[][] dataValues = new String[Sets.powerSet(partition).size()][];

        int state = 0;

        for (Set<Index> subset : Sets.powerSet(partition))
            dataValues[state++] =
                newRow(partitionNumber, state, subset, wf.get(subset),
                        Sets.intersection(previousState, partition));
            
        return new JTable(dataValues, columnNames);
    }

    /**
     * @param partitionNumber
     *      the partition being displayed
     * @param stateNumber
     *      the partition being displayed
     * @param subset
     *      subset that the row corresponds to
     * @param wfValue
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
            double wfValue,
            Set<Index> previousRecommendation)
    {
        String[] row = new String[6];

        double undoCost = transitionCost(subset, previousRecommendation);
        
        row[0] = partitionNumber + "";
        row[1] = stateNumber + "";
        row[2] = subset + "";
        row[3] = wfValue + "";
        row[4] = undoCost + "";
        row[5] = (wfValue + undoCost) + "";

        return row;
    }
}
