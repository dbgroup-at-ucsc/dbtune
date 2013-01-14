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
public class WFITStatisticsTable extends SwingVisualizer
{
    private String[] columnNames;
    static final long serialVersionUID = 0;

    /**
     */
    public WFITStatisticsTable()
    {
        columnNames = new String[6];

        columnNames[0] = "PARTITION";
        columnNames[1] = "STATE";
        columnNames[2] = "INDEXES";
        columnNames[3] = "OPTIMAL PATH COST";
        columnNames[4] = "UNDO COST";
        columnNames[5] = "SCORE";

        setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);

        getContentPane().setLayout(new BoxLayout(getContentPane(), BoxLayout.Y_AXIS));

        setTitle("   WFIT Internal Statistics");
        setBackground(Color.gray);
        setSize(511, 372);
        setLocation(0, 375);
        pack();
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

        if (!(stats.get(0) instanceof WFITRecommendationStatistics))
            throw new RuntimeException(
                    "Expecting WFIT-specific recommendation statistics");

        if (stats.get(0).size() == 0)
            return;

        RecommendationStatistics.Entry e = stats.get(0).getLastEntry();
        Set<Set<Index>> partitions = e.getCandidatePartitioning();
        Set<Index> currentState = e.getRecommendation();
        Set<Index> previousState = e.getPreviousRecommendation();
        Map<Set<Index>, Double> wfScores =
            ((WFITRecommendationStatistics.Entry) e).getWorkFunctionScores();

        int partitionNumber = 1;
        for (Set<Index> partition : partitions)
            getContentPane().add(
                new JScrollPane(
                    newTable(
                        partitionNumber++, partition, wfScores, currentState, previousState)));
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
     * @param currentState
     *      current state
     * @param previousState
     *      previous state
     * @return
     *      the table
     */
    private JTable newTable(
            int partitionNumber,
            Set<Index> partition,
            Map<Set<Index>, Double> wf,
            Set<Index> currentState,
            Set<Index> previousState)
    {
        String[][] dataValues = new String[Sets.powerSet(partition).size()][];

        int state = 0;

        for (Set<Index> subset : Sets.powerSet(partition))
            dataValues[state++] =
                newRow(partitionNumber, state, subset, wf.get(subset),
                        Sets.intersection(currentState, partition),
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
     * @param currentRecommendationForPartition
     *      the current recommendation for the partition
     * @param previousRecommendationForPartition
     *      the previous recommendation for the corresponding partition
     * @return
     *      an array of strings, where each corresponds to an attribute of the index
     */
    private String[] newRow(
            int partitionNumber,
            int stateNumber,
            Set<Index> subset,
            double wfValue,
            Set<Index> currentRecommendationForPartition,
            Set<Index> previousRecommendationForPartition)
    {
        String[] row = new String[6];

        double undoCost = transitionCost(subset, previousRecommendationForPartition);

        if (currentRecommendationForPartition.equals(subset))
            row[0] = ">> ";
        else
            row[0] = "";

        row[0] += partitionNumber + "";

        row[1] = stateNumber + "";
        row[2] = subset + "";
        row[3] = wfValue + "";
        row[4] = undoCost + "";
        row[5] = (wfValue + undoCost) + "";

        return row;
    }
}
