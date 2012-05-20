package edu.ucsc.dbtune.viz;

import java.awt.Color;

import java.util.ArrayList;

import javax.swing.BoxLayout;
import javax.swing.JFrame;

import edu.ucsc.dbtune.advisor.RecommendationStatistics;

import edu.ucsc.dbtune.advisor.wfit.WFITRecommendationStatistics;

import edu.ucsc.dbtune.metadata.Index;

import static edu.ucsc.dbtune.util.MetadataUtils.getColumnListString;

/**
 * A JFrame that displays the partitions of a set of indexes.
 *
 * @author Ivo Jimenez
 */
public class WFITIndexSetFeedbackTable extends IndexSetPartitionTable
{
    /**
     * constructor.
     */
    public WFITIndexSetFeedbackTable()
    {
        stats = new ArrayList<RecommendationStatistics>();
        frame = new JFrame();
        columnNames = new String[6];

        columnNames[0] = "NAME";
        columnNames[1] = "TABLE";
        columnNames[2] = "COLUMNS";
        columnNames[3] = "BENEFIT";
        columnNames[4] = "RECOMMENDED";
        columnNames[5] = "OPTIMAL";

        frame.setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);
        frame.getContentPane().setLayout(new BoxLayout(frame.getContentPane(), BoxLayout.Y_AXIS));
        frame.setTitle("DBTune");
        frame.setBackground(Color.gray);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String[] newRow(RecommendationStatistics.Entry e, Index index)
    {
        if (!(e instanceof WFITRecommendationStatistics.Entry))
            throw new RuntimeException("Expecting statistics of type WFIT");

        WFITRecommendationStatistics.Entry wfEntry = (WFITRecommendationStatistics.Entry) e;

        String[] row = new String[6];

        row[0] = index.getName();
        row[1] = index.getTable() + "";
        row[2] = getColumnListString(index);
        row[3] = wfEntry.getBenefits().get(index) + "";
        row[4] = wfEntry.getRecommendation().contains(index) ? "Y" : "N";
        row[5] = wfEntry.getUsefulness().get(index) ? "Y" : "N";

        return row;
    }
}
