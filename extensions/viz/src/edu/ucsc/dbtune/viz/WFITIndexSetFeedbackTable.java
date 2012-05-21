package edu.ucsc.dbtune.viz;

import java.awt.Color;

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
        columnNames = new String[8];

        columnNames[0] = "NAME";
        columnNames[1] = "TABLE";
        columnNames[2] = "COLUMNS";
        columnNames[3] = "SIZE";
        columnNames[4] = "COST";
        columnNames[5] = "BENEFIT";
        columnNames[6] = "RECOMMENDED";
        columnNames[7] = "OPTIMAL";

        frame = new JFrame();

        frame.setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);
        frame.getContentPane().setLayout(new BoxLayout(frame.getContentPane(), BoxLayout.Y_AXIS));
        frame.setTitle("DBTune");
        frame.setBackground(Color.gray);
        frame.setSize(600, 400);
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

        String[] row = new String[8];

        row[0] = index.getName();
        row[1] = index.getTable() + "";
        row[2] = getColumnListString(index);
        row[3] = index.getBytes() / (1024 * 1024) + "";
        row[4] = index.getCreationCost() + "";
        row[5] = wfEntry.getBenefits().get(index) + "";
        row[6] = wfEntry.getRecommendation().contains(index) ? "Y" : "N";
        row[7] =
            wfEntry.getUsefulness().get(index) != null &&
            wfEntry.getUsefulness().get(index) ? "Y" : "N";

        return row;
    }
}
