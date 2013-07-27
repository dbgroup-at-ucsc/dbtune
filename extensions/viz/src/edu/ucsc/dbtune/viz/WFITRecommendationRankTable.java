package edu.ucsc.dbtune.viz;

import java.awt.Color;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import javax.swing.BoxLayout;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import edu.ucsc.dbtune.advisor.wfit.WFIT;

import edu.ucsc.dbtune.metadata.Index;

import static edu.ucsc.dbtune.util.MetadataUtils.getNameListString;

/**
 * A JFrame that displays the partitions of a set of indexes.
 *
 * @author Ivo Jimenez
 */
public class WFITRecommendationRankTable extends SwingVisualizer
{
    static final long serialVersionUID = 0;
    protected String[] columnNames;

    /**
     * constructor.
     *
     * @param advisor
     *      the advisor
     */
    public WFITRecommendationRankTable(WFIT advisor)
    {
        super(advisor);

        columnNames = new String[3];

        columnNames[0] = "RANK";
        columnNames[1] = "INDEXES";
        columnNames[2] = "SCORE";

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
    public void updateContent() throws Exception
    {
        WFIT wfitAdv = (WFIT) advisor;

        getContentPane().removeAll();

        Set<Map.Entry<Set<Index>, Double>> scores = wfitAdv.getWorkFunctionScores();

        String[][] dataValues = new String[scores.size()][];

        int i = 0;

        for (Map.Entry<Set<Index>, Double> score : scores) {
            String[] row = new String[3];

            row[0] = (i + 1) + "";
            row[1] = getNameListString(new ArrayList<Index>(score.getKey()));
            row[2] = score.getValue() + "";

            dataValues[i++] = row;
        }

        getContentPane().add(new JScrollPane(new JTable(dataValues, columnNames)));
    }
}
