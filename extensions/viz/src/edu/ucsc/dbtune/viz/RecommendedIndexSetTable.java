package edu.ucsc.dbtune.viz;

import java.awt.Color;

import java.util.Set;

import javax.swing.BoxLayout;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import edu.ucsc.dbtune.advisor.RecommendationStatistics;
import edu.ucsc.dbtune.advisor.WorkloadObserverAdvisor;

import edu.ucsc.dbtune.metadata.Index;

import static edu.ucsc.dbtune.util.MetadataUtils.getColumnListString;

/**
 * A JFrame that displays the partitions of a set of indexes.
 *
 * @author Ivo Jimenez
 */
public class RecommendedIndexSetTable extends SwingVisualizer
{
    static final long serialVersionUID = 0;
    protected String[] columnNames;

    /**
     * constructor.
     *
     * @param advisor
     *      the advisor
     */
    public RecommendedIndexSetTable(WorkloadObserverAdvisor advisor)
    {
        super(advisor);

        columnNames = new String[3];

        columnNames[0] = "NAME";
        columnNames[1] = "TABLE";
        columnNames[2] = "COLUMNS";

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
        RecommendationStatistics stats = advisor.getRecommendationStatistics();

        getContentPane().removeAll();

        RecommendationStatistics.Entry e = stats.getLastEntry();
        getContentPane().add(new JScrollPane(newTable(e.getRecommendation())));
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
    protected JTable newTable(Set<Index> indexes)
    {
        String[][] dataValues = new String[indexes.size()][];

        int i = 0;

        for (Index index : indexes)
            dataValues[i++] = newRow(index);

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
    protected String[] newRow(Index index)
    {
        String[] row = new String[3];

        row[0] = index.getName();
        row[1] = index.getTable() + "";
        row[2] = getColumnListString(index);

        return row;
    }
}

