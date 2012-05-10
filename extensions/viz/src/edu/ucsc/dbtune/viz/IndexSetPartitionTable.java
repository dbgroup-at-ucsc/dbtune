package edu.ucsc.dbtune.viz;

import java.awt.Color;

import java.util.Set;

import javax.swing.BoxLayout;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import edu.ucsc.dbtune.metadata.Index;

import static edu.ucsc.dbtune.util.MetadataUtils.getColumnListString;

/**
 * A JFrame that displays the partitions of a set of indexes.
 *
 * @author Ivo Jimenez
 */
public class IndexSetPartitionTable extends JFrame
{
    private static final long serialVersionUID = 0L;
    private String[] columnNames;

    /**
     */
    public IndexSetPartitionTable()
    {
        columnNames = new String[4];

        columnNames[0] = "ID";
        columnNames[1] = "NAME";
        columnNames[2] = "TABLE";
        columnNames[3] = "COLUMNS";

        setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);

        getContentPane().setLayout(new BoxLayout(getContentPane(), BoxLayout.Y_AXIS));

        setTitle("DBTune");
        setSize(300, 200);
        setBackground(Color.gray);
    }

    /**
     * @param indexPartition
     *      the partition to display in the table
     */
    public void setPartition(Set<Set<Index>> indexPartition)
    {
        getContentPane().removeAll();

        for (Set<Index> partition : indexPartition)
            getContentPane().add(new JScrollPane(newTable(partition)));

        this.repaint();
        this.pack();
        this.setVisible(true);
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
