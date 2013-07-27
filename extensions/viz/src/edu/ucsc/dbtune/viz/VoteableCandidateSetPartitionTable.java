package edu.ucsc.dbtune.viz;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import java.sql.SQLException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.DefaultCellEditor;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.UIManager;

import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableCellRenderer;

import edu.ucsc.dbtune.DatabaseSystem;

import edu.ucsc.dbtune.advisor.RecommendationStatistics;
import edu.ucsc.dbtune.advisor.VoteableAdvisor;
import edu.ucsc.dbtune.advisor.WorkloadObserverAdvisor;

import edu.ucsc.dbtune.advisor.wfit.WFIT;

import edu.ucsc.dbtune.metadata.ColumnOrdering;
import edu.ucsc.dbtune.metadata.Index;

import static edu.ucsc.dbtune.util.MetadataUtils.getColumnListString;

/**
 * A JFrame that displays the partitions of a set of candidates, where each can be voted.
 *
 * @author Ivo Jimenez
 */
public class VoteableCandidateSetPartitionTable extends SwingVisualizer implements ActionListener
{
    static final long serialVersionUID = 0;
    protected String[] columnNames;
    protected Map<Integer, Boolean> rowToRecommended;
    protected JTextArea txtBox;

    /**
     * constructor.
     *
     * @param advisor
     *      the advisor
     * @throws SQLException
     *      if the given advisor is not an instance of {@link VoteableAdvisor}
     */
    public VoteableCandidateSetPartitionTable(WorkloadObserverAdvisor advisor)
        throws SQLException
    {
        super(advisor);

        if (!(advisor instanceof VoteableAdvisor))
            throw new SQLException("Advisor should implement the Voteable interface");

        columnNames = new String[8];

        columnNames[0] = "NAME";
        columnNames[1] = "TABLE";
        columnNames[2] = "COLUMNS";
        //columnNames[3] = "SIZE";
        columnNames[3] = "CREATION COST";
        columnNames[4] = "BENEFIT";
        columnNames[5] = "RECOMMENDED";
        columnNames[6] = "VOTE UP";
        columnNames[7] = "VOTE DOWN";

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
        rowToRecommended = new HashMap<Integer, Boolean>();

        //createIndexBox();

        RecommendationStatistics.Entry e = stats.getLastEntry();
        Set<Set<Index>> partitions = e.getCandidatePartitioning();

        for (Set<Index> partition : partitions)
            getContentPane().add(new JScrollPane(newTable(e, partition)));

        pack();
    }

    /**
     * creates.
     *
     * @throws Exception yeah
     */
    private void createIndexBox() throws Exception
    {
        JScrollPane pane = new JScrollPane();
        JPanel panel = new JPanel();
        //pane.setLayout(new BoxLayout(pane, BoxLayout.Y_AXIS));
        //pane.add(Box.createRigidArea(new Dimension(0, 127)));

        txtBox = new JTextArea();

        txtBox.setLineWrap(true);
        txtBox.setPreferredSize(new Dimension(100, 127));
        txtBox.setMaximumSize(new Dimension(100, 127));
        txtBox.setWrapStyleWord(true);
        txtBox.setAlignmentX(Component.CENTER_ALIGNMENT);
        txtBox.setBorder(BorderFactory.createEmptyBorder(8, 8, 8, 8));
        txtBox.setColumns(20);
        txtBox.setRows(5);

        JButton button = new JButton();
        button.setText("Create Index");
        button.setAlignmentX(Component.CENTER_ALIGNMENT);
        button.addActionListener(this);

        //pane.add(button);

        //CHECKSTYLE:OFF
        javax.swing.GroupLayout jPanel1Layout = new javax.swing.GroupLayout(panel);
        panel.setLayout(jPanel1Layout);
        jPanel1Layout.setHorizontalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, jPanel1Layout.createSequentialGroup()
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addComponent(pane, javax.swing.GroupLayout.PREFERRED_SIZE, 
                    javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(102, 102, 102))
            .addGroup(jPanel1Layout.createSequentialGroup()
                .addGap(105, 105, 105)
                .addComponent(button)
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );
        jPanel1Layout.setVerticalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel1Layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(pane, javax.swing.GroupLayout.PREFERRED_SIZE, 
                    javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addComponent(button)
                .addContainerGap(18, Short.MAX_VALUE))
        );

        //javax.swing.ScrollPaneLayout layout = new javax.swing.ScrollPaneLayout();
        //pane.setLayout(layout);
        //layout.addLayoutComponent("panel", panel);

        //CHECKSTYLE:ON

        //pane.add(panel);
        getContentPane().add(pane);
    }

    /**
     * actions.
     *
     * @param e the event
     */
    public void actionPerformed(ActionEvent e)
    {
        DatabaseSystem db = ((WFIT) advisor).getDatabase();
        try {
            db.newIndex(
                ColumnOrdering.newOrdering(
                    db.getCatalog(),
                    txtBox.getText()));
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
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
        Object[][] dataValues = new Object[indexes.size()][];

        int i = 0;

        for (Index index : indexes) {
            rowToRecommended.put(i, e.getRecommendation().contains(index));
            dataValues[i++] = newRow(e, index);
        }


        DefaultTableModel dm = new DefaultTableModel();
        dm.setDataVector(dataValues, columnNames);

        JTable table = new JTable(dm);

        table.setDefaultRenderer(Object.class, new DefaultTableCellRenderer()
        {
            static final long serialVersionUID = 0;
            @Override
            public Component getTableCellRendererComponent(
                JTable table, Object value, boolean isSelected, boolean hasFocus,
                int row, int column)
            {
                final Component c =
                    super.getTableCellRendererComponent(
                        table, value, isSelected, hasFocus, row, column);
                c.setBackground(rowToRecommended.get(row) ? Color.YELLOW : Color.WHITE);
                return c;
            }
        });

        table.getColumn("VOTE UP").setCellRenderer(new ButtonRenderer());
        table.getColumn("VOTE UP").setCellEditor(new ButtonEditor(new JCheckBox()));
        table.getColumn("VOTE DOWN").setCellRenderer(new ButtonRenderer());
        table.getColumn("VOTE DOWN").setCellEditor(new ButtonEditor(new JCheckBox()));

        return table;
    }

    /**
     * @param index
     *      index for which a new row is created
     * @param e
     *      entry
     * @return
     *      an array of strings, where each corresponds to an attribute of the index
     */
    protected Object[] newRow(RecommendationStatistics.Entry e, Index index)
    {
        Object[] row = new Object[9];

        row[0] = index.getName();
        row[1] = index.getTable() + "";
        row[2] = getColumnListString(index);
        //row[3] = index.getBytes() / (1024 * 1024) + "";
        row[3] = index.getCreationCost() + "";
        row[4] = e.getBenefits().get(index) == null ? "0.0" : (e.getBenefits().get(index) + "");
        row[5] = e.getRecommendation().contains(index) ? "Y" : "N";
        row[6] = new StringWithAttachment("Up", index, (VoteableAdvisor) advisor);
        row[7] = new StringWithAttachment("Down", index, (VoteableAdvisor) advisor);

        return row;
    }
}

// taken from: http://www.java2s.com/Code/Java/Swing-Components/ButtonTableExample.htm
//CHECKSTYLE:OFF
class StringWithAttachment {
    public Index index;
    public VoteableAdvisor advisor;
    public String value;

    public StringWithAttachment(String value, Index index, VoteableAdvisor advisor)
    {
        this.value = value;
        this.advisor = advisor;
        this.index = index;
    }

    @Override
    public String toString()
    {
        return value;
    }
}
class ButtonRenderer extends JButton implements TableCellRenderer {
    static final long serialVersionUID = 0;
    public ButtonRenderer() {
        setOpaque(true);
    }

    public Component getTableCellRendererComponent(JTable table, Object value,
            boolean isSelected, boolean hasFocus, int row, int column) {
        if (isSelected) {
            setForeground(table.getSelectionForeground());
            setBackground(table.getSelectionBackground());
        } else {
            setForeground(table.getForeground());
            setBackground(UIManager.getColor("Button.background"));
        }
        setText((value == null) ? "" : value.toString());
        return this;
    }
}

class ButtonEditor extends DefaultCellEditor {
    static final long serialVersionUID = 0;
    protected JButton button;

    private Object value;

    private boolean isPushed;

    public ButtonEditor(JCheckBox checkBox) {
        super(checkBox);
        button = new JButton();
        button.setOpaque(true);
        button.addActionListener(new ActionListener() {
                public void actionPerformed(ActionEvent e) {
                fireEditingStopped();
                }
                });
    }

    public Component getTableCellEditorComponent(JTable table, Object value,
            boolean isSelected, int row, int column) {
        if (isSelected) {
            button.setForeground(table.getSelectionForeground());
            button.setBackground(table.getSelectionBackground());
        } else {
            button.setForeground(table.getForeground());
            button.setBackground(table.getBackground());
        }
        this.value = value;
        button.setText(value + "");
        isPushed = true;
        return button;
    }

    public Object getCellEditorValue() {
        if (isPushed) {
            StringWithAttachment swa = (StringWithAttachment) value;

            try {
                if (swa.value.equals("Up"))
                    swa.advisor.voteUp(swa.index);
                else
                    swa.advisor.voteDown(swa.index);
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
        isPushed = false;
        return new String(value + "");
    }

    public boolean stopCellEditing() {
        isPushed = false;
        return super.stopCellEditing();
    }

    protected void fireEditingStopped() {
        super.fireEditingStopped();
    }
}
//CHECKSTYLE:ON
