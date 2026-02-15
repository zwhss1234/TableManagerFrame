import javax.swing.*;
import javax.swing.table.DefaultTableModel;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridLayout;

import java.sql.SQLException;
import java.util.*;

public class TableManagerFrame extends JFrame {

    private final TableService service = new TableService();

    private final JComboBox<String> tableCombo = new JComboBox<>();
    private final JComboBox<Integer> limitCombo = new JComboBox<>(new Integer[]{200, 500, 2000, 10000});
    private final JButton loadBtn = new JButton("Load");

    private final JButton insertBtn = new JButton("INSERT");
    private final JButton updateBtn = new JButton("UPDATE");
    private final JButton deleteBtn = new JButton("DELETE");
    
    private final JButton exportBtn = new JButton("EXPORT CSV");

    private final JTable table = new JTable();
    private DefaultTableModel model;

    private String currentTable = null;

    private List<String> currentPkCols = Collections.emptyList();
    private Set<String> currentAutoCols = Collections.emptySet();
    private Map<String, TableService.FK> currentFks = Collections.emptyMap();
    private Map<String, TableService.EnumInfo> currentEnums = Collections.emptyMap();

    public TableManagerFrame() {
        super("DBA Table Manager");
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        setSize(1100, 650);
        setLocationRelativeTo(null);

        JPanel top = new JPanel(new FlowLayout(FlowLayout.LEFT));
        top.add(new JLabel("Table:"));
        top.add(tableCombo);
        top.add(new JLabel("Limit:"));
        top.add(limitCombo);
       // top.add(loadBtn);

        top.add(Box.createHorizontalStrut(30));
        top.add(insertBtn);
        top.add(updateBtn);
        top.add(deleteBtn);

        add(top, BorderLayout.NORTH);
        add(new JScrollPane(table), BorderLayout.CENTER);

        insertBtn.setEnabled(false);
        updateBtn.setEnabled(false);
        deleteBtn.setEnabled(false);

        loadBtn.addActionListener(e -> loadSelectedTable());
        tableCombo.addActionListener(e -> {
            if (tableCombo.getSelectedItem() != null) loadSelectedTable();
        });

        insertBtn.addActionListener(e -> doInsert());
        updateBtn.addActionListener(e -> doUpdate());
        deleteBtn.addActionListener(e -> doDelete());
        
        top.add(exportBtn);
        exportBtn.addActionListener(e -> doExport());

        SwingUtilities.invokeLater(this::loadTables);
    }

    private void loadTables() {
        try {
            tableCombo.removeAllItems();
            for (String t : service.listTables()) tableCombo.addItem(t);
        } catch (SQLException ex) {
            showError(ex);
        }
    }

    private void loadSelectedTable() {
        String t = (String) tableCombo.getSelectedItem();
        if (t == null) return;

        try {
            currentTable = t;

            currentPkCols = service.getPrimaryKeyColumns(t);
            currentAutoCols = service.getAutoIncrementColumns(t);
            currentFks = service.getForeignKeys(t);
            currentEnums = service.getEnumColumns(t);

            int limit = (Integer) limitCombo.getSelectedItem();
            TableService.TableData data = service.loadTable(t, limit);

            model = new DefaultTableModel(data.columns.toArray(), 0) {
                @Override public boolean isCellEditable(int row, int col) {
                    return false;
                }
            };

            for (Object[] r : data.rows) model.addRow(r);
            table.setModel(model);

            insertBtn.setEnabled(true);
            updateBtn.setEnabled(true);
            deleteBtn.setEnabled(true);

        } catch (SQLException ex) {
            showError(ex);
        }
    }

    private void doInsert() {
        if (currentTable == null) return;

        Map<String, Object> values = promptForRowValues("INSERT into " + currentTable, null);
        if (values == null) return;

        try {
            service.insertRow(currentTable, values);
            loadSelectedTable();
        } catch (SQLException ex) {
            showError(ex);
        }
    }

    private void doUpdate() {
        if (currentTable == null) return;

        int row = table.getSelectedRow();
        if (row < 0) {
            JOptionPane.showMessageDialog(this, "Select a row first.");
            return;
        }
        if (currentPkCols.isEmpty()) {
            JOptionPane.showMessageDialog(this, "No PRIMARY KEY found. UPDATE not supported for this table.");
            return;
        }

        Map<String, Object> currentRow = rowToMap(row);
        Map<String, Object> newValues = promptForRowValues("UPDATE " + currentTable, currentRow);
        if (newValues == null) return;

        Map<String, Object> pkValues = new HashMap<>();
        for (String pk : currentPkCols) pkValues.put(pk, currentRow.get(pk));

        try {
            service.updateRowByPk(currentTable, currentPkCols, newValues, pkValues);
            loadSelectedTable();
        } catch (SQLException ex) {
            showError(ex);
        }
    }

    private void doDelete() {
        if (currentTable == null) return;

        int row = table.getSelectedRow();
        if (row < 0) {
            JOptionPane.showMessageDialog(this, "Select a row first.");
            return;
        }
        if (currentPkCols.isEmpty()) {
            JOptionPane.showMessageDialog(this, "No PRIMARY KEY found. DELETE not supported for this table.");
            return;
        }

        int ans = JOptionPane.showConfirmDialog(this, "Delete selected row?", "Confirm", JOptionPane.YES_NO_OPTION);
        if (ans != JOptionPane.YES_OPTION) return;

        Map<String, Object> currentRow = rowToMap(row);
        Map<String, Object> pkValues = new HashMap<>();
        for (String pk : currentPkCols) pkValues.put(pk, currentRow.get(pk));

        try {
            service.deleteRowByPk(currentTable, currentPkCols, pkValues);
            loadSelectedTable();
        } catch (SQLException ex) {
            showError(ex);
        }
    }

    private Map<String, Object> rowToMap(int row) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int c = 0; c < model.getColumnCount(); c++) {
            String col = model.getColumnName(c);
            map.put(col, model.getValueAt(row, c));
        }
        return map;
    }
    
    
    private void doExport() {
    if (currentTable == null || model == null) return;

    // Παράθυρο επιλογής τοποθεσίας αρχείου
    JFileChooser chooser = new JFileChooser();
    chooser.setSelectedFile(new java.io.File(currentTable + ".csv"));
    
    if (chooser.showSaveDialog(this) == JFileChooser.APPROVE_OPTION) {
        try (java.io.PrintWriter pw = new java.io.PrintWriter(chooser.getSelectedFile())) {
            
            // Παίρνουμε τα δεδομένα από τον πίνακα της Service
            int limit = (Integer) limitCombo.getSelectedItem();
            TableService.TableData data = service.loadTable(currentTable, limit);
            
            String csvContent = service.convertToCSV(data);
            pw.write(csvContent);
            
            JOptionPane.showMessageDialog(this, "Export successful!");
        } catch (Exception ex) {
            showError(ex);
        }
    }
}

    /**
     * Priority επιλογών:
     * 1) FK -> JComboBox με referenced values
     * 2) ENUM -> JComboBox με enum literals
     * 3) αλλιώς -> JTextField
     */
    private Map<String, Object> promptForRowValues(String title, Map<String, Object> initialValues) {
        if (model == null) return null;

        JPanel panel = new JPanel(new GridLayout(0, 2, 8, 8));
        Map<String, JComponent> inputs = new LinkedHashMap<>();

        for (int c = 0; c < model.getColumnCount(); c++) {
            String col = model.getColumnName(c);

            // AUTO_INCREMENT: skip στο INSERT
            if (initialValues == null && currentAutoCols.contains(col)) continue;

            JLabel label = new JLabel(col);
            JComponent input;

            // 1) FK dropdown
            if (currentFks.containsKey(col)) {
                TableService.FK fk = currentFks.get(col);
                JComboBox<Object> combo = new JComboBox<>();
                try {
                    for (Object v : service.getReferenceValues(fk.pkTable, fk.pkColumn)) combo.addItem(v);
                } catch (SQLException ex) {
                    showError(ex);
                    return null;
                }
                if (initialValues != null) combo.setSelectedItem(initialValues.get(col));
                input = combo;

            // 2) ENUM dropdown
            } else if (currentEnums.containsKey(col)) {
                TableService.EnumInfo ei = currentEnums.get(col);
                JComboBox<String> combo = new JComboBox<>();

                if (ei.nullable) combo.addItem("(NULL)");
                for (String v : ei.values) combo.addItem(v);

                if (initialValues != null && initialValues.get(col) != null) {
                    combo.setSelectedItem(String.valueOf(initialValues.get(col)));
                } else if (ei.nullable) {
                    combo.setSelectedItem("(NULL)");
                }

                input = combo;

            // 3) free text
            } else {
                JTextField tf = new JTextField();
                if (initialValues != null && initialValues.get(col) != null) {
                    tf.setText(String.valueOf(initialValues.get(col)));
                }
                input = tf;
            }

            // PK read-only στο UPDATE
            if (initialValues != null && currentPkCols.contains(col)) input.setEnabled(false);

            // AUTO_INCREMENT read-only στο UPDATE
            if (initialValues != null && currentAutoCols.contains(col)) input.setEnabled(false);

            panel.add(label);
            panel.add(input);
            inputs.put(col, input);
        }

        int res = JOptionPane.showConfirmDialog(
                this, panel, title, JOptionPane.OK_CANCEL_OPTION, JOptionPane.PLAIN_MESSAGE);

        if (res != JOptionPane.OK_OPTION) return null;

        Map<String, Object> values = new LinkedHashMap<>();

        for (Map.Entry<String, JComponent> e : inputs.entrySet()) {
            String col = e.getKey();
            JComponent comp = e.getValue();

            Object value;

            if (comp instanceof JComboBox) {
                Object sel = ((JComboBox<?>) comp).getSelectedItem();
                if (sel != null && "(NULL)".equals(sel)) value = null;
                else value = sel;
            } else {
                String txt = ((JTextField) comp).getText().trim();
                if (initialValues == null && txt.isEmpty()) continue; // INSERT: skip empty
                value = txt.isEmpty() ? null : txt; // UPDATE: empty -> NULL
            }

            values.put(col, value);
        }

        return values;
    }

    private void showError(Exception ex) {
        ex.printStackTrace();
        JOptionPane.showMessageDialog(this, ex.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> new TableManagerFrame().setVisible(true));
    }
}
