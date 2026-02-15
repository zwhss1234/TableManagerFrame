import java.sql.*;
import java.util.*;

public class TableService {

    // ===== DTOs =====
    public static class TableData {
        public final List<String> columns;
        public final List<Object[]> rows;

        public TableData(List<String> columns, List<Object[]> rows) {
            this.columns = columns;
            this.rows = rows;
        }
    }

    public static class FK {
        public final String fkColumn;
        public final String pkTable;
        public final String pkColumn;

        public FK(String fkColumn, String pkTable, String pkColumn) {
            this.fkColumn = fkColumn;
            this.pkTable = pkTable;
            this.pkColumn = pkColumn;
        }
    }

    public static class EnumInfo {
        public final List<String> values;
        public final boolean nullable;

        public EnumInfo(List<String> values, boolean nullable) {
            this.values = values;
            this.nullable = nullable;
        }
    }

    // ===== Metadata =====
    public List<String> listTables() throws SQLException {
        List<String> tables = new ArrayList<>();
        try (Connection con = DB.get()) {
            DatabaseMetaData md = con.getMetaData();
            try (ResultSet rs = md.getTables(con.getCatalog(), null, "%", new String[]{"TABLE"})) {
                while (rs.next()) tables.add(rs.getString("TABLE_NAME"));
            }
        }
        Collections.sort(tables);
        return tables;
    }

    public List<String> getPrimaryKeyColumns(String table) throws SQLException {
        List<String> pk = new ArrayList<>();
        try (Connection con = DB.get()) {
            DatabaseMetaData md = con.getMetaData();
            try (ResultSet rs = md.getPrimaryKeys(con.getCatalog(), null, table)) {
                Map<Short, String> ordered = new TreeMap<>();
                while (rs.next()) {
                    ordered.put(rs.getShort("KEY_SEQ"), rs.getString("COLUMN_NAME"));
                }
                pk.addAll(ordered.values());
            }
        }
        return pk;
    }

    public Set<String> getAutoIncrementColumns(String table) throws SQLException {
        Set<String> autoCols = new HashSet<>();
        try (Connection con = DB.get()) {
            DatabaseMetaData md = con.getMetaData();
            try (ResultSet rs = md.getColumns(con.getCatalog(), null, table, "%")) {
                while (rs.next()) {
                    String col = rs.getString("COLUMN_NAME");
                    String isAuto = rs.getString("IS_AUTOINCREMENT"); // YES/NO
                    if ("YES".equalsIgnoreCase(isAuto)) autoCols.add(col);
                }
            }
        }
        return autoCols;
    }

    public Map<String, FK> getForeignKeys(String table) throws SQLException {
        Map<String, FK> map = new HashMap<>();
        try (Connection con = DB.get()) {
            DatabaseMetaData md = con.getMetaData();
            try (ResultSet rs = md.getImportedKeys(con.getCatalog(), null, table)) {
                while (rs.next()) {
                    String fkCol = rs.getString("FKCOLUMN_NAME");
                    String pkTable = rs.getString("PKTABLE_NAME");
                    String pkCol = rs.getString("PKCOLUMN_NAME");
                    map.put(fkCol, new FK(fkCol, pkTable, pkCol));
                }
            }
        }
        return map;
    }

    public List<Object> getReferenceValues(String table, String column) throws SQLException {
        List<Object> list = new ArrayList<>();
        String sql = "SELECT `" + column + "` FROM `" + table + "` ORDER BY `" + column + "`";
        try (Connection con = DB.get();
             Statement st = con.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) list.add(rs.getObject(1));
        }
        return list;
    }

    /**
     * Επιστρέφει όλες τις ENUM στήλες του table:
     * key = columnName, value = EnumInfo(values, nullable)
     *
     * Χρησιμοποιεί INFORMATION_SCHEMA γιατί είναι ο πιο αξιόπιστος τρόπος να πάρεις τα enum literals.
     */
    public Map<String, EnumInfo> getEnumColumns(String table) throws SQLException {
        Map<String, EnumInfo> enums = new HashMap<>();

        String sql =
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE " +
            "FROM INFORMATION_SCHEMA.COLUMNS " +
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND DATA_TYPE = 'enum'";

        try (Connection con = DB.get();
             PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setString(1, table);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String col = rs.getString("COLUMN_NAME");
                    String colType = rs.getString("COLUMN_TYPE"); // enum('A','B',...)
                    boolean nullable = "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE"));

                    enums.put(col, new EnumInfo(parseEnumLiterals(colType), nullable));
                }
            }
        }
        return enums;
    }

    // ===== Data load =====
    public TableData loadTable(String table, int limit) throws SQLException {
        String sql = "SELECT * FROM `" + table + "` LIMIT ?";
        try (Connection con = DB.get();
             PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setInt(1, limit);

            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData md = rs.getMetaData();
                int cc = md.getColumnCount();

                List<String> cols = new ArrayList<>();
                for (int i = 1; i <= cc; i++) cols.add(md.getColumnLabel(i));

                List<Object[]> rows = new ArrayList<>();
                while (rs.next()) {
                    Object[] r = new Object[cc];
                    for (int i = 1; i <= cc; i++) r[i - 1] = rs.getObject(i);
                    rows.add(r);
                }
                return new TableData(cols, rows);
            }
        }
    }

    // ===== CRUD =====
    public void insertRow(String table, Map<String, Object> values) throws SQLException {
        List<String> cols = new ArrayList<>(values.keySet());
        if (cols.isEmpty()) throw new SQLException("No values to insert.");

        StringBuilder sb = new StringBuilder("INSERT INTO `").append(table).append("` (");
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("`").append(cols.get(i)).append("`");
        }
        sb.append(") VALUES (");
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("?");
        }
        sb.append(")");

        try (Connection con = DB.get();
             PreparedStatement ps = con.prepareStatement(sb.toString())) {
            for (int i = 0; i < cols.size(); i++) ps.setObject(i + 1, values.get(cols.get(i)));
            ps.executeUpdate();
        }
    }

    public void updateRowByPk(String table, List<String> pkCols,
                              Map<String, Object> newValues, Map<String, Object> pkValues) throws SQLException {
        if (pkCols.isEmpty()) throw new SQLException("Table has no PRIMARY KEY. Update not supported.");

        List<String> setCols = new ArrayList<>(newValues.keySet());
        setCols.removeAll(pkCols); // policy: δεν αλλάζουμε PK
        if (setCols.isEmpty()) throw new SQLException("No editable columns to update.");

        StringBuilder sb = new StringBuilder("UPDATE `").append(table).append("` SET ");
        for (int i = 0; i < setCols.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("`").append(setCols.get(i)).append("`=?");
        }
        sb.append(" WHERE ");
        for (int i = 0; i < pkCols.size(); i++) {
            if (i > 0) sb.append(" AND ");
            sb.append("`").append(pkCols.get(i)).append("`=?");
        }

        try (Connection con = DB.get();
             PreparedStatement ps = con.prepareStatement(sb.toString())) {
            int idx = 1;
            for (String c : setCols) ps.setObject(idx++, newValues.get(c));
            for (String c : pkCols) ps.setObject(idx++, pkValues.get(c));
            ps.executeUpdate();
        }
    }

    public void deleteRowByPk(String table, List<String> pkCols, Map<String, Object> pkValues) throws SQLException {
        if (pkCols.isEmpty()) throw new SQLException("Table has no PRIMARY KEY. Delete not supported.");

        StringBuilder sb = new StringBuilder("DELETE FROM `").append(table).append("` WHERE ");
        for (int i = 0; i < pkCols.size(); i++) {
            if (i > 0) sb.append(" AND ");
            sb.append("`").append(pkCols.get(i)).append("`=?");
        }

        try (Connection con = DB.get();
             PreparedStatement ps = con.prepareStatement(sb.toString())) {
            for (int i = 0; i < pkCols.size(); i++) ps.setObject(i + 1, pkValues.get(pkCols.get(i)));
            ps.executeUpdate();
        }
    }
    
    // Export

public String convertToCSV(TableData data) {
    StringBuilder sb = new StringBuilder();
    
    // Προσθήκη των επικεφαλίδων (Headers)
    sb.append(String.join(",", data.columns)).append("\n");
    
    // Προσθήκη των δεδομένων
    for (Object[] row : data.rows) {
        for (int i = 0; i < row.length; i++) {
            String val = (row[i] == null) ? "" : row[i].toString();
            // Αν η τιμή έχει κόμμα, την βάζουμε σε εισαγωγικά για να μη χαλάσει το CSV
            if (val.contains(",")) val = "\"" + val + "\"";
            sb.append(val);
            if (i < row.length - 1) sb.append(",");
        }
        sb.append("\n");
    }
    return sb.toString();
}

    // ===== helpers =====
    private List<String> parseEnumLiterals(String columnType) {
        // columnType: enum('A','B','C')  (MySQL returns with quotes)
        // Θα κάνουμε ασφαλές parse χωρίς regex “κόλπα”.
        int start = columnType.indexOf('(');
        int end = columnType.lastIndexOf(')');
        if (start < 0 || end < 0 || end <= start) return Collections.emptyList();

        String inside = columnType.substring(start + 1, end).trim();

        List<String> vals = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuote = false;

        for (int i = 0; i < inside.length(); i++) {
            char ch = inside.charAt(i);

            if (ch == '\'') {
                // handle escaped quote ''
                if (inQuote && i + 1 < inside.length() && inside.charAt(i + 1) == '\'') {
                    cur.append('\'');
                    i++;
                } else {
                    inQuote = !inQuote;
                }
                continue;
            }

            if (ch == ',' && !inQuote) {
                vals.add(cur.toString());
                cur.setLength(0);
                continue;
            }

            if (inQuote) cur.append(ch);
        }

        if (cur.length() > 0) vals.add(cur.toString());
        return vals;
    }
}

