import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DB {
    private static final Properties props = new Properties();

    static {
        // Χρήση try-with-resources για αυτόματο κλείσιμο του αρχείου
        try (FileInputStream in = new FileInputStream("config.properties")) {
            props.load(in);
        } catch (IOException e) {
            // Fail Fast: Αν δεν βρεθεί το αρχείο, σταματάμε εδώ την εφαρμογή
            throw new RuntimeException("CRITICAL: Could not load config.properties. Check file path.", e);
        }
    }

    public static Connection get() throws SQLException {
        
        return DriverManager.getConnection(
            props.getProperty("db.url"),
            props.getProperty("db.user"),
            props.getProperty("db.password")
        );
    }
}