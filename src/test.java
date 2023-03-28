import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class test {
    private static final String DB_NAME = "postgres";
    private static final String DB_PASSWORD = "3143";
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/javatest";

    public static void main(String[] args) {

        String desiredDate = "6"; // тут будет месяц
        boolean desiredBool = true;


        String select = "SELECT * FROM postings WHERE auth_delivery = ? AND EXTRACT(MONTH FROM doc_date) = ?";


        try {
            Connection conn = DriverManager.getConnection(DB_URL, DB_NAME, DB_PASSWORD);

            PreparedStatement ps = conn.prepareStatement(select);

            ps.setBoolean(1, desiredBool);
            ps.setString(2, desiredDate);

            ResultSet result = ps.executeQuery();



        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
