import javax.xml.crypto.Data;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class Main {
    private static final String DB_NAME = "postgres";
    private static final String DB_PASSWORD = "3143";
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/javatest";


    public static String [][] getData(List<String> buff, String sep) {
        String [][] arr = new String[buff.size()][];

        for (int i = 0;i <arr.length;i++){
            arr[i] = (buff.get(i).trim()).split(sep);
        }
        return arr;

    }

    public static List<String> readFromFile(String path) {
        List<String> buff = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String l;
            while ((l = br.readLine()) != null) {
                if(l.isEmpty()){
                    continue;
                }
                buff.add(l);
            }

        } catch (IOException e) {
            throw new IOException();
        }
        finally {
            return buff;
        }
    }
    public static void showArray(String[][]arr){

        for (int i =0; i< arr.length; i++){
            for (int j =0; j< arr[i].length; j++){

                System.out.print(arr[i][j] +" |");
            }
            System.out.println();
        }

    }
    public static String[][] getInformationToCheck(String[][]data){
        String[][] check = new String[data.length][2];
        for (int i =0; i< data.length; i++){
            check[i][0] = data[i][1];
            check[i][1] = data[i][2];

        }
        return check;
    }

    public static boolean contains(String val, String[][] arr){

        for (int i =1; i<arr.length; i++) {
            if((val.trim()).equals(arr[i][0].trim()) && ((arr[i][1].trim())).equalsIgnoreCase("TRUE") ){
                return true;
            }
        }
        return false;
    }

    public static void updatePost(String[][] dataPost,  String[][] infoToCheck ){
        dataPost[0] = (String.join(";",dataPost[0])+"; AuthorizedDelivery" ).split(";");
        for (int i =1; i<dataPost.length; i++) {
            if(contains(dataPost[i][9],infoToCheck)){
                dataPost[i] = (String.join(";",dataPost[i])+"; true" ).split(";");
            }
            else {
                dataPost[i] = (String.join(";",dataPost[i])+"; false" ).split(";");
            }
        }

    }
    public static void main(String[] args) throws SQLException {





        String logPath = "D:/sqlfiles/logins.csv";
        String postPath = "D:/sqlfiles/postings.csv";
        //File f1 = new File(logPath);
        //File f2 = new File(postPath);
        //System.out.println(f1.getName());

        List<String> buffLog;
        List<String> buffPost;

        String[][] dataLog;
        String[][] dataPost;

        String[][] infoToCheck;

        String sql;


        try {
            buffLog = readFromFile(logPath);
            buffPost = readFromFile(postPath);

            dataLog = getData(buffLog,",");
            dataPost = getData(buffPost,";");


            showArray(dataLog);
            System.out.println("===============================================================================================================================");
            showArray(dataPost);

            System.out.println("===============================================================================================================================");


            infoToCheck = getInformationToCheck(dataLog);
            showArray(infoToCheck);
            System.out.println("===============================================================================================================================");
            updatePost(dataPost,infoToCheck);
            showArray(dataPost);
            System.out.println("===============================================================================================================================");
            Connection connection = DriverManager.getConnection(DB_URL,DB_NAME,DB_PASSWORD);

            /* sql = "CREATE TABLE LOGINS(" +
                                "id SERIAL PRIMARY KEY," +
                                "application VARCHAR(20)," +
                                "app_account_name VARCHAR(50)," +
                                "is_active BOOLEAN," +
                                "job_title VARCHAR(100)," +
                                "department VARCHAR(100)" +
                                ");";
                        Statement statement1 = connection.createStatement();
                        statement1.execute(sql);*//*


            */
                String insertlog = "INSERT INTO logins (application, app_account_name,is_active, job_title, department) VALUES (?, ?, ?, ?, ?)";

                        PreparedStatement statement = connection.prepareStatement(insertlog);
                        System.out.println("start entering data into the LOGGING table");
                        for(int i =1; i < dataLog.length; i ++){
                            statement.setString(1,dataLog[i][0]);
                            statement.setString(2,dataLog[i][1]);
                            statement.setBoolean(3,Boolean.parseBoolean(dataLog[i][2]));
                            statement.setString(4,dataLog[i][3]);
                            statement.setString(5,dataLog[i][4]);
                            statement.executeUpdate();
                        }
                        System.out.println("data entered in the table");


                /*ql = "CREATE TABLE POSTINGS(" +
                                "id SERIAL PRIMARY KEY," +
                                "mat_doc DOUBLE PRECISION," +
                                "item INT," +
                                "doc_date DATE," +
                                "pstng_date DATE," +
                                "mat_description VARCHAR(100)," +
                                "quantity INT," +
                                "bun VARCHAR(20)," +
                                "amount_lc VARCHAR(20)," +
                                "crcy VARCHAR(20)," +
                                "user_name VARCHAR(50)," +
                                "auth_delivery BOOLEAN" +
                                ");";
                Statement statement2 = connection.createStatement();
                statement2.execute(sql);
*/


            String insertpost = "INSERT INTO postings (mat_doc, item, doc_date, pstng_date, mat_description, quantity, bun, amount_lc, crcy, user_name, auth_delivery ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            PreparedStatement statement3 = connection.prepareStatement(insertpost);
            System.out.println("start entering data into the POST table");
            SimpleDateFormat dFormat = new SimpleDateFormat("dd.MM.yyyy");

            for(int i =1; i < dataPost.length; i ++){
                statement3.setDouble(1, Double.parseDouble(dataPost[i][0].trim()));

                statement3.setInt(2,Integer.parseInt(dataPost[i][1].trim()));

                statement3.setDate(3,new java.sql.Date((dFormat.parse(dataPost[i][2].trim())).getTime()));

                statement3.setDate(4,new java.sql.Date((dFormat.parse(dataPost[i][2].trim())).getTime()));

                statement3.setString(5,dataPost[i][4].trim());

                statement3.setInt(6,Integer.parseInt(dataPost[i][5].trim()));

                statement3.setString(7,dataPost[i][6].trim());

                statement3.setString(8,dataPost[i][7]);

                statement3.setString(9,dataPost[i][8].trim());

                statement3.setString(10,dataPost[i][9].trim());

                statement3.setBoolean(11,Boolean.parseBoolean( dataPost[i][10].trim()));

                statement3.executeUpdate();
            }
            System.out.println("end");
            System.out.println("===============================================================================================================================");


            int desiredDate = 7; // тут будет месяц
            boolean desiredBool = false;


            String select = "SELECT * FROM postings WHERE auth_delivery = ? AND EXTRACT(MONTH FROM doc_date)= ?;";
            PreparedStatement ps = connection.prepareStatement(select);
            //Statement statement = connection.createStatement();

            ps.setBoolean(1, desiredBool);
            ps.setInt(2, desiredDate);


            ResultSet result = ps.executeQuery();
            while (result.next()) {
                System.out.println("id:" + result.getInt("id")+" product:"
                        +result.getString("mat_description")+" name:"
                        +result.getString("user_name")+" "
                        +result.getBoolean("auth_delivery")
                );
            }
        }
        catch (Throwable e) {
            e.printStackTrace();
        }




    }
}
