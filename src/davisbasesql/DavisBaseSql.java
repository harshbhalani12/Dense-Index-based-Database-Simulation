/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package davisbasesql;

import java.io.File;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Scanner;

/**
 *
 * @author Admin
 */
public class DavisBaseSql {

    /**
     * @param args the command line arguments
     */
    static boolean isExit = false;
    static Scanner scanner = new Scanner(System.in).useDelimiter(";");

    /*
     * by default page size is 512
     */
    static long pageSize = 512;
    static long ps = pageSize;
    static long pageSizec = 2048;
    static long psc = pageSizec;
    static long ps_col = pageSize;
    static long psi = pageSize;

    private static void printTokens(String[] insertQueryTokens) {
        for (int i = 0; i < insertQueryTokens.length; i++) {
            System.out.println("he:" + insertQueryTokens[i]);
        }
    }

    private static String convertStringToDate(String dateString) {
        String pattern = "MM:dd:yyyy";
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        try {
            Date date = format.parse(dateString);
            return Long.toString(date.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return Long.toString(new Date().getTime());
    }

    String tableName = null;
    RandomAccessFile tbl_details;
    RandomAccessFile col_details;
    static int pk_occurence = 0;
    static String databaseName = null;
    static String promt_name = "DavisSql> ";

    public static void main(String[] args) {
        // TODO code application logic here

        showStartingDetails();

        createDataDirectory();

        String userCommand = "";
        while (!isExit) {
            System.out.print(promt_name);

            userCommand = scanner.next().replace("\n", "").replace("\r", "").trim().toLowerCase();
            parseCommand(userCommand);

        }
        System.out.println("Exiting from davisBase");
    }

    private static void setPromtName(String userCommand) {
        String str = userCommand.split(" ")[2];
        promt_name = str + "> ";
    }

    public static String line(String s, int num) {
        String a = "";
        for (int i = 0; i < num; i++) {
            a += s;
        }
        return a;
    }

    private static void parseCommand(String userCommand) {
//        System.out.println(userCommand);
        ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
        switch (commandTokens.get(0)) {
            case "set":
                setPromtName(userCommand);
                break;
            case "use":
//                System.out.println("in USE");
                useDatabase(userCommand);
                break;
            case "help":
                showHelpToUsers();
                break;
            case "create":
//                createTables1(userCommand);
                if (commandTokens.get(1).equalsIgnoreCase("database")) {
                    createDatabase(userCommand);
                } else {
                    parseCreateTable(userCommand);
                }
                break;
            case "insert":
                insertIntoTable(userCommand);
                break;
            case "select":
//                System.out.print("select");
                if (commandTokens.get(3).equalsIgnoreCase("davisbase_columns")) {
//                    parseColumnsString(userCommand);
                    showColumns(userCommand);
                } else if (commandTokens.get(3).equalsIgnoreCase("davisbase_tables")) {
                    showTables(userCommand);
                } else {
//                    parseQueryString(userCommand);
                    parseQueryStringfromr(userCommand);
                }
                break;

            case "delete":
                deleteRecord(userCommand);
                break;
            case "show":
                if (commandTokens.get(1).equals("tables") || commandTokens.get(1).equals("table")) {
                    showTables(userCommand);
                } else if (commandTokens.get(1).equals("databases") || commandTokens.get(1).equals("database")) {
                    showDatabases(userCommand);
                }
                break;
            case "drop":
                if (commandTokens.get(1).equals("tables") || commandTokens.get(1).equals("table")) {
                    dropTable(userCommand);
                }else if(commandTokens.get(1).equals("databases") || commandTokens.get(1).equals("database")){
                    dropDatabase(userCommand);
                }
                break;
            case "update":
                updateRecord(userCommand);
                break;
            case "exit":
                isExit = true;
                break;
            case "version":
                getVersionOfDatabase();
                break;
            default:
                System.out.print("I cannot understand your command. Please use \"help;\" for correct command syntax.");
//                isExit = true;
                break;
        }

    }

    private static void showStartingDetails() {
        System.out.println("------------------------------------------------------------------------------------");
        showWelcomeMessage();
        System.out.println("\nType \"help;\" command to display supported commands.");
        System.out.println("------------------------------------------------------------------------------------");
    }

    private static void showWelcomeMessage() {
//        System.out.println("------------------------------------------------------------------------------------");
        System.out.println("Welcome to Davisbase");
        System.out.println("Version : DavisBase v1.0");
        System.out.println("Â©2017 Harsh Bhalani.");

    }

    private static void getVersionOfDatabase() {
        System.out.println("###############################################");
        System.out.println("Version of the Database System : DavisBase v1.0");
        System.out.println("###############################################");
    }

    public static void parseQueryStringfromr(String queryString) {
        if (databaseName != null) {
            try {
                System.out.println("STUB: Calling parseQueryString(String s) to process queries");
                System.out.println("Parsing the string:\"" + queryString + "\"");

                ArrayList<String> QueryStringTokens = new ArrayList<String>(Arrays.asList(queryString.split(" ")));
                String deciding_col = null;
                String comp_val = null;
                String tbl_nm = QueryStringTokens.get(3);
                //   System.out.println(tbl_nm);
                if (QueryStringTokens.size() > 4) {
                    deciding_col = QueryStringTokens.get(5);
                    comp_val = QueryStringTokens.get(7);
                }
                // System.out.println(QueryStringTokens);
                File ftbl = new File("data\\user_data\\" + databaseName + "\\" + tbl_nm);

                File[] fileNames = ftbl.listFiles();
                ArrayList<String> names_col = new ArrayList<String>();

                ArrayList<String> tmp = new ArrayList<>();

                for (int i = 0; i < fileNames.length; i++) {
                    if (fileNames[i].isFile()) {
                        String ndx_names = fileNames[i].getName();
                        if (ndx_names.contains(".ndx")) {
                            names_col.add(ndx_names.substring(0, ndx_names.indexOf(".")));
                        }

                    }
                }
                //System.out.println(names_col);

                ArrayList<String> ord_position = new ArrayList<>();

                for (int o = 0; o < names_col.size() + 15; o++) {
                    ord_position.add(".");
                }

                for (int g = 0; g < names_col.size(); g++) {
                    RandomAccessFile nmc = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tbl_nm + "/" + names_col.get(g) + ".ndx", "rw");
                    nmc.seek(2);
                    int position = nmc.read();
                    ord_position.set(position, names_col.get(g));
                }
                ArrayList<Integer> match_id = new ArrayList<>();
                ArrayList<Integer> match_index = new ArrayList<>();
                ArrayList<String> match_index1 = new ArrayList<>();
                // System.out.println(QueryStringTokens);

                if (QueryStringTokens.size() <= 4) {
//                    System.out.println("in first if(QueryStringTokens.size() <= 4)");
                    if (QueryStringTokens.get(1).equals("*")) {
//                        System.out.println("in if(QueryStringTokens.size() <= 4) -> QueryStringTokens.get(1).equals(\"*\")");

                        ArrayList<Character> row_data = new ArrayList<>();
                        RandomAccessFile queryFile = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tbl_nm + "/" + tbl_nm + ".tbl", "rw");

                        int start = 8;

                        queryFile.seek(start);
                        int a = 0;
                        int tem = queryFile.readShort();
                        //  System.out.println("tem"+tem);
                        while (tem != 0) {
                            if (-1 == tem) {
                                start = start + 2;//queryFile2.seek(start1);
                                queryFile.seek(start);

                                tem = queryFile.readShort();
                            } else {
                                queryFile.seek(start);

                                tem = queryFile.readShort();

                                String h = Integer.toHexString(tem);

                                queryFile.seek(tem);
                                String data = (Integer.toString(queryFile.read()));

                                queryFile.seek(tem + 5);
                                int ncol = queryFile.read();

                                int seek_rlength = tem + 5 + ncol + 1;

                                queryFile.seek(seek_rlength);

                                int first_length = queryFile.read();

                                for (int i = 0; i < first_length; i++) {
                                    a++;
                                    queryFile.seek(seek_rlength + 1);
                                    char b = (char) (queryFile.read());
                                    //row_data.add((String)Integer.toHexString(queryFile.read()));
                                    row_data.add(b);
                                }
                                //  System.out.println(row_data);
                                //  System.out.println("seek rlength"+seek_rlength + " first "+first_length);
                                int sl = seek_rlength + first_length + 1;
                                row_data.add('\t');
                                row_data.add('\t');
                                for (int k = 1; k < ncol; k++) {
                        //a=tem+5+ncol+3;

                                    //System.out.println(sl);
                                    queryFile.seek(sl);
                                    int rlength = queryFile.read();
                                    //System.out.println("r"+rlength);

                                    for (int i = 0; i < rlength; i++) {
                                        sl += 1;
                                        queryFile.seek(sl);

                                        char b = (char) (queryFile.read());
                                        //row_data.add((String)Integer.toHexString(queryFile.read()));
                                        row_data.add(b);
                                        //System.out.println(row_data);
                                    }
                                    row_data.add('\t');
                                    sl += 1;
                                    row_data.add('\t');

                                    //rlength=queryFile.read();
                                }
                                //System.out.println(row_data);
                                start = start + 2;
                                row_data.add(';');
                                queryFile.seek(start);
                            }
                        }
                        int y7 = 0;
                        for (int i = 0; i < names_col.size(); i++) {
                            System.out.print(ord_position.get(i));
                            System.out.print("\t\t|");
                            y7++;
                        }
                        int lines = y7 * 16;
                        System.out.println();
                        System.out.println(line("-", lines));
                        //System.out.println(row_data);
                        String final_data = "";
                        String column_data = "";
                        String column_wise[] = null;
                        for (int j = 0; j < row_data.size(); j++) {
                            final_data = final_data + row_data.get(j);
                        }
                        String row_wise[] = final_data.split(";");

//                    System.out.println("\nin if loop of containing *\n");
                        for (int k = 0; k < row_wise.length - 1; k++) {
                            //System.out.println(row_wise[k]);
                            //column_data=row_wise[k];
                            //row_wise[k].replace('<', '\t');
                            System.out.println(row_wise[k]);
                            //column_wise=column_data.split("^");
                        }
                    } else {
//                        System.out.println("QueryStringTokens.get(1).equals(\"*\") no else part");
                        String yu = QueryStringTokens.get(1);
                        int yo = ord_position.indexOf(yu);

                        RandomAccessFile queryFile = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tbl_nm + "/" + tbl_nm + ".tbl", "rw");

                        ArrayList<Character> row_data11 = new ArrayList<>();
                        int start1 = 8;

                        queryFile.seek(start1);
                        int tem1 = queryFile.readShort();

                        while (tem1 != 0) {
                            if (-1 == tem1) {
                                start1 = start1 + 2;//queryFile2.seek(start1);
                                queryFile.seek(start1);

                                tem1 = queryFile.readShort();
                            } else {
                                queryFile.seek(start1);

                                tem1 = queryFile.readShort();

                                String h1 = Integer.toHexString(tem1);

                                queryFile.seek(tem1);
                                String data1 = (Integer.toString(queryFile.read()));

                                queryFile.seek(tem1 + 5);
                                int ncol1 = queryFile.read();

                                int seek_rlength1 = tem1 + 5 + ncol1 + 1;

                                queryFile.seek(seek_rlength1);

                                int first_length1 = queryFile.read();

                                for (int i = 0; i < first_length1; i++) {
                                    queryFile.seek(seek_rlength1 + 1);
                                    char b1 = (char) (queryFile.read());
                                    //row_data.add((String)Integer.toHexString(queryFile.read()));
                                    row_data11.add(b1);
                                }

                                int sl1 = seek_rlength1 + first_length1 + 1;
                                row_data11.add('`');

                                for (int k = 1; k < ncol1; k++) {

                                    queryFile.seek(sl1);
                                    int rlength1 = queryFile.read();

                                    for (int i = 0; i < rlength1; i++) {
                                        sl1 += 1;
                                        queryFile.seek(sl1);

                                        char b1 = (char) (queryFile.read());

                                        row_data11.add(b1);

                                    }

                                    sl1 += 1;
                                    row_data11.add('`');

                                }

                                start1 = start1 + 2;
                                row_data11.add(';');
                                System.out.println(row_data11);
                                queryFile.seek(start1);
                            }
                        }
                        int y77 = 0;
                        for (int i = 0; i < names_col.size(); i++) {
                            y77++;
                        }

                        String final_data1 = "";
                        String column_data1 = "";
                        String column_wise1[] = null;
                        for (int j = 0; j < row_data11.size(); j++) {
                            final_data1 = final_data1 + row_data11.get(j);
                        }
                        //System.out.println(final_data1);
                        //String row_wise11[]=final_data.split(";");
                        ArrayList<String> row_wise12 = new ArrayList<String>(Arrays.asList(final_data1.split(";")));
                        // System.out.println(row_wise12);

                        ArrayList<String> colex = new ArrayList<>();
//                        System.out.println("rowwise"+row_wise12);
                        for (int k = 0; k < row_wise12.size() - 1; k++) {
                            //System.out.println(row_wise12.get(k));
                            String column_extract[] = row_wise12.get(k).split("`");
                            //System.out.println(column_extract[index_of_query_element]);
                            String temppp = column_extract[yo];
                            // System.out.println("temp"+temppp);
                            colex.add(temppp);

                            //System.out.println("colex"+colex);
                        }

                        System.out.println(yu);
                        int line7 = 16;
//                    //System.out.println("------------------");
//                    System.out.println("\n if there is no * in select\n");
                        System.out.println(line("-", line7));
                        for (int y = 0; y < colex.size(); y++) {
                            System.out.println(colex.get(y));
                        }
//                }
//                        System.out.println(ord_position.indexOf("name"));
                    }
                } else if (QueryStringTokens.contains("where")) {
//                System.out.println("in else if of QueryStringTokens.contains(\"where\")");
                    if (deciding_col.equals("rowid") || deciding_col.equals("row_id")) {
//                    System.out.println("in if deciding_col.equals(\"rowid\")");
                        int fg = 0;
                        int row_no = Integer.parseInt(QueryStringTokens.get(7));
                        //String query_col=QueryStringTokens.get(5);
                        String operator = QueryStringTokens.get(6);
                        if (operator.equals("=")) {
                            fg = 1;
                        } else if (operator.equals("<")) {
                            fg = 2;
                        } else if (operator.equals(">")) {
                            fg = 3;
                        } else {
                            fg = 0;
                        }
                        RandomAccessFile queryFile = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tbl_nm + "/" + tbl_nm + ".tbl", "rw");
                        int start1, result;
                        //System.out.println(operator);
                        switch (fg) {
                            case 0:
                                System.out.println(operator + " Operator not supported");
                                break;
                            case 1:
                                queryFile.seek(0x10);
                                start1 = 0x08;
                                result = 0;
                                queryFile.seek(start1);
                                while (queryFile.readShort() != 0) {
                                    queryFile.seek(start1);
                                    result = queryFile.readShort();
                                    result += 4;
                                    queryFile.seek(result);
                                    int rec_val = queryFile.read();
                                    //System.out.println(rec_val);
                                    if (rec_val == row_no) {
                                        match_index.add(row_no);
                                        match_id.add(result);
                                    }
                                    start1 += 2;
                                }
                                //System.out.println(match_index);
                                //System.out.println(match_id);
                                break;
                            case 2:
                                queryFile.seek(0x10);
                                start1 = 0x08;
                                result = 0;
                                queryFile.seek(start1);
                                while (queryFile.readShort() != 0) {
                                    queryFile.seek(start1);
                                    result = queryFile.readShort();
                                    result += 4;
                                    queryFile.seek(result);
                                    int rec_val = queryFile.read();
                                    if (rec_val < row_no) {
                                        match_index.add(row_no);
                                        match_id.add(result);
                                    }
                                    start1 += 2;
                                }
                                //System.out.println(match_index);
                                break;
                            case 3:
                                queryFile.seek(0x10);
                                start1 = 0x08;
                                result = 0;
                                queryFile.seek(start1);
                                while (queryFile.readShort() != 0) {
                                    queryFile.seek(start1);
                                    result = queryFile.readShort();
                                    result += 4;
                                    queryFile.seek(result);
                                    int rec_val = queryFile.read();
                                    if (rec_val > row_no) {
                                        match_index.add(row_no);
                                        match_id.add(result);
                                    }
                                    start1 += 2;
                                }
                                //System.out.println(match_index);
                                break;
                        }

                        ArrayList<Character> row_data1 = new ArrayList<>();
                        RandomAccessFile queryFile1 = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tbl_nm + "/" + tbl_nm + ".tbl", "rw");
                        for (int r = 0; r < match_id.size(); r++) {
                            queryFile.seek(match_id.get(r));

                            String data = (Integer.toString(queryFile.read()));
                            int tem1 = match_id.get(r);
                            queryFile1.seek(tem1 + 1);
                            int ncol = queryFile1.read();
                            System.out.println(tem1 + 1);

                            int seek_rlength = tem1 + 1 + ncol + 1;
                            System.out.println(seek_rlength);
                            queryFile1.seek(seek_rlength);

                            int first_length = queryFile1.read();
                            System.out.println(first_length);

                            for (int i = 0; i < first_length; i++) {

                                queryFile1.seek(seek_rlength + 1);
                                char b = (char) (queryFile1.read());

                                row_data1.add(b);
                            }

                            int sl = seek_rlength + first_length + 1;
//                            System.out.println("1 wth is this??????" + sl);
                            row_data1.add('\t');
                            row_data1.add('\t');
//                            System.out.println("n wth is this??????" + ncol);
                            for (int k = 1; k < ncol; k++) {

                                System.out.println(sl);
                                queryFile1.seek(sl);
                                int rlength = queryFile1.readByte();
                                System.out.println("r wth" + rlength);

                                for (int i = 0; i < rlength; i++) {
                                    sl += 1;
                                    queryFile1.seek(sl);

                                    char b = (char) (queryFile1.read());

                                    row_data1.add(b);
//                         
                                }
                                row_data1.add('\t');
                                sl += 1;
                                row_data1.add('\t');

                                //rlength=queryFile.read();
                            }
                            //System.out.println(row_data);
//                start=start+2;
                            row_data1.add(';');
                        }
                        System.out.println(row_data1);
                        int y71 = 0;
                        for (int i = 0; i < names_col.size(); i++) {
                            System.out.print(ord_position.get(i));
                            System.out.print("\t\t|");
                            y71++;
                        }
                        int lines = y71 * 16;
                        System.out.println();
                        System.out.println(line("-", lines));
                        //System.out.println(row_data);
                        String final_data = "";
                        String column_data = "";
                        String column_wise[] = null;
                        for (int j = 0; j < row_data1.size(); j++) {
                            final_data = final_data + row_data1.get(j);
                        }

                        String row_wise[] = final_data.split(";");

                        //System.out.println(row_wise);
//                    System.out.println("\nin else if of where -> in if(equal row id)\n");
                        for (int k = 0; k < row_wise.length; k++) {
                            //System.out.println(row_wise[k]);
                            //column_data=row_wise[k];
                            //row_wise[k].replace('<', '\t');
                            System.out.println(row_wise[k]);
                            //column_wise=column_data.split("^");
                        }
                        //System.out.println(names_col);
                    } else if ((names_col.contains(deciding_col))) {
//                    System.out.println("in else if (names_col.contains(deciding_col) ");
                        ArrayList<Character> row_data = new ArrayList<>();

                        RandomAccessFile queryFile = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tbl_nm + "/" + tbl_nm + ".tbl", "rw");

                        int start = 8;

                        queryFile.seek(start);
                        int a = 0;
                        int tem = queryFile.readShort();

                        while (tem != 0) {
                            if (-1 == tem) {
                                start = start + 2;//queryFile2.seek(start1);
                                queryFile.seek(start);

                                tem = queryFile.readShort();
                            } else {
                                queryFile.seek(start);

                                tem = queryFile.readShort();

                                String h = Integer.toHexString(tem);

                                queryFile.seek(tem);
                                String data = (Integer.toString(queryFile.read()));

                                queryFile.seek(tem + 5);
                                int ncol = queryFile.read();

                                int seek_rlength = tem + 5 + ncol + 1;

                                queryFile.seek(seek_rlength);

                                int first_length = queryFile.read();

                                for (int i = 0; i < first_length; i++) {
                                    a++;
                                    queryFile.seek(seek_rlength + 1);
                                    char b = (char) (queryFile.read());
                                    //row_data.add((String)Integer.toHexString(queryFile.read()));
                                    row_data.add(b);
                                }
                                //  System.out.println(row_data);
                                //  System.out.println("seek rlength"+seek_rlength + " first "+first_length);
                                int sl = seek_rlength + first_length + 1;
                                row_data.add('\t');
                                row_data.add('\t');
                                for (int k = 1; k < ncol; k++) {
                        //a=tem+5+ncol+3;

                                    //System.out.println(sl);
                                    queryFile.seek(sl);
                                    int rlength = queryFile.read();
                                    //System.out.println("r"+rlength);

                                    for (int i = 0; i < rlength; i++) {
                                        sl += 1;
                                        queryFile.seek(sl);

                                        char b = (char) (queryFile.read());
                                        //row_data.add((String)Integer.toHexString(queryFile.read()));
                                        row_data.add(b);

                                    }
                                    row_data.add('\t');
                                    sl += 1;
                                    row_data.add('\t');

                                    //rlength=queryFile.read();
                                }

                                start = start + 2;
                                row_data.add(';');
                                queryFile.seek(start);
                            }
                        }
                        int y7 = 0;
                        for (int i = 0; i < names_col.size(); i++) {
//                        System.out.print(ord_positon.get(i));
//                        System.out.print("\t\t");
                            y7++;
                        }

                        int lines = y7 * 16;
//                    System.out.println();
//                    System.out.println(line("-", lines));
                        //System.out.println(row_data);
                        String final_data = "";
                        String column_data = "";
                        String column_wise[] = null;
                        for (int j = 0; j < row_data.size(); j++) {
                            final_data = final_data + row_data.get(j);
                        }

                        String row_wise[] = final_data.split(";");
                        ArrayList<String> tempo = new ArrayList<>();

                        //        System.out.println(tempo);
//                    System.out.println("\nin else if of decidiing column\n");
                        for (int k = 0; k < row_wise.length - 1; k++) {
                            //System.out.println(row_wise[k]);
                            //column_data=row_wise[k];
                            //row_wise[k].replace('<', '\t');
                            if (row_wise[k].contains(comp_val)) {
//########################### //
                                System.out.println(row_wise[k]); //###########################
                            }
                            //column_wise=column_data.split("^");
                        }

                        ArrayList<Character> row_data11 = new ArrayList<>();
                        int start1 = 8;

                        queryFile.seek(start1);
                        int tem1 = queryFile.readShort();

                        while (tem1 != 0) {
                            if (-1 == tem) {
                                start = start + 2;//queryFile2.seek(start1);
                                queryFile.seek(start);

                                tem = queryFile.readShort();
                            } else {
                                queryFile.seek(start1);

                                tem1 = queryFile.readShort();

                                String h1 = Integer.toHexString(tem1);

                                queryFile.seek(tem1);
                                String data1 = (Integer.toString(queryFile.read()));

                                queryFile.seek(tem1 + 5);
                                int ncol1 = queryFile.read();

                                int seek_rlength1 = tem1 + 5 + ncol1 + 1;

                                queryFile.seek(seek_rlength1);

                                int first_length1 = queryFile.read();

                                for (int i = 0; i < first_length1; i++) {
                                    queryFile.seek(seek_rlength1 + 1);
                                    char b1 = (char) (queryFile.read());
                                    //row_data.add((String)Integer.toHexString(queryFile.read()));
                                    row_data11.add(b1);
                                }

                                int sl1 = seek_rlength1 + first_length1 + 1;
                                row_data11.add('`');

                                for (int k = 1; k < ncol1; k++) {

                                    queryFile.seek(sl1);
                                    int rlength1 = queryFile.read();

                                    for (int i = 0; i < rlength1; i++) {
                                        sl1 += 1;
                                        queryFile.seek(sl1);

                                        char b1 = (char) (queryFile.read());

                                        row_data11.add(b1);

                                    }

                                    sl1 += 1;
                                    row_data11.add('`');

                                }

                                start1 = start1 + 2;
                                row_data11.add(';');
                                queryFile.seek(start1);
                                //System.out.println(row_data11);
                            }
                        }
                        int y77 = 0;
                        for (int i = 0; i < names_col.size(); i++) {
                            y7++;
                        }

                        String final_data1 = "";
                        String column_data1 = "";
                        String column_wise1[] = null;
                        for (int j = 0; j < row_data11.size(); j++) {
                            final_data1 = final_data1 + row_data11.get(j);
                        }
                        //System.out.println(final_data1);
                        //String row_wise11[]=final_data.split(";");
                        ArrayList<String> row_wise12 = new ArrayList<String>(Arrays.asList(final_data1.split(";")));
                        //  System.out.println(row_wise12);

                        int index_of_query_element = ord_position.indexOf(deciding_col);
                        ArrayList<Integer> cols_to_output = new ArrayList<>();

                        ArrayList<String> depth = new ArrayList<>();

                        ArrayList<String> colex = new ArrayList<>();
                        //  System.out.println("rowwise"+row_wise12);
                        for (int k = 0; k < row_wise12.size() - 1; k++) {
                            //System.out.println(row_wise12.get(k));
                            String column_extract[] = row_wise12.get(k).split("`");
                            //System.out.println(column_extract[index_of_query_element]);
                            String temppp = column_extract[index_of_query_element];
                            // System.out.println("temp"+temppp);
                            colex.add(temppp);
                            depth = colex;
                            //System.out.println("colex"+colex);
                        }
//                    System.out.println("cp0" + colex);
                        int red = 0;
                        //if(colex.contains(comp_val)){

                        int fg = 0;
                        int row_no = Integer.parseInt(QueryStringTokens.get(7));
                        //String query_col=QueryStringTokens.get(5);
                        String operator = QueryStringTokens.get(6);
                        if (operator.equals("=")) {
                            fg = 1;
                        } else if (operator.equals("<")) {
                            fg = 2;
                        } else if (operator.equals(">")) {
                            fg = 3;
                        } else {
                            fg = 0;
                        }

                        if (fg == 1) {
                            for (String cv : colex) {

                                if (cv.equalsIgnoreCase(comp_val)) {
                                    cols_to_output.add(red);
                                    //System.out.println(row_wise12.get(red));
                                }
                                red++;
                            }
                        } else if (fg == 2) {
                            for (String cv : colex) {

                                if (Integer.parseInt(cv) < Integer.parseInt(comp_val)) {
                                    //System.out.println(cv+" "+comp_val);
                                    cols_to_output.add(red);
                                    //System.out.println(row_wise12.get(red));
                                }
                                red++;
                            }
                        } else if (fg == 3) {
                            for (String cv : colex) {

                                if (Integer.parseInt(cv) > Integer.parseInt(comp_val)) {
                                    //System.out.println(cv+" "+comp_val);
                                    cols_to_output.add(red);
                                    //System.out.println(row_wise12.get(red));
                                }
                                red++;
                            }
                        }
                        //System.out.println(cols_to_output);

                        if (QueryStringTokens.get(1).equals("*")) {
//                            System.out.println("else if  deciding col ni ander pachho if( *) aayo");
                            int y74 = 0;
                            for (int i = 0; i < names_col.size(); i++) {
                                System.out.print(ord_position.get(i));
                                System.out.print("\t\t");
                                y74++;
                            }

                            int lines4 = y74 * 16;
                            System.out.println();
//                        System.out.println("\nelse if  deciding col ni ander pachho if( *) aayo\n");
                            System.out.println(line("-", lines4));
                            for (int n = 0; n < cols_to_output.size(); n++) {
                                //row_wise12.get(n).replace("`", "\t\t");
                                System.out.println(row_wise12.get(cols_to_output.get(n)).replaceAll("`", "\t\t"));
                            }
                        } else {
                            ArrayList<String> colex1 = new ArrayList<>();
                            String col_name_q = QueryStringTokens.get(1);
                            int q = ord_position.indexOf(col_name_q);

                            // System.out.println("rowwise"+row_wise12);
                            for (int k = 0; k < row_wise12.size() - 1; k++) {
                                //System.out.println(row_wise12.get(k));
                                String column_extract[] = row_wise12.get(k).split("`");
                                //System.out.println(column_extract[index_of_query_element]);
                                String temppp = column_extract[q];
                                //System.out.println("temp"+temppp);
                                colex1.add(temppp);
                                //depth = colex;
                                //System.out.println("colex"+colex);
                            }

//                        System.out.println("q"+q);
                            System.out.println("" + col_name_q);
                            int line7 = 16;
                            //System.out.println("------------------");
//                        System.out.println("\nelse if  deciding col ni ander pachho * aayo and eno else\n");
                            System.out.println(line("-", line7));
                            for (int y = 0; y < colex1.size(); y++) {
                                System.out.println(colex1.get(cols_to_output.get(y)));
                                String colValue = colex1.get(cols_to_output.get(y));
//                                System.out.println("\n\nCOLValue:" + colex1.get(cols_to_output.get(y)) + "\n\n");
                            }
                        }
//                        System.out.println("....row id j chlau chhe have aana pa6i else aavse....");
//                    System.out.println(ord_position.indexOf("name"));
                    } else {
                        System.out.println("Column " + deciding_col + " not found in " + tbl_nm + " table");
                    }
                } else {
                    System.out.println("Syntax error....");
                }

            } catch (Exception ex) {
                //Logger.getLogger(DavisBase.class.getName()).log(Level.SEVERE, null, ex);
                //System.out.println("msg"+ex.getMessage());
            }
        } else {
            System.out.println("database is not selected.");
        }
    }

    public static String parseQueryStringfromrForSelectWhere(String queryString) {
        String colValue = null;
        if (databaseName != null) {
            try {

//                System.out.println("STUB: Calling parseQueryString(String s) to process queries");
//                System.out.println("Parsing the string:\"" + queryString + "\"");
                ArrayList<String> QueryStringTokens = new ArrayList<String>(Arrays.asList(queryString.split(" ")));
                String deciding_col = null;
                String comp_val = null;
                String tbl_nm = QueryStringTokens.get(3);
                //   System.out.println(tbl_nm);
                if (QueryStringTokens.size() > 4) {
                    deciding_col = QueryStringTokens.get(5);
                    comp_val = QueryStringTokens.get(7);
                }
                // System.out.println(QueryStringTokens);
                File ftbl = new File("data\\user_data\\" + databaseName + "\\" + tbl_nm);

                File[] fileNames = ftbl.listFiles();
                ArrayList<String> names_col = new ArrayList<String>();

                ArrayList<String> tmp = new ArrayList<>();

                for (int i = 0; i < fileNames.length; i++) {
                    if (fileNames[i].isFile()) {
                        String ndx_names = fileNames[i].getName();
                        if (ndx_names.contains(".ndx")) {
                            names_col.add(ndx_names.substring(0, ndx_names.indexOf(".")));
                        }

                    }
                }
                //System.out.println(names_col);

                ArrayList<String> ord_position = new ArrayList<>();

                for (int o = 0; o < names_col.size() + 15; o++) {
                    ord_position.add(".");
                }

                for (int g = 0; g < names_col.size(); g++) {
                    RandomAccessFile nmc = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tbl_nm + "/" + names_col.get(g) + ".ndx", "rw");
                    nmc.seek(2);
                    int position = nmc.read();
                    ord_position.set(position, names_col.get(g));
                }
                ArrayList<Integer> match_id = new ArrayList<>();
                ArrayList<Integer> match_index = new ArrayList<>();
                ArrayList<String> match_index1 = new ArrayList<>();
                // System.out.println(QueryStringTokens);

                if (QueryStringTokens.size() <= 4) {
                    //System.out.println("in first if(QueryStringTokens.size() <= 4)");
                    if (QueryStringTokens.get(1).equals("*")) {
                        //System.out.println("in if(QueryStringTokens.size() <= 4) -> QueryStringTokens.get(1).equals(\"*\")");

                        ArrayList<Character> row_data = new ArrayList<>();
                        RandomAccessFile queryFile = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tbl_nm + "/" + tbl_nm + ".tbl", "rw");

                        int start = 8;

                        queryFile.seek(start);
                        int a = 0;
                        int tem = queryFile.readShort();
                        //  System.out.println("tem"+tem);
                        while (tem != 0) {
                            if (-1 == tem) {
                                start = start + 2;//queryFile2.seek(start1);
                                queryFile.seek(start);

                                tem = queryFile.readShort();
                            } else {
                                queryFile.seek(start);

                                tem = queryFile.readShort();

                                String h = Integer.toHexString(tem);

                                queryFile.seek(tem);
                                String data = (Integer.toString(queryFile.read()));

                                queryFile.seek(tem + 5);
                                int ncol = queryFile.read();

                                int seek_rlength = tem + 5 + ncol + 1;

                                queryFile.seek(seek_rlength);

                                int first_length = queryFile.read();

                                for (int i = 0; i < first_length; i++) {
                                    a++;
                                    queryFile.seek(seek_rlength + 1);
                                    char b = (char) (queryFile.read());
                                    //row_data.add((String)Integer.toHexString(queryFile.read()));
                                    row_data.add(b);
                                }
                                //  System.out.println(row_data);
                                //  System.out.println("seek rlength"+seek_rlength + " first "+first_length);
                                int sl = seek_rlength + first_length + 1;
                                row_data.add('\t');
                                row_data.add('\t');
                                for (int k = 1; k < ncol; k++) {
                        //a=tem+5+ncol+3;

                                    //System.out.println(sl);
                                    queryFile.seek(sl);
                                    int rlength = queryFile.read();
                                    //System.out.println("r"+rlength);

                                    for (int i = 0; i < rlength; i++) {
                                        sl += 1;
                                        queryFile.seek(sl);

                                        char b = (char) (queryFile.read());
                                        //row_data.add((String)Integer.toHexString(queryFile.read()));
                                        row_data.add(b);
                                        //System.out.println(row_data);
                                    }
                                    row_data.add('\t');
                                    sl += 1;
                                    row_data.add('\t');

                                    //rlength=queryFile.read();
                                }
                                //System.out.println(row_data);
                                start = start + 2;
                                row_data.add(';');
                                queryFile.seek(start);
                            }
                        }
                        int y7 = 0;
                        for (int i = 0; i < names_col.size(); i++) {
                            System.out.print(ord_position.get(i));
                            System.out.print("\t\t|");
                            y7++;
                        }
                        int lines = y7 * 16;
                        System.out.println();
                        System.out.println(line("-", lines));
                        //System.out.println(row_data);
                        String final_data = "";
                        String column_data = "";
                        String column_wise[] = null;
                        for (int j = 0; j < row_data.size(); j++) {
                            final_data = final_data + row_data.get(j);
                        }
                        String row_wise[] = final_data.split(";");

//                    System.out.println("\nin if loop of containing *\n");
                        for (int k = 0; k < row_wise.length - 1; k++) {
                            //System.out.println(row_wise[k]);
                            //column_data=row_wise[k];
                            //row_wise[k].replace('<', '\t');
                            System.out.println(row_wise[k]);
                            //column_wise=column_data.split("^");
                        }
                    } else {
                        //System.out.println("QueryStringTokens.get(1).equals(\"*\") no else part");
                        String yu = QueryStringTokens.get(1);
                        int yo = ord_position.indexOf(yu);

                        RandomAccessFile queryFile = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tbl_nm + "/" + tbl_nm + ".tbl", "rw");

                        ArrayList<Character> row_data11 = new ArrayList<>();
                        int start1 = 8;

                        queryFile.seek(start1);
                        int tem1 = queryFile.readShort();

                        while (tem1 != 0) {
                            if (-1 == tem1) {
                                start1 = start1 + 2;//queryFile2.seek(start1);
                                queryFile.seek(start1);

                                tem1 = queryFile.readShort();
                            } else {
                                queryFile.seek(start1);

                                tem1 = queryFile.readShort();

                                String h1 = Integer.toHexString(tem1);

                                queryFile.seek(tem1);
                                String data1 = (Integer.toString(queryFile.read()));

                                queryFile.seek(tem1 + 5);
                                int ncol1 = queryFile.read();

                                int seek_rlength1 = tem1 + 5 + ncol1 + 1;

                                queryFile.seek(seek_rlength1);

                                int first_length1 = queryFile.read();

                                for (int i = 0; i < first_length1; i++) {
                                    queryFile.seek(seek_rlength1 + 1);
                                    char b1 = (char) (queryFile.read());
                                    //row_data.add((String)Integer.toHexString(queryFile.read()));
                                    row_data11.add(b1);
                                }

                                int sl1 = seek_rlength1 + first_length1 + 1;
                                row_data11.add('`');

                                for (int k = 1; k < ncol1; k++) {

                                    queryFile.seek(sl1);
                                    int rlength1 = queryFile.read();

                                    for (int i = 0; i < rlength1; i++) {
                                        sl1 += 1;
                                        queryFile.seek(sl1);

                                        char b1 = (char) (queryFile.read());

                                        row_data11.add(b1);

                                    }

                                    sl1 += 1;
                                    row_data11.add('`');

                                }

                                start1 = start1 + 2;
                                row_data11.add(';');
                                System.out.println(row_data11);
                                queryFile.seek(start1);
                            }
                        }
                        int y77 = 0;
                        for (int i = 0; i < names_col.size(); i++) {
                            y77++;
                        }

                        String final_data1 = "";
                        String column_data1 = "";
                        String column_wise1[] = null;
                        for (int j = 0; j < row_data11.size(); j++) {
                            final_data1 = final_data1 + row_data11.get(j);
                        }
                        //System.out.println(final_data1);
                        //String row_wise11[]=final_data.split(";");
                        ArrayList<String> row_wise12 = new ArrayList<String>(Arrays.asList(final_data1.split(";")));
                        // System.out.println(row_wise12);

                        ArrayList<String> colex = new ArrayList<>();
//                        System.out.println("rowwise"+row_wise12);
                        for (int k = 0; k < row_wise12.size() - 1; k++) {
                            //System.out.println(row_wise12.get(k));
                            String column_extract[] = row_wise12.get(k).split("`");
                            //System.out.println(column_extract[index_of_query_element]);
                            String temppp = column_extract[yo];
                            // System.out.println("temp"+temppp);
                            colex.add(temppp);

                            //System.out.println("colex"+colex);
                        }

                        System.out.println(yu);
                        int line7 = 16;
//                    //System.out.println("------------------");
//                    System.out.println("\n if there is no * in select\n");
                        System.out.println(line("-", line7));
                        for (int y = 0; y < colex.size(); y++) {
                            System.out.println(colex.get(y));
                        }
//                }
//                        System.out.println(ord_position.indexOf("name"));
                    }
                } else if (QueryStringTokens.contains("where")) {
//                System.out.println("in else if of QueryStringTokens.contains(\"where\")");
                    if (deciding_col.equals("rowid") || deciding_col.equals("row_id")) {
//                    System.out.println("in if deciding_col.equals(\"rowid\")");
                        int fg = 0;
                        int row_no = Integer.parseInt(QueryStringTokens.get(7));
                        //String query_col=QueryStringTokens.get(5);
                        String operator = QueryStringTokens.get(6);
                        if (operator.equals("=")) {
                            fg = 1;
                        } else if (operator.equals("<")) {
                            fg = 2;
                        } else if (operator.equals(">")) {
                            fg = 3;
                        } else {
                            fg = 0;
                        }
                        RandomAccessFile queryFile = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tbl_nm + "/" + tbl_nm + ".tbl", "rw");
                        int start1, result;
                        //System.out.println(operator);
                        switch (fg) {
                            case 0:
                                System.out.println(operator + " Operator not supported");
                                break;
                            case 1:
                                queryFile.seek(0x10);
                                start1 = 0x08;
                                result = 0;
                                queryFile.seek(start1);
                                while (queryFile.readShort() != 0) {
                                    queryFile.seek(start1);
                                    result = queryFile.readShort();
                                    result += 4;
                                    queryFile.seek(result);
                                    int rec_val = queryFile.read();
                                    //System.out.println(rec_val);
                                    if (rec_val == row_no) {
                                        match_index.add(row_no);
                                        match_id.add(result);
                                    }
                                    start1 += 2;
                                }
                                //System.out.println(match_index);
                                //System.out.println(match_id);
                                break;
                            case 2:
                                queryFile.seek(0x10);
                                start1 = 0x08;
                                result = 0;
                                queryFile.seek(start1);
                                while (queryFile.readShort() != 0) {
                                    queryFile.seek(start1);
                                    result = queryFile.readShort();
                                    result += 4;
                                    queryFile.seek(result);
                                    int rec_val = queryFile.read();
                                    if (rec_val < row_no) {
                                        match_index.add(row_no);
                                        match_id.add(result);
                                    }
                                    start1 += 2;
                                }
                                //System.out.println(match_index);
                                break;
                            case 3:
                                queryFile.seek(0x10);
                                start1 = 0x08;
                                result = 0;
                                queryFile.seek(start1);
                                while (queryFile.readShort() != 0) {
                                    queryFile.seek(start1);
                                    result = queryFile.readShort();
                                    result += 4;
                                    queryFile.seek(result);
                                    int rec_val = queryFile.read();
                                    if (rec_val > row_no) {
                                        match_index.add(row_no);
                                        match_id.add(result);
                                    }
                                    start1 += 2;
                                }
                                //System.out.println(match_index);
                                break;
                        }

                        ArrayList<Character> row_data1 = new ArrayList<>();
                        RandomAccessFile queryFile1 = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tbl_nm + "/" + tbl_nm + ".tbl", "rw");
                        for (int r = 0; r < match_id.size(); r++) {
                            queryFile.seek(match_id.get(r));

                            String data = (Integer.toString(queryFile.read()));
                            int tem1 = match_id.get(r);
                            queryFile1.seek(tem1 + 1);
                            int ncol = queryFile1.read();
                            System.out.println(tem1 + 1);

                            int seek_rlength = tem1 + 1 + ncol + 1;
                            System.out.println(seek_rlength);
                            queryFile1.seek(seek_rlength);

                            int first_length = queryFile1.read();
                            System.out.println(first_length);

                            for (int i = 0; i < first_length; i++) {

                                queryFile1.seek(seek_rlength + 1);
                                char b = (char) (queryFile1.read());

                                row_data1.add(b);
                            }

                            int sl = seek_rlength + first_length + 1;
//                            System.out.println("1 wth is this??????" + sl);
                            row_data1.add('\t');
                            row_data1.add('\t');
//                            System.out.println("n wth is this??????" + ncol);
                            for (int k = 1; k < ncol; k++) {

                                System.out.println(sl);
                                queryFile1.seek(sl);
                                int rlength = queryFile1.readByte();
                                System.out.println("r wth" + rlength);

                                for (int i = 0; i < rlength; i++) {
                                    sl += 1;
                                    queryFile1.seek(sl);

                                    char b = (char) (queryFile1.read());

                                    row_data1.add(b);
//                         
                                }
                                row_data1.add('\t');
                                sl += 1;
                                row_data1.add('\t');

                                //rlength=queryFile.read();
                            }
                            //System.out.println(row_data);
//                start=start+2;
                            row_data1.add(';');
                        }
                        System.out.println(row_data1);
                        int y71 = 0;
                        for (int i = 0; i < names_col.size(); i++) {
                            System.out.print(ord_position.get(i));
                            System.out.print("\t\t|");
                            y71++;
                        }
                        int lines = y71 * 16;
                        System.out.println();
                        System.out.println(line("-", lines));
                        //System.out.println(row_data);
                        String final_data = "";
                        String column_data = "";
                        String column_wise[] = null;
                        for (int j = 0; j < row_data1.size(); j++) {
                            final_data = final_data + row_data1.get(j);
                        }

                        String row_wise[] = final_data.split(";");

                        //System.out.println(row_wise);
//                    System.out.println("\nin else if of where -> in if(equal row id)\n");
                        for (int k = 0; k < row_wise.length; k++) {
                            //System.out.println(row_wise[k]);
                            //column_data=row_wise[k];
                            //row_wise[k].replace('<', '\t');
                            System.out.println(row_wise[k]);
                            //column_wise=column_data.split("^");
                        }
                        //System.out.println(names_col);
                    } else if ((names_col.contains(deciding_col))) {
//                    System.out.println("in else if (names_col.contains(deciding_col) ");
                        ArrayList<Character> row_data = new ArrayList<>();

                        RandomAccessFile queryFile = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tbl_nm + "/" + tbl_nm + ".tbl", "rw");

                        int start = 8;

                        queryFile.seek(start);
                        int a = 0;
                        int tem = queryFile.readShort();

                        while (tem != 0) {
                            if (-1 == tem) {
                                start = start + 2;//queryFile2.seek(start1);
                                queryFile.seek(start);

                                tem = queryFile.readShort();
                            } else {
                                queryFile.seek(start);

                                tem = queryFile.readShort();

                                String h = Integer.toHexString(tem);

                                queryFile.seek(tem);
                                String data = (Integer.toString(queryFile.read()));

                                queryFile.seek(tem + 5);
                                int ncol = queryFile.read();

                                int seek_rlength = tem + 5 + ncol + 1;

                                queryFile.seek(seek_rlength);

                                int first_length = queryFile.read();

                                for (int i = 0; i < first_length; i++) {
                                    a++;
                                    queryFile.seek(seek_rlength + 1);
                                    char b = (char) (queryFile.read());
                                    //row_data.add((String)Integer.toHexString(queryFile.read()));
                                    row_data.add(b);
                                }
                                //  System.out.println(row_data);
                                //  System.out.println("seek rlength"+seek_rlength + " first "+first_length);
                                int sl = seek_rlength + first_length + 1;
                                row_data.add('\t');
                                row_data.add('\t');
                                for (int k = 1; k < ncol; k++) {
                        //a=tem+5+ncol+3;

                                    //System.out.println(sl);
                                    queryFile.seek(sl);
                                    int rlength = queryFile.read();
                                    //System.out.println("r"+rlength);

                                    for (int i = 0; i < rlength; i++) {
                                        sl += 1;
                                        queryFile.seek(sl);

                                        char b = (char) (queryFile.read());
                                        //row_data.add((String)Integer.toHexString(queryFile.read()));
                                        row_data.add(b);

                                    }
                                    row_data.add('\t');
                                    sl += 1;
                                    row_data.add('\t');

                                    //rlength=queryFile.read();
                                }

                                start = start + 2;
                                row_data.add(';');
                                queryFile.seek(start);
                            }
                        }
                        int y7 = 0;
                        for (int i = 0; i < names_col.size(); i++) {
//                        System.out.print(ord_positon.get(i));
//                        System.out.print("\t\t");
                            y7++;
                        }

                        int lines = y7 * 16;
//                    System.out.println();
//                    System.out.println(line("-", lines));
                        //System.out.println(row_data);
                        String final_data = "";
                        String column_data = "";
                        String column_wise[] = null;
                        for (int j = 0; j < row_data.size(); j++) {
                            final_data = final_data + row_data.get(j);
                        }

                        String row_wise[] = final_data.split(";");
                        ArrayList<String> tempo = new ArrayList<>();

                        //        System.out.println(tempo);
//                    System.out.println("\nin else if of decidiing column\n");
                        for (int k = 0; k < row_wise.length - 1; k++) {
                            //System.out.println(row_wise[k]);
                            //column_data=row_wise[k];
                            //row_wise[k].replace('<', '\t');
                            if (row_wise[k].contains(comp_val)) {
//########################### //
                                //System.out.println(row_wise[k]); //###########################
                            }
                            //column_wise=column_data.split("^");
                        }

                        ArrayList<Character> row_data11 = new ArrayList<>();
                        int start1 = 8;

                        queryFile.seek(start1);
                        int tem1 = queryFile.readShort();

                        while (tem1 != 0) {
                            if (-1 == tem) {
                                start = start + 2;//queryFile2.seek(start1);
                                queryFile.seek(start);

                                tem = queryFile.readShort();
                            } else {
                                queryFile.seek(start1);

                                tem1 = queryFile.readShort();

                                String h1 = Integer.toHexString(tem1);

                                queryFile.seek(tem1);
                                String data1 = (Integer.toString(queryFile.read()));

                                queryFile.seek(tem1 + 5);
                                int ncol1 = queryFile.read();

                                int seek_rlength1 = tem1 + 5 + ncol1 + 1;

                                queryFile.seek(seek_rlength1);

                                int first_length1 = queryFile.read();

                                for (int i = 0; i < first_length1; i++) {
                                    queryFile.seek(seek_rlength1 + 1);
                                    char b1 = (char) (queryFile.read());
                                    //row_data.add((String)Integer.toHexString(queryFile.read()));
                                    row_data11.add(b1);
                                }

                                int sl1 = seek_rlength1 + first_length1 + 1;
                                row_data11.add('`');

                                for (int k = 1; k < ncol1; k++) {

                                    queryFile.seek(sl1);
                                    int rlength1 = queryFile.read();

                                    for (int i = 0; i < rlength1; i++) {
                                        sl1 += 1;
                                        queryFile.seek(sl1);

                                        char b1 = (char) (queryFile.read());

                                        row_data11.add(b1);

                                    }

                                    sl1 += 1;
                                    row_data11.add('`');

                                }

                                start1 = start1 + 2;
                                row_data11.add(';');
                                queryFile.seek(start1);
                                //System.out.println(row_data11);
                            }
                        }
                        int y77 = 0;
                        for (int i = 0; i < names_col.size(); i++) {
                            y7++;
                        }

                        String final_data1 = "";
                        String column_data1 = "";
                        String column_wise1[] = null;
                        for (int j = 0; j < row_data11.size(); j++) {
                            final_data1 = final_data1 + row_data11.get(j);
                        }
                        //System.out.println(final_data1);
                        //String row_wise11[]=final_data.split(";");
                        ArrayList<String> row_wise12 = new ArrayList<String>(Arrays.asList(final_data1.split(";")));
                        //  System.out.println(row_wise12);

                        int index_of_query_element = ord_position.indexOf(deciding_col);
                        ArrayList<Integer> cols_to_output = new ArrayList<>();

                        ArrayList<String> depth = new ArrayList<>();

                        ArrayList<String> colex = new ArrayList<>();
                        //  System.out.println("rowwise"+row_wise12);
                        for (int k = 0; k < row_wise12.size() - 1; k++) {
                            //System.out.println(row_wise12.get(k));
                            String column_extract[] = row_wise12.get(k).split("`");
                            //System.out.println(column_extract[index_of_query_element]);
                            String temppp = column_extract[index_of_query_element];
                            // System.out.println("temp"+temppp);
                            colex.add(temppp);
                            depth = colex;
                            //System.out.println("colex"+colex);
                        }
//                    System.out.println("cp0" + colex);
                        int red = 0;
                        //if(colex.contains(comp_val)){

                        int fg = 0;
                        int row_no = Integer.parseInt(QueryStringTokens.get(7));
                        //String query_col=QueryStringTokens.get(5);
                        String operator = QueryStringTokens.get(6);
                        if (operator.equals("=")) {
                            fg = 1;
                        } else if (operator.equals("<")) {
                            fg = 2;
                        } else if (operator.equals(">")) {
                            fg = 3;
                        } else {
                            fg = 0;
                        }

                        if (fg == 1) {
                            for (String cv : colex) {

                                if (cv.equalsIgnoreCase(comp_val)) {
                                    cols_to_output.add(red);
                                    //System.out.println(row_wise12.get(red));
                                }
                                red++;
                            }
                        } else if (fg == 2) {
                            for (String cv : colex) {

                                if (Integer.parseInt(cv) < Integer.parseInt(comp_val)) {
                                    //System.out.println(cv+" "+comp_val);
                                    cols_to_output.add(red);
                                    //System.out.println(row_wise12.get(red));
                                }
                                red++;
                            }
                        } else if (fg == 3) {
                            for (String cv : colex) {

                                if (Integer.parseInt(cv) > Integer.parseInt(comp_val)) {
                                    //System.out.println(cv+" "+comp_val);
                                    cols_to_output.add(red);
                                    //System.out.println(row_wise12.get(red));
                                }
                                red++;
                            }
                        }
                        //System.out.println(cols_to_output);

                        if (QueryStringTokens.get(1).equals("*")) {
                            //System.out.println("else if  deciding col ni ander pachho if( *) aayo");
                            int y74 = 0;
                            for (int i = 0; i < names_col.size(); i++) {
                                System.out.print(ord_position.get(i));
                                System.out.print("\t\t");
                                y74++;
                            }

                            int lines4 = y74 * 16;
                            System.out.println();
//                        System.out.println("\nelse if  deciding col ni ander pachho if( *) aayo\n");
                            System.out.println(line("-", lines4));
                            for (int n = 0; n < cols_to_output.size(); n++) {
                                //row_wise12.get(n).replace("`", "\t\t");
                                System.out.println(row_wise12.get(cols_to_output.get(n)).replaceAll("`", "\t\t"));
                            }
                        } else {
                            ArrayList<String> colex1 = new ArrayList<>();
                            String col_name_q = QueryStringTokens.get(1);
                            int q = ord_position.indexOf(col_name_q);

                            // System.out.println("rowwise"+row_wise12);
                            for (int k = 0; k < row_wise12.size() - 1; k++) {
                                //System.out.println(row_wise12.get(k));
                                String column_extract[] = row_wise12.get(k).split("`");
                                //System.out.println(column_extract[index_of_query_element]);
                                String temppp = column_extract[q];
                                //System.out.println("temp"+temppp);
                                colex1.add(temppp);
                                //depth = colex;
                                //System.out.println("colex"+colex);
                            }

//                        System.out.println("q"+q);
                            System.out.println("" + col_name_q);
                            int line7 = 16;
                            //System.out.println("------------------");
//                        System.out.println("\nelse if  deciding col ni ander pachho * aayo and eno else\n");
                            System.out.println(line("-", line7));
                            for (int y = 0; y < colex1.size(); y++) {
                                System.out.println(colex1.get(cols_to_output.get(y)));
                                /*String colValue*/
                                colValue = colex1.get(cols_to_output.get(y));
                                //System.out.println("\n\nCOLValue:" + colex1.get(cols_to_output.get(y)) + "\n\n");

                            }
                        }
                        //System.out.println("....row id j chlau chhe have aana pa6i else aavse....");
//                    System.out.println(ord_position.indexOf("name"));
                    } else {
                        System.out.println("Column " + deciding_col + " not found in " + tbl_nm + " table");
                    }
                } else {
                    System.out.println("Syntax error....");
                }

            } catch (Exception ex) {
                //Logger.getLogger(DavisBase.class.getName()).log(Level.SEVERE, null, ex);
                //System.out.println("msg"+ex.getMessage());
            }
        } else {
            System.out.println("database is not selected.");
        }
        return colValue;
    }

    private static void updateRecord(String updateString) {
        if (databaseName != null) {
            try {
                System.out.println("STUB: Calling parseQueryString(String s) to process queries");
                System.out.println("Parsing the string:\"" + updateString + "\"");

                ArrayList<String> UpdateStringTokens = new ArrayList<String>(Arrays.asList(updateString.split(" ")));
                String tblName = UpdateStringTokens.get(1);
                String updateColumnName = UpdateStringTokens.get(3);
                String updateColumnValue = UpdateStringTokens.get(5);
                String criteriaName = UpdateStringTokens.get(7);
                String criteriaValue = UpdateStringTokens.get(9);

                //update table set name = abc where id = 2
                String selectWhereString = "select " + updateColumnName + " from " + tblName + " where " + criteriaName + " = " + criteriaValue;
//                System.out.println("selectWhereString:" + selectWhereString);
                String str = parseQueryStringfromrForSelectWhere(selectWhereString);
//                System.out.println("\n\nstr in update:" + str + "\n\n");

                ArrayList<String> ord_position = new ArrayList<>();
                ArrayList<String> names_col = new ArrayList<String>();

                ArrayList<String> tmp = new ArrayList<>();
                File ftbl = new File("data\\user_data\\" + databaseName + "\\" + tblName);

                File[] fileNames = ftbl.listFiles();

                for (int i = 0; i < fileNames.length; i++) {
                    if (fileNames[i].isFile()) {
                        String ndx_names = fileNames[i].getName();
                        if (ndx_names.contains(".ndx")) {
                            names_col.add(ndx_names.substring(0, ndx_names.indexOf(".")));
                        }

                    }
                }
                for (int i = 0; i < names_col.size() + 15; i++) {
                    ord_position.add(".");
                }

                for (int i = 0; i < names_col.size(); i++) {
                    RandomAccessFile nmc = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tblName + "/" + names_col.get(i) + ".ndx", "rw");
                    nmc.seek(2);
                    int position = nmc.read();
                    ord_position.set(position, names_col.get(i));
                }
                int yo = ord_position.indexOf(criteriaName);

                int updateColumnNameIndex = ord_position.indexOf(updateColumnName);

                RandomAccessFile queryFile = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tblName + "/" + tblName + ".tbl", "rw");

                ArrayList<Character> row_data11 = new ArrayList<>();
                int start1 = 8;

                queryFile.seek(start1);
                int tem1 = queryFile.readShort();

                while (tem1 != 0) {
                    if (-1 == tem1) {
                        start1 = start1 + 2;//queryFile2.seek(start1);
                        queryFile.seek(start1);

                        tem1 = queryFile.readShort();
                    } else {
                        queryFile.seek(start1);

                        tem1 = queryFile.readShort();

                        String h1 = Integer.toHexString(tem1);

                        queryFile.seek(tem1);
                        String data1 = (Integer.toString(queryFile.read()));

                        queryFile.seek(tem1 + 5);
                        int ncol1 = queryFile.read();

                        int seek_rlength1 = tem1 + 5 + ncol1 + 1;

                        queryFile.seek(seek_rlength1);

                        int first_length1 = queryFile.read();

                        for (int i = 0; i < first_length1; i++) {
                            queryFile.seek(seek_rlength1 + 1);
                            char b1 = (char) (queryFile.read());
                            //row_data.add((String)Integer.toHexString(queryFile.read()));
                            row_data11.add(b1);
                        }

                        int sl1 = seek_rlength1 + first_length1 + 1;
                        row_data11.add('`');

                        for (int k = 1; k < ncol1; k++) {

                            queryFile.seek(sl1);
                            int rlength1 = queryFile.read();

                            for (int i = 0; i < rlength1; i++) {
                                sl1 += 1;
                                queryFile.seek(sl1);

                                char b1 = (char) (queryFile.read());

                                row_data11.add(b1);

                            }

                            sl1 += 1;
                            row_data11.add('`');

                        }

                        start1 = start1 + 2;
                        row_data11.add(';');
                        queryFile.seek(start1);
//                        System.out.println(row_data11);
                    }
                }

                String final_data1 = "";
                String column_data1 = "";
                String column_wise1[] = null;
                for (int j = 0; j < row_data11.size(); j++) {
                    final_data1 = final_data1 + row_data11.get(j);
                }
//                System.out.println(final_data1);
//                //String row_wise11[]=final_data.split(";");
                ArrayList<String> row_wise12 = new ArrayList<String>(Arrays.asList(final_data1.split(";")));
//                System.out.println(row_wise12);
//                
                ArrayList<String> colex = new ArrayList<>();
////                        System.out.println("rowwise"+row_wise12);
                for (int k = 0; k < row_wise12.size() - 1; k++) {
                    //System.out.println(row_wise12.get(k));
                    String column_extract[] = row_wise12.get(k).split("`");
                    //System.out.println(column_extract[index_of_query_element]);
                    String temppp = column_extract[yo];
//                System.out.println("temp" + temppp);
                    colex.add(temppp);

                    //System.out.println("colex"+colex);
                }
                ArrayList<Integer> recno1 = new ArrayList<>();
                int red1 = 1;
                for (int io = 0; io < colex.size(); io++) {
                    if (colex.get(io).equals(criteriaValue)) {
                        recno1.add(red1);
                    }
                    red1++;
                }
//            System.out.println("abcd" + recno1);
                int recno = colex.indexOf(criteriaValue) + 1;
//            System.out.println("colex" + colex);
//            System.out.println("rec" + recno);
                int to_update = 0;
                for (int y = 0; y < colex.size(); y++) {
                    //     System.out.println(colex.get(y));
                    if (colex.get(y).equalsIgnoreCase(criteriaValue)) {
                        to_update = y;
                    }
                }
                ArrayList<String> colex1 = new ArrayList<>();

                for (int k = 0; k < row_wise12.size() - 1; k++) {

                    String column_extract[] = row_wise12.get(k).split("`");

                    String temppp1 = column_extract[updateColumnNameIndex];

                    colex1.add(temppp1);

                    //System.out.println("colex"+colex);
                }
                colex1.set(to_update, updateColumnValue);

                for (int t = 0; t < recno1.size(); t++) {
                    start1 = 0x08;
                    int result = 0;
                    queryFile.seek(start1);
                    int tem2 = queryFile.readShort();
                    while (tem2 != 0) {
                        if (-1 == tem2) {
                            start1 = start1 + 2;//queryFile2.seek(start1);
                            queryFile.seek(start1);

                            tem2 = queryFile.readShort();
                        } else {
                            queryFile.seek(start1);
                            result = queryFile.readShort();
                            result += 4;
                            queryFile.seek(result);
                            int rec_val = queryFile.read();
                            int cntr = 1;
                            //System.out.println(rec_val);
                            if (rec_val == recno1.get(t)) {
                                queryFile.seek(result + 1);
                                int ncol = queryFile.read();

                                int how_much1 = result + 1 + ncol + 1;

                                queryFile.seek(how_much1);
                                int rlength = queryFile.read();
                                while (cntr != updateColumnNameIndex + 1) {
                                    queryFile.seek(how_much1);
                                    rlength = queryFile.read();
                                    for (int u = 0; u < rlength + 1; u++) {
                                        how_much1++;
                                    }
                                    cntr++;
                                }
                                queryFile.seek(how_much1 + 1);
                                queryFile.writeBytes(updateColumnValue);

                            }
                            start1 += 2;
                            queryFile.seek(start1);
                        }
                    }
                }
                System.out.println("Record updated.");
            } catch (Exception e) {

            }
            System.out.println("Record updated.");
        } else {
            System.out.println("database is not selected.");
        }
    }

    private static void deleteRecord(String deleteTableString) {
        //code
        if (databaseName != null) {
            try {
                System.out.println("STUB: Calling parseQueryString(String s) to process queries");
                System.out.println("Parsing the string:\"" + deleteTableString + "\"");

                ArrayList<String> QueryStringTokens = new ArrayList<String>(Arrays.asList(deleteTableString.split(" ")));
                //String query_col=QeryStringTokens.get(5);
                int rowNo = Integer.parseInt(QueryStringTokens.get(6));
                String tableName = QueryStringTokens.get(2);

                boolean flag = false;

                RandomAccessFile queryFile2 = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tableName + "\\" + tableName + ".tbl", "rw");

                if (QueryStringTokens.get(0).equalsIgnoreCase("delete")) {
                    int start, result;
                    int matchIndex = 0;
                    int matchId = 0;
                    int start1 = 0x08;

                    queryFile2.seek(start1);
                    while (queryFile2.readShort() != 0) {
                        queryFile2.seek(start1);
                        if (-1 == queryFile2.readShort()) {
                            //queryFile2.seek(start1);
                        } else {
                            queryFile2.seek(start1);
                            result = (int) queryFile2.readShort();
                            result += 4;
                            queryFile2.seek(result);
                            int recordVal = queryFile2.read();

                            if (recordVal == rowNo) {
                                matchIndex = recordVal;
                                matchId = result - 4;

                                start = 8;

                                while (queryFile2.readShort() != 0) {
                                    queryFile2.seek(start);
                                    result = queryFile2.readShort();
                                    if (result == matchId) {
                                        queryFile2.seek(start1);
                                        queryFile2.writeShort(0xFFFF);
                                        break;
                                    }
                                    start += 2;
                                }
                                flag = true;
                            } else {
                                flag = false;
                            }
                        }
                        start1 += 2;
                    }
                    if (flag) {
                        System.out.println("Query OK. 1 row affected.");
                    } else {
                        System.out.println("Record is not available for row_id = " + rowNo);
                    }
                }
            } catch (Exception e) {
                System.out.println(e);
            }
        } else {
            System.out.println("database is not selected.");
        }
    }

    public static void parseQueryString(String queryString) {
        if (databaseName != null) {
            try {
                System.out.println("STUB: Calling parseQueryString(String s) to process queries");
                System.out.println("Parsing the string:\"" + queryString + "\"");

                ArrayList<String> QueryStringTokens = new ArrayList<String>(Arrays.asList(queryString.split(" ")));
                String deciding_col = null;
                String comp_val = null;
                String tbl_nm = QueryStringTokens.get(3);
                //   System.out.println(tbl_nm);
                if (QueryStringTokens.size() > 4) {
                    deciding_col = QueryStringTokens.get(5);
                    comp_val = QueryStringTokens.get(7);
                }
                System.out.println(QueryStringTokens);
                File ftbl = new File("data\\user_data\\" + databaseName + "\\" + tbl_nm);

                File[] fileNames = ftbl.listFiles();
                ArrayList<String> names_col = new ArrayList<String>();

                ArrayList<String> tmp = new ArrayList<>();

                for (int i = 0; i < fileNames.length; i++) {
                    if (fileNames[i].isFile()) {
                        String ndx_names = fileNames[i].getName();
                        if (ndx_names.contains(".ndx")) {
                            names_col.add(ndx_names.substring(0, ndx_names.indexOf(".")));
                        }
                    }
                }
                //System.out.println(names_col);

                ArrayList<String> ord_position = new ArrayList<>();

                for (int o = 0; o < names_col.size() + 15; o++) {
                    ord_position.add(".");
                }

                for (int g = 0; g < names_col.size(); g++) {
                    RandomAccessFile nmc = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tbl_nm + "\\" + names_col.get(g) + ".ndx", "rw");
                    nmc.seek(2);
                    int position = nmc.read();
                    ord_position.set(position, names_col.get(g));
                }
                ArrayList<Integer> match_id = new ArrayList<>();
                ArrayList<Integer> match_index = new ArrayList<>();
                ArrayList<String> match_index1 = new ArrayList<>();
                System.out.println(QueryStringTokens);

                if (QueryStringTokens.size() <= 4) {
                    if (QueryStringTokens.get(1).equals("*")) {

                        ArrayList<Character> row_data = new ArrayList<>();
                        RandomAccessFile queryFile = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tbl_nm + "\\" + tbl_nm + ".tbl", "rw");

                        int start = 8;

                        queryFile.seek(start);
                        int a = 0;
                        int tem = queryFile.readShort();
                        //                        System.out.println("tem"+tem);
                        while (tem != 0) {
                            if (-1 == tem) {
                                start = start + 2;//queryFile2.seek(start1);
                                queryFile.seek(start);

                                tem = queryFile.readShort();
                            } else {
                                queryFile.seek(start);

                                tem = queryFile.readShort();

                                String h = Integer.toHexString(tem);

                                queryFile.seek(tem);
                                String data = (Integer.toString(queryFile.read()));

                                queryFile.seek(tem + 5);
                                int ncol = queryFile.read();

                                int seek_rlength = tem + 5 + ncol + 1;

                                queryFile.seek(seek_rlength);

                                int first_length = queryFile.read();

                                for (int i = 0; i < first_length; i++) {
                                    a++;
                                    queryFile.seek(seek_rlength + 1);
                                    char b = (char) (queryFile.read());
                                    //row_data.add((String)Integer.toHexString(queryFile.read()));
                                    row_data.add(b);
                                }
                                //  System.out.println(row_data);
                                //  System.out.println("seek rlength"+seek_rlength + " first "+first_length);
                                int srlength = seek_rlength + first_length + 1;
                                row_data.add('\t');
                                row_data.add('\t');
                                for (int k = 1; k < ncol; k++) {
                            //a=tem+5+ncol+3;

                                    //System.out.println(srlength);
                                    queryFile.seek(srlength);
                                    int rlength = queryFile.read();
                                    //System.out.println("r"+rlength);

                                    for (int i = 0; i < rlength; i++) {
                                        srlength += 1;
                                        queryFile.seek(srlength);

                                        char b = (char) (queryFile.read());
                                        //row_data.add((String)Integer.toHexString(queryFile.read()));
                                        row_data.add(b);
                                        //System.out.println(row_data);
                                    }
                                    row_data.add('\t');
                                    srlength += 1;
                                    row_data.add('\t');

                                    //rlength=queryFile.read();
                                }
                                //System.out.println(row_data);
                                start = start + 2;
                                row_data.add(';');
                                queryFile.seek(start);
                            }
                        }
                        int y7 = 0;
                        for (int i = 0; i < names_col.size(); i++) {
                            System.out.print(ord_position.get(i));
                            System.out.print("\t\t");
                            y7++;
                        }
                        int lines = y7 * 16;
                        System.out.println();
                        //                    System.out.println(line("-", lines));
                        //System.out.println(row_data);
                        String final_data = "";
                        String column_data = "";
                        String column_wise[] = null;
                        for (int j = 0; j < row_data.size(); j++) {
                            final_data = final_data + row_data.get(j);
                        }
                        String row_wise[] = final_data.split(";");

                        for (int k = 0; k < row_wise.length - 1; k++) {
                            //System.out.println(row_wise[k]);
                            //column_data=row_wise[k];
                            //row_wise[k].replace('<', '\t');
                            System.out.println(row_wise[k]);
                            //column_wise=column_data.split("^");
                        }
                    } else {
                        String yu = QueryStringTokens.get(1);
                        int yo = ord_position.indexOf(yu);

                        RandomAccessFile queryFile = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tbl_nm + "\\" + tbl_nm + ".tbl", "rw");

                        ArrayList<Character> row_data11 = new ArrayList<>();
                        int start1 = 8;

                        queryFile.seek(start1);
                        int tem1 = queryFile.readShort();

                        while (tem1 != 0) {
                            if (-1 == tem1) {
                                start1 = start1 + 2;//queryFile2.seek(start1);
                                queryFile.seek(start1);

                                tem1 = queryFile.readShort();
                            } else {
                                queryFile.seek(start1);

                                tem1 = queryFile.readShort();

                                String h1 = Integer.toHexString(tem1);

                                queryFile.seek(tem1);
                                String data1 = (Integer.toString(queryFile.read()));

                                queryFile.seek(tem1 + 5);
                                int ncol1 = queryFile.read();

                                int seek_rlength1 = tem1 + 5 + ncol1 + 1;

                                queryFile.seek(seek_rlength1);

                                int first_length1 = queryFile.read();

                                for (int i = 0; i < first_length1; i++) {
                                    queryFile.seek(seek_rlength1 + 1);
                                    char b1 = (char) (queryFile.read());
                                    //row_data.add((String)Integer.toHexString(queryFile.read()));
                                    row_data11.add(b1);
                                }

                                int srlength1 = seek_rlength1 + first_length1 + 1;
                                row_data11.add('`');

                                for (int k = 1; k < ncol1; k++) {

                                    queryFile.seek(srlength1);
                                    int rlength1 = queryFile.read();

                                    for (int i = 0; i < rlength1; i++) {
                                        srlength1 += 1;
                                        queryFile.seek(srlength1);

                                        char b1 = (char) (queryFile.read());

                                        row_data11.add(b1);

                                    }

                                    srlength1 += 1;
                                    row_data11.add('`');

                                }

                                start1 = start1 + 2;
                                row_data11.add(';');
                                System.out.println(row_data11);
                                queryFile.seek(start1);
                            }
                        }
                        int y77 = 0;
                        for (int i = 0; i < names_col.size(); i++) {
                            y77++;
                        }

                        String final_data1 = "";
                        String column_data1 = "";
                        String column_wise1[] = null;
                        for (int j = 0; j < row_data11.size(); j++) {
                            final_data1 = final_data1 + row_data11.get(j);
                        }
                        //System.out.println(final_data1);
                        //String row_wise11[]=final_data.split(";");
                        ArrayList<String> row_wise12 = new ArrayList<String>(Arrays.asList(final_data1.split(";")));
                        System.out.println(row_wise12);

                        ArrayList<String> colex = new ArrayList<>();
                        //                        System.out.println("rowwise"+row_wise12);
                        for (int k = 0; k < row_wise12.size() - 1; k++) {
                            //System.out.println(row_wise12.get(k));
                            String column_extract[] = row_wise12.get(k).split("`");
                            //System.out.println(column_extract[index_of_query_element]);
                            String temppp = column_extract[yo];
                            //                    System.out.println("temp"+temppp);
                            colex.add(temppp);

                            //System.out.println("colex"+colex);
                        }

                        System.out.println(yu);
                        int line7 = 16;
                        //                    //System.out.println("------------------");
                        //                    System.out.println(line("-", line7));
                        System.out.println("in else");
                        for (int y = 0; y < colex.size(); y++) {
                            System.out.println(colex.get(y));
                        }
                        //                }
                        //                        System.out.println(ord_position.indexOf("name"));
                    }
                } else if (QueryStringTokens.contains("where")) {
                    if (deciding_col.equals("rowid") || deciding_col.equals("row_id")) {
                        int fg = 0;
                        int row_no = Integer.parseInt(QueryStringTokens.get(7));
                        //String query_col=QueryStringTokens.get(5);
                        String operator = QueryStringTokens.get(6);
                        if (operator.equals("=")) {
                            fg = 1;
                        } else if (operator.equals("<")) {
                            fg = 2;
                        } else if (operator.equals(">")) {
                            fg = 3;
                        } else {
                            fg = 0;
                        }
                        RandomAccessFile queryFile = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tbl_nm + "\\" + tbl_nm + ".tbl", "rw");
                        int start1, result;
                        //System.out.println(operator);
                        switch (fg) {
                            case 0:
                                System.out.println(operator + " Operator not supported");
                                break;
                            case 1:
                                queryFile.seek(0x10);
                                start1 = 0x08;
                                result = 0;
                                queryFile.seek(start1);
                                while (queryFile.readShort() != 0) {
                                    queryFile.seek(start1);
                                    result = queryFile.readShort();
                                    result += 4;
                                    queryFile.seek(result);
                                    int rec_val = queryFile.read();
                                    //System.out.println(rec_val);
                                    if (rec_val == row_no) {
                                        match_index.add(row_no);
                                        match_id.add(result);
                                    }
                                    start1 += 2;
                                }
                                //                            System.out.println(match_index);
                                //                            System.out.println(match_id);
                                break;
                            case 2:
                                queryFile.seek(0x10);
                                start1 = 0x08;
                                result = 0;
                                queryFile.seek(start1);
                                while (queryFile.readShort() != 0) {
                                    queryFile.seek(start1);
                                    result = queryFile.readShort();
                                    result += 4;
                                    queryFile.seek(result);
                                    int rec_val = queryFile.read();
                                    if (rec_val < row_no) {
                                        match_index.add(row_no);
                                        match_id.add(result);
                                    }
                                    start1 += 2;
                                }
                                //                            System.out.println(match_index);
                                break;
                            case 3:
                                queryFile.seek(0x10);
                                start1 = 0x08;
                                result = 0;
                                queryFile.seek(start1);
                                while (queryFile.readShort() != 0) {
                                    queryFile.seek(start1);
                                    result = queryFile.readShort();
                                    result += 4;
                                    queryFile.seek(result);
                                    int rec_val = queryFile.read();
                                    if (rec_val > row_no) {
                                        match_index.add(row_no);
                                        match_id.add(result);
                                    }
                                    start1 += 2;
                                }
                                //                            System.out.println(match_index);
                                break;
                        }

                        ArrayList<Character> row_data1 = new ArrayList<>();
                        RandomAccessFile queryFile1 = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tbl_nm + "\\" + tbl_nm + ".tbl", "rw");
                        for (int r = 0; r < match_id.size(); r++) {
                            queryFile.seek(match_id.get(r));

                            String data = (Integer.toString(queryFile.read()));
                            int tem1 = match_id.get(r);
                            queryFile1.seek(tem1 + 1);
                            int ncol = queryFile1.read();
                            //                        System.out.println(tem1+1);

                            int seek_rlength = tem1 + 1 + ncol + 1;
                            //                        System.out.println(seek_rlength);
                            queryFile1.seek(seek_rlength);

                            int first_length = queryFile1.read();
                            //                        System.out.println(first_length);

                            for (int i = 0; i < first_length; i++) {

                                queryFile1.seek(seek_rlength + 1);
                                char b = (char) (queryFile1.read());

                                row_data1.add(b);
                            }

                            int srlength = seek_rlength + first_length + 1;
                            //                        System.out.println("1"+srlength);
                            row_data1.add('\t');
                            row_data1.add('\t');
                            //                        System.out.println("n " +ncol);
                            for (int k = 1; k < ncol; k++) {

                                //System.out.println("srlength" + srlength);
                                queryFile1.seek(srlength);
                                int rlength = queryFile1.readByte();
                                //                        System.out.println("r"+rlength);

                                for (int i = 0; i < rlength; i++) {
                                    srlength += 1;
                                    queryFile1.seek(srlength);

                                    char b = (char) (queryFile1.read());

                                    row_data1.add(b);
                                    //                         
                                }
                                row_data1.add('\t');
                                srlength += 1;
                                row_data1.add('\t');

                                //rlength=queryFile.read();
                            }
                            //System.out.println(row_data);
                            //                start=start+2;
                            row_data1.add(';');
                        }
                        System.out.println(row_data1);
                        int y71 = 0;
                        for (int i = 0; i < names_col.size(); i++) {
                            System.out.print("" + ord_position.get(i));
                            System.out.print("\t\t");
                            y71++;
                        }
                        int lines = y71 * 16;
                        System.out.println();
                        //                    System.out.println(line("-", lines));
                        //System.out.println(row_data);
                        String final_data = "";
                        String column_data = "";
                        String column_wise[] = null;
                        for (int j = 0; j < row_data1.size(); j++) {
                            final_data = final_data + row_data1.get(j);
                        }

                        String row_wise[] = final_data.split(";");

                        //                    System.out.println("row_wise[k]");
                        for (int k = 0; k < row_wise.length; k++) {
                            //System.out.println(row_wise[k]);
                            //column_data=row_wise[k];
                            //row_wise[k].replace('<', '\t');
                            System.out.println(row_wise[k]);
                            //column_wise=column_data.split("^");
                        }
                        //System.out.println(names_col);
                    } else if ((names_col.contains(deciding_col))) {
                        ArrayList<Character> row_data = new ArrayList<>();

                        RandomAccessFile queryFile = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tbl_nm + "\\" + tbl_nm + ".tbl", "rw");

                        int start = 8;

                        queryFile.seek(start);
                        int a = 0;
                        int tem = queryFile.readShort();

                        while (tem != 0) {
                            if (-1 == tem) {
                                start = start + 2;//queryFile2.seek(start1);
                                queryFile.seek(start);

                                tem = queryFile.readShort();
                            } else {
                                queryFile.seek(start);

                                tem = queryFile.readShort();

                                String h = Integer.toHexString(tem);

                                queryFile.seek(tem);
                                String data = (Integer.toString(queryFile.read()));

                                queryFile.seek(tem + 5);
                                int ncol = queryFile.read();

                                int seek_rlength = tem + 5 + ncol + 1;

                                queryFile.seek(seek_rlength);

                                int first_length = queryFile.read();

                                for (int i = 0; i < first_length; i++) {
                                    a++;
                                    queryFile.seek(seek_rlength + 1);
                                    char b = (char) (queryFile.read());
                                    //row_data.add((String)Integer.toHexString(queryFile.read()));
                                    row_data.add(b);
                                }
                                //  System.out.println(row_data);
                                //  System.out.println("seek rlength"+seek_rlength + " first "+first_length);
                                int srlength = seek_rlength + first_length + 1;
                                row_data.add('\t');
                                row_data.add('\t');
                                for (int k = 1; k < ncol; k++) {
                            //a=tem+5+ncol+3;

                                    //System.out.println(srlength);
                                    queryFile.seek(srlength);
                                    int rlength = queryFile.read();
                                    //System.out.println("r"+rlength);

                                    for (int i = 0; i < rlength; i++) {
                                        srlength += 1;
                                        queryFile.seek(srlength);

                                        char b = (char) (queryFile.read());
                                        //row_data.add((String)Integer.toHexString(queryFile.read()));
                                        row_data.add(b);

                                    }
                                    row_data.add('\t');
                                    srlength += 1;
                                    row_data.add('\t');

                                    //rlength=queryFile.read();
                                }

                                start = start + 2;
                                row_data.add(';');
                                queryFile.seek(start);
                            }
                        }
                        int y7 = 0;
                        for (int i = 0; i < names_col.size(); i++) {
                            //                        System.out.print("ord_position1"+ord_position.get(i));
                            //                        System.out.print("\t\t");
                            y7++;
                        }

                        int lines = y7 * 16;
                        System.out.println();
                        //                    System.out.println(line("-", lines));
                        //System.out.println(row_data);
                        String final_data = "";
                        String column_data = "";
                        String column_wise[] = null;
                        for (int j = 0; j < row_data.size(); j++) {
                            final_data = final_data + row_data.get(j);
                        }

                        String row_wise[] = final_data.split(";");
                        ArrayList<String> tempo = new ArrayList<>();

                        //        System.out.println(tempo);
                        //                    System.out.println("roewise k for if loop");
                        for (int k = 0; k < row_wise.length - 1; k++) {
                            //System.out.println(row_wise[k]);
                            //column_data=row_wise[k];
                            //row_wise[k].replace('<', '\t');
                            if (row_wise[k].contains(comp_val)) {
                                System.out.println(row_wise[k]);
                            }
                            //column_wise=column_data.split("^");
                        }

                        ArrayList<Character> row_data11 = new ArrayList<>();
                        int start1 = 8;

                        queryFile.seek(start1);
                        int tem1 = queryFile.readShort();

                        while (tem1 != 0) {
                            if (-1 == tem) {
                                start = start + 2;//queryFile2.seek(start1);
                                queryFile.seek(start);

                                tem = queryFile.readShort();
                            } else {
                                queryFile.seek(start1);

                                tem1 = queryFile.readShort();

                                String h1 = Integer.toHexString(tem1);

                                queryFile.seek(tem1);
                                String data1 = (Integer.toString(queryFile.read()));

                                queryFile.seek(tem1 + 5);
                                int ncol1 = queryFile.read();

                                int seek_rlength1 = tem1 + 5 + ncol1 + 1;

                                queryFile.seek(seek_rlength1);

                                int first_length1 = queryFile.read();

                                for (int i = 0; i < first_length1; i++) {
                                    queryFile.seek(seek_rlength1 + 1);
                                    char b1 = (char) (queryFile.read());
                                    //row_data.add((String)Integer.toHexString(queryFile.read()));
                                    row_data11.add(b1);
                                }

                                int srlength1 = seek_rlength1 + first_length1 + 1;
                                row_data11.add('`');

                                for (int k = 1; k < ncol1; k++) {

                                    queryFile.seek(srlength1);
                                    int rlength1 = queryFile.read();

                                    for (int i = 0; i < rlength1; i++) {
                                        srlength1 += 1;
                                        queryFile.seek(srlength1);

                                        char b1 = (char) (queryFile.read());

                                        row_data11.add(b1);

                                    }

                                    srlength1 += 1;
                                    row_data11.add('`');

                                }

                                start1 = start1 + 2;
                                row_data11.add(';');
                                queryFile.seek(start1);
                                //System.out.println(row_data11);
                            }
                        }
                        int y77 = 0;
                        for (int i = 0; i < names_col.size(); i++) {
                            y7++;
                        }

                        String final_data1 = "";
                        String column_data1 = "";
                        String column_wise1[] = null;
                        for (int j = 0; j < row_data11.size(); j++) {
                            final_data1 = final_data1 + row_data11.get(j);
                        }
                        //System.out.println(final_data1);
                        //String row_wise11[]=final_data.split(";");
                        ArrayList<String> row_wise12 = new ArrayList<String>(Arrays.asList(final_data1.split(";")));
                        //  System.out.println(row_wise12);

                        int index_of_query_element = ord_position.indexOf(deciding_col);
                        ArrayList<Integer> cols_to_output = new ArrayList<>();

                        ArrayList<String> depth = new ArrayList<>();

                        ArrayList<String> colex = new ArrayList<>();
                        //                        System.out.println("rowwise"+row_wise12);
                        for (int k = 0; k < row_wise12.size() - 1; k++) {
                            //System.out.println(row_wise12.get(k));
                            String column_extract[] = row_wise12.get(k).split("`");
                            //System.out.println(column_extract[index_of_query_element]);
                            String temppp = column_extract[index_of_query_element];
                            //                    System.out.println("temp"+temppp);
                            colex.add(temppp);
                            depth = colex;
                            //System.out.println("colex"+colex);
                        }
                        //                        System.out.println("cp0"+colex);
                        int red = 0;
                        //if(colex.contains(comp_val)){

                        int fg = 0;
                        int row_no = Integer.parseInt(QueryStringTokens.get(7));
                        //String query_col=QueryStringTokens.get(5);
                        String operator = QueryStringTokens.get(6);
                        if (operator.equals("=")) {
                            fg = 1;
                        } else if (operator.equals("<")) {
                            fg = 2;
                        } else if (operator.equals(">")) {
                            fg = 3;
                        } else {
                            fg = 0;
                        }

                        if (fg == 1) {
                            for (String cv : colex) {

                                if (cv.equalsIgnoreCase(comp_val)) {
                                    cols_to_output.add(red);
                                    //System.out.println(row_wise12.get(red));
                                }
                                red++;
                            }
                        } else if (fg == 2) {
                            for (String cv : colex) {

                                if (Integer.parseInt(cv) < Integer.parseInt(comp_val)) {
                                    //                                System.out.println("cv"+cv + " " + comp_val);
                                    cols_to_output.add(red);
                                    //System.out.println(row_wise12.get(red));
                                }
                                red++;
                            }
                        } else if (fg == 3) {
                            for (String cv : colex) {

                                if (Integer.parseInt(cv) > Integer.parseInt(comp_val)) {
                                    //                                System.out.println("cv1"+cv + " " + comp_val);
                                    cols_to_output.add(red);
                                    //System.out.println(row_wise12.get(red));
                                }
                                red++;
                            }
                        }
                        //System.out.println(cols_to_output);

                        if (QueryStringTokens.get(1).equals("*")) {
                            int y74 = 0;
                            for (int i = 0; i < names_col.size(); i++) {
                                System.out.print("" + ord_position.get(i));
                                System.out.print("\t\t");
                                y74++;
                            }

                            int lines4 = y74 * 16;
                            System.out.println();
                            //                        System.out.println(line("-", lines4));
                            //                        System.out.println("row_wise12.get(cols_to_output.get(n)).replaceAll");
                            for (int n = 0; n < cols_to_output.size(); n++) {
                                //row_wise12.get(n).replace("`", "\t\t");
                                System.out.println(row_wise12.get(cols_to_output.get(n)).replaceAll("`", "\t\t"));
                            }
                        } else {
                            String col_name_q = QueryStringTokens.get(1);
                            int q = ord_position.indexOf(col_name_q);
                            ArrayList<String> colex1 = new ArrayList<>();
                            //                        System.out.println("rowwise"+row_wise12);
                            for (int k = 0; k < row_wise12.size() - 1; k++) {
                                //System.out.println(row_wise12.get(k));
                                String column_extract[] = row_wise12.get(k).split("`");
                                //System.out.println(column_extract[index_of_query_element]);
                                String temppp = column_extract[q];
                                //System.out.println("temp"+temppp);
                                colex1.add(temppp);
                                //depth = colex;
                                //System.out.println("colex"+colex);
                            }

                            //                        System.out.println("q"+q);
                            System.out.println("" + col_name_q);
                            int line7 = 16;
                            //System.out.println("------------------");
                            //                        System.out.println(line("-", line7));
                            for (int y = 0; y < colex1.size(); y++) {
                                System.out.println(colex1.get(cols_to_output.get(y)));
                            }
                        }
                        System.out.println(ord_position.indexOf("name"));
                    } else {
                        System.out.println("Column " + deciding_col + " not found in " + tbl_nm + " table");
                    }
                } else {
                    System.out.println("Syntax error....");
                }

            } catch (Exception ex) {
                //Logger.getLogger(DavisBase.class.getName()).log(Level.SEVERE, null, ex);
                //System.out.println("msg"+ex.getMessage());
            }
        } else {
            System.out.println("database is not selected.");
        }
    }

    private static void dropTable(String userCommand) {
        if (databaseName != null) {
            try {
                String tableName = userCommand.split(" ")[2];
//                System.out.println("tableName:" + tableName);

//                String str1 = "data\\user_data\\" + databaseName + "\\" + tableName;
                String str1 = "data\\user_data\\" + databaseName + "\\" + tableName;
                File file = new File(str1);
                deleteDirectory(file);
//                deleteDirectory(new File("data\\user_data\\"+databaseName));
                RandomAccessFile queryFile = new RandomAccessFile("data\\catalog\\davisbase_tables.tbl", "rw");

                int start = 8;
                int next = 6;
                int len;
                int startVal, nextVal;
                queryFile.seek(start);

                while (queryFile.readShort() != 0) {
                    String str = "";
                    queryFile.seek(start);
                    startVal = queryFile.readShort();
                    if (startVal == -1) {

                    } else {
                        startVal++;
                        queryFile.seek(next);
                        nextVal = queryFile.readShort();
                        if (nextVal == 0) {
                            nextVal = 512;
                        }
                        len = nextVal - startVal;
                        queryFile.seek(startVal);
                        ArrayList<Character> rowData = new ArrayList<Character>();
                        for (int i = 0; i < len; i++) {
                            queryFile.seek(startVal++);
                            char c = (char) (queryFile.read());
                            rowData.add(c);
                        }

                        for (int i = 0; i < rowData.size(); i++) {
                            str += rowData.get(i);
                        }
                        if (str.equals(tableName)) {
                            queryFile.seek(start);
                            queryFile.writeShort(0xFFFF);
                            System.out.println("'" + tableName + "' table has been deleted.");
                            break;
                        }
                    }
                    start += 2;
                    next += 2;
                    queryFile.seek(start);
                }

            } catch (Exception e) {
//                e.printStackTrace();
            }
        } else {
            System.out.println("database is not selected.");
        }
    }

    private static void dropDatabase(String userCommand) {
        String dbName = userCommand.split(" ")[2];
//                System.out.println("tableName:" + tableName);

//                String str1 = "data\\user_data\\" + databaseName + "\\" + tableName;
        String str1 = "data\\user_data\\" + dbName;
        File file = new File(str1);
        deleteDb(file);
        System.out.println("Database has been deleted.");
    }

    private static void deleteDirectory(File file) {
        if (file.isDirectory()) {
            File[] contentOfFile = file.listFiles();

            if (contentOfFile != null) {
                for (File f : contentOfFile) {
                    deleteDirectory(f);
                }
            }
        }
        System.out.println("now deleting"+ file.getAbsolutePath());
        file.delete();
        System.out.println("deleted");
        
    }

    private static void deleteDb(File file) {
        if (file.isDirectory()) {           
            File[] list = file.listFiles(); 
            if (list != null) {              
                for (int i = 0; i < list.length; i++) {
                    File tmpF1 = list[i];
                    if (tmpF1.isDirectory()) {   
                        deleteDb(tmpF1);
                    }
                    tmpF1.delete();             
                }
            }
//            if (!file.delete()) {            //if not able to delete folder print message
//                System.out.println("can't delete folder : " + file);
//            }
        }
    }
    
    private static void showColumns(String userCommand) {
        ArrayList<Character> row_data = new ArrayList<>();
        try {

            ArrayList<String> tmp = new ArrayList<>();

            RandomAccessFile queryFile = new RandomAccessFile("data\\catalog\\davisbase_columns.tbl", "rw");

            int start = 0x08;
            //System.out.println(start);

            queryFile.seek(start);
            int a = 0;
            int tem = queryFile.readShort();
            //System.out.println("tem"+tem);
            while (tem != 0) {

                queryFile.seek(start);

                tem = queryFile.readShort();

                start += 2;

                queryFile.seek(++tem);
                int row_id = queryFile.read();

                int seek_rlength = tem + 1;

                queryFile.seek(seek_rlength);

                int first_length = queryFile.read();
                //System.out.println("first"+first_length);             
                for (int i = 0; i < first_length; i++) {

                    queryFile.seek(++seek_rlength);
                    char b = (char) (queryFile.read());
                    //row_data.add((String)Integer.toHexString(queryFile.read()));
                    row_data.add(b);
                }
                //System.out.println(row_data);
                //  System.out.println("rl"+seek_rlength);
                //  System.out.println("seek rlength"+seek_rlength + " first "+first_length);
                int srlength = seek_rlength + 1;
                row_data.add(' ');

                for (int k = 2; k < 6; k++) {

                    queryFile.seek(srlength);
                    int rlength = queryFile.read();
                    //System.out.println("r"+rlength);

                    for (int i = 0; i < rlength; i++) {
                        srlength += 1;
                        queryFile.seek(srlength);

                        char b = (char) (queryFile.read());
                        //System.out.println("character: "+b);
                        //row_data.add((String)Integer.toHexString(queryFile.read()));
                        row_data.add(b);
                        //System.out.println(row_data);
                    }
                    row_data.add(' ');

                    srlength += 1;

                    rlength = queryFile.read();
                }
                //System.out.println(row_data);
                //start=start+2;
                row_data.add(';');
                queryFile.seek(start - 2);
                tem = queryFile.read();
                //                
            }
            //                int y7 = 0;
            //                for(int i=0;i<names_col.size();i++)
            //                {
            //                    System.out.print(ord_position.get(i));
            //                    System.out.print("\t\t");
            //                    y7++;
            //                }
            //                int lines = y7*16; 
            //                System.out.println();
            //                System.out.println(line("-",lines));
            // System.out.println(row_data);
            String final_data = "";
            String column_data = "";
            String column_wise[] = null;
            for (int j = 0; j < row_data.size(); j++) {
                final_data = final_data + row_data.get(j);
            }
            String row_wise[] = final_data.split(";");
            String[] key1;
            ArrayList<String> columns = new ArrayList<>();
            for (int k = 0; k < row_wise.length - 1; k++) {

                //System.out.println(row_wise[k]);
                key1 = row_wise[k].split(" ");

                for (int jo = 0; jo < key1.length; jo++) {
                    columns.add(key1[jo]);

                }
                //System.out.println("cols"+columns);

            }
            int try3 = 0;
            int try4 = 1;
            //System.out.println(columns.size());
            ArrayList<Integer> al = new ArrayList<>();
            ArrayList<String> al1 = new ArrayList<>();
            String day = null;

            for (int tre1 = 0; tre1 < columns.size(); tre1 += 5) {
                day = columns.get(tre1);
                if (!al1.contains(day)) {
                    al1.add(day);
                }
            }
            //                System.out.println(al1);
            for (int j = 0; j < al1.size(); j++) {
                int count = 0;
                for (int i = 0; i < columns.size(); i += 5) {
                    if ((columns.get(i).equalsIgnoreCase(al1.get(j)))) {
                        count++;
                        al.add(count);
                    }
                }
            }
            int ray1 = 3;
            for (int ray = 0; ray < columns.size() / 5; ray++) {
                columns.set(ray1, al.get(ray).toString());
                ray1 += 5;
            }
            ArrayList<String> headings = new ArrayList<String>();
            headings.add("row_id");
            headings.add("table_name");
            headings.add("column_name");
            headings.add("data_type");
            headings.add("ordinal_position");
            headings.add("is_nullable");

            for (int i = 0; i < headings.size(); i++) {
                System.out.format("%-20s", headings.get(i).toUpperCase());
            }
            System.out.println();
            System.out.println("-------------------------------------------------------------------------------------------------------------------");

            //System.out.println(columns);
            for (int try1 = 0; try1 < columns.size() / 5; try1++) {
                System.out.format("%-20s", try4);
                for (int try2 = try3; try2 < try3 + 5; try2++) {

                    System.out.format("%-20s", columns.get(try2).toUpperCase());

                }
                System.out.println();
                try4++;
                try3 += 5;
            }

        } catch (Exception e) {
            //System.out.println(e.getMessage());
        }

    }

    private static void showTables(String userCommand) {
        if (databaseName != null) {
            try {

                System.out.println("STUB: Calling parseQueryString(String s) to show tables");
                System.out.println("Parsing the string:\"" + userCommand + "\"");
                System.out.println("+---------------------+");
                System.out.println("|Table Names          |");
                System.out.println("+---------------------+");
                File file = new File("data\\user_data\\" + databaseName);
                String[] tb_names = file.list();
                for (String name : tb_names) {
                    if (new File("data\\user_data\\" + databaseName + "\\" + name).isDirectory()) {
                        //                    System.out.println(name);
                        System.out.format("|%-20s%3s", name.toUpperCase(), "|\n");
                        //                    System.out.println("\n-----------------------");
                    }
                }
                System.out.println("+---------------------+");
                //            System.out.println("davisbase_columns");
                //            System.out.println("davisbase_tables");

            } catch (Exception e) {

            }
        } else {
            System.out.println("database is not selected.");
        }
    }

    private static void showDatabases(String userCommand) {
        try {

            System.out.println("STUB: Calling parseQueryString(String s) to show databases");
            System.out.println("Parsing the string:\"" + userCommand + "\"");
            System.out.println("+---------------------+");
            System.out.println("|Database Names          |");
            System.out.println("+---------------------+");
            File file = new File("data\\user_data\\");
            String[] tb_names = file.list();
            for (String name : tb_names) {
                if (new File("data\\user_data\\" + name).isDirectory()) {
                    //                    System.out.println(name);
                    System.out.format("|%-20s%3s", name.toUpperCase(), "|\n");
                    //                    System.out.println("\n-----------------------");
                }
            }
            System.out.println("+---------------------+");
            //            System.out.println("davisbase_columns");
            //            System.out.println("davisbase_tables");

        } catch (Exception e) {

        }
    }

    private static void insertIntoTable(String userCommand) {
        if (databaseName != null) {
            String insertQuery = userCommand.trim();
            System.out.println("insert query :" + insertQuery);
            String insertQueryTokens[] = insertQuery.replace('(', '#').replace(')', ' ').split("#");
//            printTokens(insertQueryTokens);

            String tableName = insertQueryTokens[0].trim().split(" ")[2];
//            System.out.println("table name:" + tableName);
            //        System.out.println("(id, name):"+insertQueryTokens[1].replaceAll("values", ""));
            int try1 = 0;
            int is_pk = 0;

            File ftbl = new File("data\\user_data\\" + databaseName + "\\" + tableName);
            if (ftbl.exists()) {
                String inserttemp1 = insertQueryTokens[1].replaceAll("values", "").replaceAll(" ", "").trim();
                String inserttemp2 = insertQueryTokens[2].replaceAll(" ", "").trim();

                String columnNames[] = inserttemp1.split(",");
                String columnValues[] = inserttemp2.split(",");

                ArrayList<String> columnNames1 = new ArrayList<String>();
                ArrayList<String> columnValues1 = new ArrayList<String>();

                for (int i = 0; i < columnNames.length; i++) {
                    columnNames1.add(columnNames[i]);
                }
                for (int i = 0; i < columnValues.length; i++) {
                    columnValues1.add(columnValues[i]);
                }

                //remaining column names
                ArrayList<String> remainingColumnNames = new ArrayList<String>();
                Boolean length = true;
                int diff = columnNames.length - columnValues.length;
//                System.out.println("diff:" + diff);
                if (columnNames.length < columnValues.length) {
                    length = false;
                } else {
                    for (int vl = columnNames.length; vl > (columnNames.length - diff); vl--) {
                        String remainingName = columnNames[vl - 1];
//                        System.out.println("remain:" + remainingName);
                        remainingColumnNames.add(remainingName);
                    }
                }

                ArrayList<String> nameOfColumns = new ArrayList<String>();
                File[] fileNames = ftbl.listFiles();
                int flag = 0;
                int nullFlag = 0;

                int crecord_size = 0;

                ArrayList<String> tmp = new ArrayList<>();

                for (int i = 0; i < fileNames.length; i++) {
                    if (fileNames[i].isFile()) {
                        String ndx_names = fileNames[i].getName();
                        if (ndx_names.contains(".ndx")) {
                            nameOfColumns.add(ndx_names.substring(0, ndx_names.indexOf(".")));
                        }
                    }
                }
                tmp = nameOfColumns;
                // System.out.println(tmp);

                //check if column names are present ot not 
                for (int m = 0; m < columnNames1.size(); m++) {
                    if (nameOfColumns.contains(columnNames1.get(m))) {
                        flag = 1;
                    } else {
                        flag = 0;
                        break;
                    }
                }
                //for remaining columns names detections
                for (int j = 0; j < columnNames1.size(); j++) {
                    if (tmp.contains(columnNames1.get(j))) {
                        tmp.remove(columnNames1.get(j));
                    }
                }
                for (int r = 0; r < tmp.size(); r++) {
                    remainingColumnNames.add(tmp.get(r));
                }

                ArrayList<Integer> t = new ArrayList<>();
                ArrayList<Integer> t1 = new ArrayList<>();

                // check whether remaining columns names are primary key or not
                for (int h = 0; h < remainingColumnNames.size(); h++) {
                    try {
                        RandomAccessFile cols = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tableName + "\\" + remainingColumnNames.get(h) + ".ndx", "rw");
                        cols.seek(0);
                        try1 = cols.read();
                        //                           System.out.println(try1);
                        t.add(try1);
                        //                        System.out.println("d "+t);
                        cols.seek(1);
                        is_pk = cols.read();
                        t1.add(is_pk);
                        //                        System.out.println("n "+cols.read());
                    } catch (Exception e) {
                        System.out.println(tableName + " -> " + remainingColumnNames.get(h) + ".ndx file not found!!" + e);
                    }
                }

                //set null flag according to primary key (tl conatains 0 or 1)
                if (t1.contains(0)) {
                    nullFlag = 0;
                } else {
                    nullFlag = 1;
                }

                ArrayList<Integer> null_type = new ArrayList<>();
                if (flag == 1 && length) {
                    if (nullFlag == 1) {
                        System.out.println("Query OK. 1 row affected.");
                        for (int y = 0; y < t.size(); y++) {
                            if (t.get(y) == 0x04) {
                                null_type.add(1);
                            } else if (t.get(y) == 0x05) {
                                null_type.add(2);
                            } else if (t.get(y) == 0x06 || t.get(y) == 0x08) {
                                null_type.add(4);
                            } else if (t.get(y) == 0x09 || t.get(y) == 0x0A || t.get(y) == 0x0B) {
                                null_type.add(8);
                            } else {
                                null_type.add(0);
                            }
                        }

                        //   System.out.println(null_type);
                        //Actual data inserting...
                        for (int g = 0; g < columnNames.length; g++) {
                            try {
                                RandomAccessFile cols_given = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tableName + "\\" + columnNames[g] + ".ndx", "rw");
                                cols_given.seek(0);
                                int data_type = cols_given.read();
                                if (data_type == 0x04) {
                                    crecord_size += 1;
                                } else if (data_type == 0x05) {
                                    crecord_size += 2;
                                } else if (data_type == 0x06) {
                                    crecord_size += 4;
                                } else if (data_type == 0x07) {
                                    crecord_size += 8;
                                } else if (data_type == 0x08) {
                                    crecord_size += 4;
                                } else if (data_type == 0x09) {
                                    crecord_size += 8;
                                } else if (data_type == 0x0A) {
                                    crecord_size += 8;
                                } else if (data_type == 0x0B) {
                                    crecord_size += 8;
                                } else if (data_type == 0x0C) {

                                    crecord_size += columnValues[g].length();
                                }
                            } catch (Exception ex) {
                                //Logger.getLogger(DavisBase.class.getName()).log(Level.SEVERE, null, ex);
                            }

                        }
                        // System.out.println("Record size:"+crecord_size);
                        nameOfColumns.clear();
                        for (int i = 0; i < fileNames.length; i++) {
                            if (fileNames[i].isFile()) {
                                String ndx_names = fileNames[i].getName();
                                if (ndx_names.contains(".ndx")) {
                                    nameOfColumns.add(ndx_names.substring(0, ndx_names.indexOf(".")));
                                }

                            }
                        }
                        ArrayList<String> ord_values = new ArrayList<>();
                        int null_byte = 0;
                        for (int o = 0; o < nameOfColumns.size() + 15; o++) {
                            ord_values.add(".");
                        }

                        ArrayList<String> ord_values_type = new ArrayList<>();

                        for (int o = 0; o < nameOfColumns.size() + 15; o++) {
                            ord_values_type.add(".");
                        }
                        String null_print = null;
                        //   System.out.println("diff "+diff);
                        // set other params to ~
                        if (diff >= 0) {
                            for (int u = 0; u < remainingColumnNames.size(); u++) {
                                try {
                                    RandomAccessFile rcols_type = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tableName + "\\" + remainingColumnNames.get(u) + ".ndx", "rw");
                                    rcols_type.seek(2);
                                    int asd = rcols_type.read();
                                    //   System.out.println("ordinal pos"+asd);
                                    rcols_type.seek(0);
                                    int dtype = rcols_type.read();
                                    // System.out.println("type"+dtype);
                                    if (dtype == 0x04) {
                                        ord_values.set(asd, "~");
                                        null_byte += 1;
                                    } else if (dtype == 0x05) {
                                        null_byte += 2;
                                        ord_values.set(asd, "~");
                                    } else if (dtype == 0x06 || dtype == 0x08) {
                                        null_byte += 4;
                                        ord_values.set(asd, "~");
                                    } else if (dtype == 0x09 || dtype == 0x0A || dtype == 0x0B) {
                                        null_byte += 8;
                                        ord_values.set(asd, "~");
                                    } else {
                                        null_byte += 0;
                                        ord_values.set(asd, "~");
                                    }
                                } catch (Exception e) {
//                                    e.printStackTrace();
                                }
                            }

                        }

                        int bytes_req = 2 + 4 + (2 * nameOfColumns.size()) + 1 + crecord_size + null_byte;

//                        System.out.println("Size:" + ord_values.size());
                        for (int i = 0; i < columnValues1.size(); i++) {
                            try {
                                RandomAccessFile cols_ord = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tableName + "\\" + columnNames1.get(i) + ".ndx", "rw");
                                cols_ord.seek(2);
                                int ord = cols_ord.read();
                                cols_ord.seek(0);
                                int dtype = cols_ord.read();
//                                System.out.println("columnname.get(i):" + dtype);
                                // for date purpose
                                if (dtype == 0x0B) {
                                    ZoneId zoneId = ZoneId.of ( "America/Chicago" );
                                    String[] str1 = columnValues[i].split("-");
                                    ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(str1[0]),Integer.parseInt(str1[1]),Integer.parseInt(str1[2]),0,0,0,0, zoneId );
                                    String str = zdt.toLocalDate().toString();
//                                    columnValues[i] = convertStringToDate(columnValues[i]);

//                                    columnValues[i] = "hello";
//                                    String pattern = "MM:dd:yyyy";
//                                    SimpleDateFormat format = new SimpleDateFormat(pattern);
//                                    Date d = new Date(columnValues[i]);
//                                    String str = format.format(d);
                                    columnValues[i] = str;

                                    System.out.println("Datestr:"+str);
                                } // for date time purpose
                                else if (dtype == 0x0A) {

                                    String pattern = "YYYY-MM-DD_hh:mm:ss";
                                    SimpleDateFormat format = new SimpleDateFormat(pattern);
                                    Date d = new Date(columnValues[i]);
                                    columnValues[i] = format.format(d);
                                }
                                ord_values.set(ord, columnValues[i]);
                                //System.out.println(ord_values);
                            } catch (Exception e) {
//                                e.printStackTrace();
                            }
                        }

                        // update tableName.tbl
                        try {
                            RandomAccessFile insert_table = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tableName + "\\" + tableName + ".tbl", "rw");
                            insert_table.setLength(pageSize);
                            insert_table.seek(1);
                            int row_id = insert_table.read();
                            //          System.out.println("row id"+row_id);
                            row_id++;
                            insert_table.seek(1);
                            insert_table.write(row_id);

                            insert_table.seek(2);
                            psi = insert_table.readShort();
                            psi = psi - bytes_req;
                            //           System.out.println(psi);
                            insert_table.seek(2);
                            insert_table.writeShort((int) psi);
                            insert_table.seek(psi);
                            insert_table.writeByte(crecord_size);
                            insert_table.writeInt(row_id);
                            int y7 = 0;
                            for (int y = 0; y < nameOfColumns.size(); y++) {
                                y7++;
                            }
                            //        System.out.println("colnames"+names_of_col);
                            insert_table.write(y7);
                            for (int d = 0; d < nameOfColumns.size(); d++) {
                                RandomAccessFile cols_dt = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tableName + "\\" + nameOfColumns.get(d) + ".ndx", "rw");
                                cols_dt.seek(0);
                                int dt = cols_dt.read();

                                cols_dt.seek(2);
                                int op = cols_dt.read();
                                ord_values_type.set(op, Integer.toString(dt));
                                //insert_table.write(dt);
                            }
                            for (int o = 0; o < ord_values_type.size(); o++) {
                                if (ord_values_type.get(o).equalsIgnoreCase(".")) {

                                } else {
                                    //insert_table.writeByte(ord_values_type.get(o).length());
                                    insert_table.writeByte(Integer.parseInt(ord_values_type.get(o)));
                                }
                            }
                            for (int o = 0; o < ord_values.size(); o++) {
                                if (ord_values.get(o).equalsIgnoreCase(".")) {

                                } else {
                                    insert_table.writeByte(ord_values.get(o).length());
                                    insert_table.writeBytes(ord_values.get(o));
                                }
                            }
                            insert_table.seek(8 + 2 * row_id - 2);
                            insert_table.writeShort((int) psi);

                            // update colName.ndx file
                            for (int cc = 0; cc < nameOfColumns.size(); cc++) {
                                try {
                                    RandomAccessFile rcols_records = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tableName + "\\" + nameOfColumns.get(cc) + ".ndx", "rw");
                                    rcols_records.seek(3);
                                    //rcols_records.writeBytes("\n");
                                    int psx = rcols_records.readShort();
                                    rcols_records.seek(2);
                                    int ordp = rcols_records.read();
                                    rcols_records.seek(psx);
                                    rcols_records.writeByte(row_id);
                                    rcols_records.writeBytes(ord_values.get(ordp));
                                    psx += 16;
                                    rcols_records.seek(3);
                                    rcols_records.writeShort(psx);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }

                        } catch (Exception e) {
//                            e.printStackTrace();
                        }
                    } else {
                        System.out.println("Primary key connot have null values or Null Values not allowed in not_nullable column.");
                    }
                } else {
                    System.out.println("Column does not exist.....");
                }
            } else {
                System.out.println(tableName + " does not exists!!!");
            }
        } else {
            System.out.println("database is not selected.");
        }
    }

    private static void showHelpToUsers() {
        System.out.println("*************************************************************************************************************");
        System.out.println("************************************  Welcome to Help Center ************************************************");
        System.out.println("SUPPORTED COMMANDS LIST");
        System.out.println("All command are case insensitive");
        System.out.println("\tSET PROMT promt_name;                                                     Set promt name according to user's choice.");
        System.out.println("\tUSE database_name;                                                        Database selection.");
        System.out.println("\tCREATE TABLE table_name (data_type1 column_name1,data_type2 column_name2); Create table.");
        System.out.println("\tSELECT * FROM table_name;                                                 Display all records in the table.");
        System.out.println("\tSELECT * FROM table_name WHERE rowid operator <value>;                    Display records whose rowid is <id>.");
        System.out.println("\tSELECT col_name FROM table_name;                                          Display records whose rowid is <id>.");
        System.out.println("\tINSERT INTO TABLE [col_names] table_name VALUES [values];                 Display records whose rowid is <id>.");
        System.out.println("\tDROP TABLE table_name;                                                    Remove table data and its schema.");
        System.out.println("\tDELETE FROM table_name WHERE rowid = value;                               Delete records from table.");
        System.out.println("\tUPDATE table_name SET col_name = col_value WHERE col_name = col_value;    Show the program version.");
        System.out.println("\tSELECT * FROM davisbase_columns;                                          Show the details of davisbase_columns(describe the columns).");
        System.out.println("\tVERSION;                                                                  Show the program version.");
        System.out.println("\tHELP;                                                                     Show this help information");
        System.out.println("\tEXIT;                                                                     Exit the program");
        System.out.println("********************************************************************************************************");
    }

    private static void useDatabase(String userCommand) {
//        System.out.println(userCommand.length());
        ArrayList<String> dbTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
//        System.out.println("db:"+dbTokens.size());
        if (dbTokens.size() <= 2) {
//            System.out.println("in if");
            String dbname = dbTokens.get(1);
            File file = new File("data\\user_data\\");
            String[] str = file.list();
            boolean databaseFound = false;
            for (String db : str) {
                if (new File("data\\user_data\\" + dbname).isDirectory()) {
//                    System.out.println("database chnaged.......");
                    databaseFound = true;
                }
            }
            if (databaseFound) {
                System.out.println("database changed");
                databaseName = dbname;
            } else {
                System.out.println("not found db");
            }
//            databaseName = dbTokens.get(1);
//            System.out.println("Database changed.");
        } else {
            System.out.println("in correct syntax");
        }
    }

    private static void createTables1(String userCommand) {
        System.out.println("database name : " + databaseName);
        String columnName;
        ArrayList<String> listOfColumnName = new ArrayList<>();
        System.out.println("Creating the table with Query : " + userCommand + ";");
        ArrayList<String> tableTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
//        System.out.println("tokens:"+tableTokens);
//        System.out.println("tabletookens:" + tableTokens.get(1) + tableTokens.size());

        if (tableTokens.size() < 4) {
            System.out.println("creating database");
            try {
                String databaseName1 = tableTokens.get(2);
                File file = new File("data\\user_data\\" + databaseName1);
                boolean isDatabaseEsixts = file.exists();
                if (!isDatabaseEsixts) {
                    file.mkdir();
                    System.out.println(databaseName1 + "is successfully created.");
                } else {
                    System.out.println(databaseName1 + " is already exists!!");
                }
            } catch (Exception e) {
                System.out.println("error : " + e);
            }

        } else {
            if (databaseName != null) {
                System.out.println("creating table");
                String tableName1 = tableTokens.get(2);
                String bracketStuff[] = tableName1.replace("(", " ").split(" ");
                //        System.out.println("temp[]:"+temp[1]);
                String tableFileName = bracketStuff[0] + ".tbl";
                String tableName = bracketStuff[0];
                //        System.out.println(tableName+"     "+tableFileName);

                int occurence;
                occurence = Collections.frequency(tableTokens, "primary");
                String dataTypeString;
                int recordSize = 0;
                int serialCode = 0;

                //create database
                try {
//                    File file = new File("data\\user_data\\" + bracketStuff[0]);
                    File file = new File("data\\user_data\\" + databaseName + "\\" + bracketStuff[0]);
                    //            System.out.println(file.getPath());
                    boolean isfileExist = file.exists();
                    //            System.out.println("file:"+fileExist);
                    String tableArguments = userCommand.substring(userCommand.indexOf("(") + 1, userCommand.length() - 1);
                    String dataType[] = tableArguments.split(","); //like (int id, int id1) => int id and int id1

                    int ordinalPos1 = 0;

                    if (!isfileExist) {
                        if (occurence <= 1) {
                            Boolean isDataType = false;
                            String dataTypeName;
                            for (int i = 0; i < dataType.length; i++) {
                                dataTypeString = dataType[i].trim();

                                //                        System.out.println("dataTypeString:" + dataTypeString);
                                int cnt = 0;
                                String temp1[] = dataTypeString.split(" ");
                                //                        System.out.println("temp1[0] : " + temp1[0] + "temp1[1] :" + temp1[1]);
                                //                        System.out.println("length : " + temp1.length);

                                dataTypeName = temp1[1];

                                //                        System.out.println("datatypeanme : " + dataTypeName);
                                //                        columnName = temp1[1];
                                columnName = dataTypeString.substring(0, dataTypeString.indexOf(" "));
                                //                        System.out.println("columnName:" + columnName);
                                listOfColumnName.add(columnName);

                                isDataType = checkDataType(dataTypeName);
                                //                        System.out.println("is Data type : " + isDataType);
                                if (isDataType == false) {
                                    System.out.println("Incorrect syntax");
                                    break;

                                } else {

                                    if (dataTypeName.equalsIgnoreCase("int")) {
                                        recordSize = recordSize + 4;
                                        serialCode = 0x06;
                                    } else if (dataTypeName.equalsIgnoreCase("tinyint")) {
                                        recordSize = recordSize + 1;
                                        serialCode = 0x04;
                                    } else if (dataTypeName.equalsIgnoreCase("bigint")) {
                                        recordSize = recordSize + 8;
                                        serialCode = 0x07;
                                    } else if (dataTypeName.equalsIgnoreCase("smallint")) {
                                        recordSize = recordSize + 2;
                                        serialCode = 0x05;
                                    } else if (dataTypeName.equalsIgnoreCase("real")) {
                                        recordSize = recordSize + 4;
                                        serialCode = 0x08;
                                    } else if (dataTypeName.equalsIgnoreCase("double")) {
                                        recordSize = recordSize + 8;
                                        serialCode = 0x09;
                                    } else if (dataTypeName.equalsIgnoreCase("datetime")) {
                                        recordSize = recordSize + 8;
                                        serialCode = 0x0A;
                                    } else if (dataTypeName.equalsIgnoreCase("date")) {
                                        recordSize = recordSize + 8;
                                        serialCode = 0x0B;
                                    } else if (dataTypeName.equalsIgnoreCase("text")) {
                                        serialCode = 0x0C;
                                    }
                                    //                        System.out.println("record size:" + recordSize);
                                    int dataTypeLength = dataTypeName.length();
                                    int colLength = columnName.length();
                                    int tblLength = bracketStuff[0].length();
                                    int isNullLength = 0;
                                    int primaryKeyOccurence = 0;
                                    String pk = null;
                                    String primary = null;

                                    int ordinalPos = 0;

                                    if (dataTypeString.toLowerCase().contains("primary key") || dataTypeString.toLowerCase().contains("[not null]")) {
                                        if (dataTypeString.toLowerCase().contains("primary key")) {
                                            isNullLength = 3;
                                            pk = "PRI";
                                        } else {
                                            isNullLength = 2;
                                            pk = "NO";
                                        }
                                    } else {
                                        isNullLength = 3;
                                        pk = "YES";
                                    }
                                    //                        System.out.println("tbllength:"+tblLength+"collength:"+colLength+"datatypelength"+dataTypeLength+"isNulllength:"+isNullLength);
                                    int totalSize = 1 + tblLength + colLength + dataTypeLength + 1 + isNullLength + 6;
                                    //                        System.out.println("totalSize : " + totalSize);
                                    //                        System.out.println("col name:"+columnName);
                                    if (isDataType && occurence <= 1) {
                                        file.mkdir();
                                        RandomAccessFile colFile1 = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tableName + "\\" + columnName + ".ndx", "rw");

                                        colFile1.setLength(pageSize);
                                        colFile1.seek(0);
                                        colFile1.writeByte(serialCode);
                                        int nullVal;
                                        if (pk.equalsIgnoreCase("yes")) {
                                            nullVal = 1;
                                        } else {
                                            nullVal = 0;
                                        }
                                        colFile1.writeByte(nullVal);
                                        //                            System.out.println("ordinalpos1:" + ordinalPos1);
                                        colFile1.write(ordinalPos1);
                                        ordinalPos1++;
                                        colFile1.writeByte(0x00);
                                        colFile1.writeByte(0x10);
                                    // column name .ndx writing is done here.....................

                                        //davisbase columns.tbl creation
                                        RandomAccessFile colFile = new RandomAccessFile("data\\catalog\\davisbase_columns.tbl", "rw");
                                        colFile.setLength(2048);

                                        colFile.seek(1);
                                        int columnNumber = colFile.read();

                                        colFile.seek(2);
                                        int b1 = colFile.readShort();

                                        colFile.seek(2);
                                        colFile.writeShort((int) (b1 - totalSize));
                                        //                            System.out.println("bl :" + b1);
                                        //                            System.out.println("bl-totalSize: " + (b1 - totalSize) + "totalSize: " + totalSize);

                                        colFile.seek(b1 - totalSize);
                                        columnNumber++;
                                        colFile.writeByte(1);
                                        colFile.writeByte(columnNumber);

                                        colFile.writeByte(tblLength);
                                        colFile.writeBytes(tableName);

                                        colFile.writeByte(colLength);
                                        colFile.writeBytes(columnName);

                                        colFile.writeByte(dataTypeLength);
                                        colFile.writeBytes(temp1[1]);

                                        colFile.writeByte(1);
                                        ordinalPos++;
                                        colFile.writeByte(ordinalPos);

                                        colFile.writeByte(isNullLength);
                                        colFile.writeBytes(pk);

                                        colFile.seek(1);
                                        colFile.writeByte(columnNumber);

                                        colFile.seek(2);
                                        colFile.writeShort((int) (b1 - totalSize));

                                        colFile.seek(8 + columnNumber * 2 - 2);
                                        colFile.writeShort(b1 - totalSize);
                                    }
                                    if (isDataType && occurence <= 1) {
                                        file.mkdir();
                                        RandomAccessFile tf = new RandomAccessFile("data\\catalog\\davisbase_tables.tbl", "rw");
                                        tf.seek(1);
                                        int rec_cnt = tf.read();
                                        tf.seek(2);
                                        ps = tf.readShort();
                                        tf.setLength(pageSize);
                                        ps = ps - tableName.length() - 1;
                                        tf.seek(ps);
                                        rec_cnt = rec_cnt + 1;
                                        tf.writeByte(rec_cnt);
                                        //System.out.println(rec_cnt);
                                        tf.writeBytes(tableName);
                                        tf.seek(1);
                                        tf.writeByte(rec_cnt);
                                        tf.seek(2);
                                        tf.writeShort((int) (ps));
                                        tf.seek(8 + rec_cnt * 2 - 2);
                                        tf.writeShort((int) ps);

                                        RandomAccessFile tableFile = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + tableName + "\\" + tableName + ".tbl", "rw");
                                        tableFile.setLength(pageSize);
                                        tableFile.seek(0);
                                        tableFile.write(0x0d);
                                        tableFile.seek(1);
                                        int col_rec_count = tableFile.readByte();
                                        int size_col_header = 0;
                                        if (col_rec_count == 0) {
                                            ps_col = 512;
                                            //                                                tableFile.seek(2);
                                            //                                                tableFile.writeShort((int)(ps_col-1));
                                            //                                                tableFile.seek(2);
                                            //                                                ps_col = tableFile.readShort();
                                            for (int cl = 0; cl < listOfColumnName.size(); cl++) {
                                                size_col_header += listOfColumnName.get(cl).length();
                                            }
                                            tableFile.seek(ps_col - size_col_header - listOfColumnName.size());

                                            for (int wr = 0; wr < listOfColumnName.size(); wr++) {
                                                tableFile.writeByte(listOfColumnName.get(wr).length());
                                                tableFile.writeBytes(listOfColumnName.get(wr));
                                            }
                                            ps_col = ps_col - size_col_header - listOfColumnName.size();
                                            tableFile.seek(2);
                                            tableFile.writeShort((int) ps_col);
                                        } else {

                                        }
                                    }
                                }

                            }
                        } else {
                            System.out.println("A table cannot have two primary keys.....");
                        }
                        System.out.println("'" + tableName.toUpperCase() + "' TABLE is successfully created...");
                    } else {
                        System.out.println(tableName + " is already exists.");
                    }

                } catch (Exception e) {
//                    e.printStackTrace();
                }
            } else {
                System.out.println("Database is not selected.");
            }
        }
    }

    private static Boolean checkDataType(String dataTypeName) {
        try {
            switch (dataTypeName) {
                case "int":
                case "text":
                case "double":
                case "date":
                case "smallint":
                case "tinyint":
                case "bigint":
                case "null":
                case "datetime":
                case "real":
                    return true;
                default:
                    throw new Exception("Invalid DataType!!!!!!!!!!!");
            }
        } catch (Exception e) {
//            e.printStackTrace();
            return false;
        }
    }

    private static void createDataDirectory() {
        File f1 = new File("data");
        String path = "data\\";
        File sf = new File(path + "catalog");
        File usf = new File(path + "user_data");

        if (!f1.exists()) {
            boolean res = false;
            try {
                f1.mkdir();
                sf.mkdir();

                RandomAccessFile tbl_details = new RandomAccessFile("data\\catalog\\davisbase_tables.tbl", "rw");
                tbl_details.setLength(pageSize);
                RandomAccessFile col_details = new RandomAccessFile("data\\catalog\\davisbase_columns.tbl", "rw");
                col_details.setLength(pageSizec);
                int l = "davisbase_tables".length();
                int c = "davisbase_columns".length();

                ps = ps - l - 1;
                tbl_details.seek(ps);
                tbl_details.writeByte(1);
                tbl_details.writeBytes("davisbase_tables");

                ps = ps - c - 1;
                tbl_details.seek(ps);
                tbl_details.writeByte(2);
                tbl_details.writeBytes("davisbase_columns");

                tbl_details.seek(1);
                tbl_details.writeByte(02);

                tbl_details.seek(2);
                tbl_details.writeShort(477);

                tbl_details.seek(8);
                tbl_details.writeShort(495);

                tbl_details.seek(10);
                tbl_details.writeShort(477);
                        //tbl_details.writeBytes(" ");

                //pageSizec=2048;
                col_details.setLength(pageSizec);
                col_details.seek(8);
                col_details.writeShort((int) (pageSizec - 34));
                col_details.seek(pageSizec - 34);

                col_details.writeByte(1);
                col_details.writeByte(1);

                col_details.writeByte(16);
                col_details.writeBytes("davisbase_tables");

                col_details.writeByte(5);
                col_details.writeBytes("rowid");

                col_details.writeByte(3);
                col_details.writeBytes("INT");

                col_details.writeByte(1);
                col_details.writeByte(1);

                col_details.writeByte(2);
                col_details.writeBytes("NO");

                col_details.seek(2);
                col_details.writeShort((int) (pageSizec - 34));

                pageSizec = pageSizec - 34;
                col_details.seek(10);
                col_details.writeShort((int) (pageSizec - 40));
                col_details.seek(pageSizec - 40);

                col_details.writeByte(1);
                col_details.writeByte(2);

                col_details.writeByte(16);
                col_details.writeBytes("davisbase_tables");

                col_details.writeByte(10);
                col_details.writeBytes("table_name");

                col_details.writeByte(4);
                col_details.writeBytes("TEXT");

                col_details.writeByte(1);
                col_details.writeByte(2);

                col_details.writeByte(2);
                col_details.writeBytes("NO");

                col_details.seek(2);
                col_details.writeShort((int) (pageSizec - 40));

                pageSizec = pageSizec - 40;
                col_details.seek(12);
                col_details.writeShort((int) (pageSizec - 35));
                col_details.seek(pageSizec - 35);

                col_details.writeByte(1);
                col_details.writeByte(3);

                col_details.writeByte(17);
                col_details.writeBytes("davisbase_columns");

                col_details.writeByte(5);
                col_details.writeBytes("rowid");

                col_details.writeByte(3);
                col_details.writeBytes("INT");

                col_details.writeByte(1);
                col_details.writeByte(1);

                col_details.writeByte(2);
                col_details.writeBytes("NO");

                col_details.seek(2);
                col_details.writeShort((int) (pageSizec - 35));

                pageSizec = pageSizec - 35;
                col_details.seek(14);
                col_details.writeShort((int) (pageSizec - 41));
                col_details.seek(pageSizec - 41);

                col_details.writeByte(1);
                col_details.writeByte(4);

                col_details.writeByte(17);
                col_details.writeBytes("davisbase_columns");

                col_details.writeByte(10);
                col_details.writeBytes("table_name");

                col_details.writeByte(4);
                col_details.writeBytes("TEXT");

                col_details.writeByte(1);
                col_details.writeByte(2);

                col_details.writeByte(2);
                col_details.writeBytes("NO");

                col_details.seek(2);
                col_details.writeShort((int) (pageSizec - 41));

                pageSizec = pageSizec - 41;
                col_details.seek(16);
                col_details.writeShort((int) (pageSizec - 42));
                col_details.seek(pageSizec - 42);

                col_details.writeByte(1);
                col_details.writeByte(4);

                col_details.writeByte(17);
                col_details.writeBytes("davisbase_columns");

                col_details.writeByte(11);
                col_details.writeBytes("column_name");

                col_details.writeByte(4);
                col_details.writeBytes("TEXT");

                col_details.writeByte(1);
                col_details.writeByte(3);

                col_details.writeByte(2);
                col_details.writeBytes("NO");

                col_details.seek(2);
                col_details.writeShort((int) (pageSizec - 42));

                pageSizec = pageSizec - 42;
                col_details.seek(18);
                col_details.writeShort((int) (pageSizec - 40));
                col_details.seek(pageSizec - 40);

                col_details.writeByte(1);
                col_details.writeByte(6);

                col_details.writeByte(17);
                col_details.writeBytes("davisbase_columns");

                col_details.writeByte(9);
                col_details.writeBytes("data_type");

                col_details.writeByte(4);
                col_details.writeBytes("TEXT");

                col_details.writeByte(1);
                col_details.writeByte(4);

                col_details.writeByte(2);
                col_details.writeBytes("NO");

                col_details.seek(2);
                col_details.writeShort((int) (pageSizec - 40));

                pageSizec = pageSizec - 40;
                col_details.seek(20);
                col_details.writeShort((int) (pageSizec - 50));
                col_details.seek(pageSizec - 50);

                col_details.writeByte(1);
                col_details.writeByte(7);

                col_details.writeByte(17);
                col_details.writeBytes("davisbase_columns");

                col_details.writeByte(16);
                col_details.writeBytes("ordinal_position");

                col_details.writeByte(7);
                col_details.writeBytes("TINYINT");

                col_details.writeByte(1);
                col_details.writeByte(5);

                col_details.writeByte(2);
                col_details.writeBytes("NO");

                col_details.seek(2);
                col_details.writeShort((int) (pageSizec - 50));

                pageSizec = pageSizec - 50;
                col_details.seek(22);
                col_details.writeShort((int) (pageSizec - 42));
                col_details.seek(pageSizec - 42);

                col_details.writeByte(1);
                col_details.writeByte(8);

                col_details.writeByte(17);
                col_details.writeBytes("davisbase_columns");

                col_details.writeByte(11);
                col_details.writeBytes("is_nullable");

                col_details.writeByte(4);
                col_details.writeBytes("TEXT");

                col_details.writeByte(1);
                col_details.writeByte(5);

                col_details.writeByte(2);
                col_details.writeBytes("NO");

                col_details.seek(2);
                col_details.writeShort((int) (pageSizec - 42));
                usf.mkdir();
                res = true;

                col_details.seek(1);
                col_details.writeByte(8);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            if (res) {
                System.out.println("Success");
            }
        }
    }

    public static void parseCreateTable(String createTableString) {
        if (databaseName != null) {
            ArrayList<String> list_col_names = new ArrayList<>();
            System.out.println("STUB: Calling your method to create a table");
            System.out.println("Parsing the string:\"" + createTableString + "\"");
            ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));
            int occurence;
            occurence = Collections.frequency(createTableTokens, "primary");
            //System.out.println(occurence);
                /* Define table file name */

            String tableName = createTableTokens.get(2);
            String temp[] = tableName.replace("(", " ").split(" ");
            String tableFileName = temp[0] + ".tbl";

            if (createTableTokens.size() < 4) {
                System.out.println("creating database");
                try {
                    String databaseName1 = createTableTokens.get(2);
                    File file = new File("data\\user_data\\" + databaseName1);
                    boolean isDatabaseEsixts = file.exists();
                    if (!isDatabaseEsixts) {
                        file.mkdir();
                        System.out.println(databaseName1 + "is successfully created.");
                    } else {
                        System.out.println(databaseName1 + " is already exists!!");
                    }
                } catch (Exception e) {
                    System.out.println("error : " + e);
                }

            }

            /* YOUR CODE GOES HERE */
            /*  Code to create a .tbl file to contain table data */
            try {
                /*  Create RandomAccessFile tableFile in read-write mode.
                 *  Note that this doesn't create the table file in the correct directory structure
                 */
                File f = new File("data\\user_data\\" + databaseName + "\\" + temp[0]);
                boolean e = f.exists();
                //RandomAccessFile cf = new RandomAccessFile("data\\catalog\\davisbase_columns.tbl","rw");
                String dataTypeString;

                String colname;
                createTableString = createTableString.substring(createTableString.indexOf("(") + 1, createTableString.length() - 1);
                int recordSize = 0;
                int serialCode = 0;
                //System.out.println(pk_occurence);
                String datatype[] = createTableString.split(",");
                int ordinal_pos1 = 0;
                if (!e) {
                    if (occurence <= 1) {
                        Boolean datatypetrue = false;
                        for (int i = 0; i < datatype.length; i++) {
                            //System.out.println(datatype[i]);
                            //dataTypeString=datatype[i].substring(datatype[i].indexOf(" ")+1,datatype[i].length()).toString();
                            //dataTypeString=datatype[i].toLowerCase();
                            dataTypeString = datatype[i];
                            int cnt = 0;

                            String temp1[] = dataTypeString.split(" ");

                            //System.out.println(cnt);
                            //System.out.println(temp1[1]);
                            //System.out.println(temp1[2]);
                            colname = datatype[i].substring(0, datatype[i].indexOf(" "));

                            list_col_names.add(colname);

                            datatypetrue = checkDataType(temp1[1]);
                            if (datatypetrue == false) {
                                break;
                            }
                            //System.out.println(datatypetrue);
                            String pk = null;
                            String data_type = temp1[1];
                            if (data_type.equalsIgnoreCase("int")) {
                                recordSize = recordSize + 4;
                                serialCode = 0x06;
                            } else if (data_type.equalsIgnoreCase("tinyint")) {
                                recordSize = recordSize + 1;
                                serialCode = 0x04;
                            } else if (data_type.equalsIgnoreCase("smallint")) {
                                recordSize = recordSize + 2;
                                serialCode = 0x05;
                            } else if (data_type.equalsIgnoreCase("bigint")) {
                                recordSize = recordSize + 8;
                                serialCode = 0x07;
                            } else if (data_type.equalsIgnoreCase("real")) {
                                recordSize = recordSize + 4;
                                serialCode = 0x08;
                            } else if (data_type.equalsIgnoreCase("double")) {
                                recordSize = recordSize + 8;
                                serialCode = 0x09;
                            } else if (data_type.equalsIgnoreCase("datetime")) {
                                recordSize = recordSize + 8;
                                serialCode = 0x0A;
                            } else if (data_type.equalsIgnoreCase("date")) {
                                recordSize = recordSize + 8;
                                serialCode = 0x0B;
                            } else if (data_type.equalsIgnoreCase("text")) {
                                //recordSize=recordSize+8;
                                serialCode = 0x0C;
                            }
                            //System.out.println("Record size"+recordSize);
                            int ordinal_pos = 0;
                            int data_type_length = data_type.length();
                            int col_length = colname.length();
                            int tbl_length = temp[0].length();
//                        int tbl_length = bracke
                            int isnull_length = 0;
                            int primary_occurence = 0;
                            String primary = null;
                            if (dataTypeString.toLowerCase().contains("primary key") || dataTypeString.toLowerCase().contains("[not null]")) {
                                if (dataTypeString.toLowerCase().contains("primary key")) {
                                    isnull_length = 3;
                                    pk = "PRI";
                                } else {
                                    isnull_length = 2;
                                    pk = "NO";
                                }
                            } else {
                                isnull_length = 3;
                                pk = "YES";
                            }

                            int total_size = 1 + tbl_length + col_length + data_type_length + 1 + isnull_length + 6;

                            if (datatypetrue && occurence <= 1) {
                                f.mkdir();

                                RandomAccessFile colFile1 = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + temp[0] + "\\" + colname + ".ndx", "rw");
                                colFile1.setLength(pageSize);
                                colFile1.seek(0);
                                colFile1.writeByte(serialCode);
                                int null_val;
                                if (pk.equalsIgnoreCase("yes")) {
                                    null_val = 1;
                                } else {
                                    null_val = 0;
                                }
                                colFile1.writeByte(null_val);
                                //System.out.println(ordinal_pos1);

                                colFile1.write(ordinal_pos1);
                                ordinal_pos1++;
                                colFile1.writeByte(0x00);
                                colFile1.writeByte(0x10);
                                RandomAccessFile colFile = new RandomAccessFile("data\\catalog\\davisbase_columns.tbl", "rw");
                                colFile.setLength(2048);

                                colFile.seek(1);
                                int columnNumber = colFile.read();

                                colFile.seek(2);
                                int b1 = colFile.readShort();

                                colFile.seek(2);
                                colFile.writeShort((int) (b1 - total_size));

                                colFile.seek(b1 - total_size);
                                columnNumber++;
                                colFile.writeByte(1);
                                colFile.writeByte(columnNumber);

                                colFile.writeByte(tbl_length);
                                colFile.writeBytes(temp[0]);

                                colFile.writeByte(col_length);
                                colFile.writeBytes(colname);

                                colFile.writeByte(data_type_length);
                                colFile.writeBytes(data_type);

                                colFile.writeByte(1);
                                ordinal_pos++;
                                colFile.writeByte(ordinal_pos);

                                colFile.writeByte(isnull_length);
                                colFile.writeBytes(pk);

                                colFile.seek(1);
                                colFile.writeByte(columnNumber);

                                colFile.seek(2);
                                colFile.writeShort((int) (b1 - total_size));

                                colFile.seek(8 + columnNumber * 2 - 2);
                                colFile.writeShort(b1 - total_size);

                            }
                        }
                        if (datatypetrue && occurence <= 1) {
                            f.mkdir();
                            //RandomAccessFile tableFile = new RandomAccessFile("data\\user_data\\"+temp[0]+"\\"+temp[0], "rw");
                            //tableFile.setLength(pageSize);

                            RandomAccessFile tf = new RandomAccessFile("data\\catalog\\davisbase_tables.tbl", "rw");
                            tf.seek(1);
                            int rec_cnt = tf.read();
                            tf.seek(2);
                            ps = tf.readShort();
                            tf.setLength(pageSize);
                            ps = ps - temp[0].length() - 1;
                            tf.seek(ps);
                            rec_cnt = rec_cnt + 1;
                            tf.writeByte(rec_cnt);
                            //System.out.println(rec_cnt);
                            tf.writeBytes(temp[0]);
                            tf.seek(1);
                            tf.writeByte(rec_cnt);
                            tf.seek(2);
                            tf.writeShort((int) (ps));
                            tf.seek(8 + rec_cnt * 2 - 2);
                            tf.writeShort((int) ps);

                            RandomAccessFile tableFile = new RandomAccessFile("data\\user_data\\" + databaseName + "\\" + temp[0] + "\\" + temp[0] + ".tbl", "rw");
                            tableFile.setLength(pageSize);
                            tableFile.seek(0);
                            tableFile.write(0x0d);
                            tableFile.seek(1);
                            int col_rec_count = tableFile.readByte();
                            int size_col_header = 0;
                            if (col_rec_count == 0) {
                                ps_col = 512;

                                for (int cl = 0; cl < list_col_names.size(); cl++) {
                                    size_col_header += list_col_names.get(cl).length();
                                }
                                tableFile.seek(ps_col - size_col_header - list_col_names.size());

                                for (int wr = 0; wr < list_col_names.size(); wr++) {
                                    tableFile.writeByte(list_col_names.get(wr).length());
                                    tableFile.writeBytes(list_col_names.get(wr));
                                }
                                ps_col = ps_col - size_col_header - list_col_names.size();
                                tableFile.seek(2);
                                tableFile.writeShort((int) ps_col);
                            } else {

                            }

                        }
                        System.out.println("Table " + temp[0] + " is created.");
                    } else {
                        System.out.println("A table cannot have two primary keys.....");
                    }
                } else {
                    System.out.println("File " + temp[0] + " already exists");
                }
                //System.out.println(list_col_names);

//                 
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        } else {
            System.out.println("database is not selected");
        }
        /*  Code to insert rows in the davisbase_columns table  
         *  for each column in the new table 
         *  i.e. database catalog meta-data 
         */
    }

    private static void createDatabase(String userCommand) {
        ArrayList<String> tableTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
        if (tableTokens.size() < 4) {
            System.out.println("creating database");
            try {
                String databaseName1 = tableTokens.get(2);
                File file = new File("data\\user_data\\" + databaseName1);
                boolean isDatabaseEsixts = file.exists();
                if (!isDatabaseEsixts) {
                    file.mkdir();
                    System.out.println(databaseName1 + "is successfully created.");
                } else {
                    System.out.println(databaseName1 + " is already exists!!");
                }
            } catch (Exception e) {
                System.out.println("error : " + e);
            }

        }
    }
}
