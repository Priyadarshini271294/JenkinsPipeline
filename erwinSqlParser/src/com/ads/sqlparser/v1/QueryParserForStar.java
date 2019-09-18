/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ads.sqlparser.v1;

import demos.getstatement.getstatement;
import com.ads.sqlparser.v1.ErwinSQLParser;
import com.ads.sqlparser.v1.ErwinSqlParserExtererFinancev1;
import static com.ads.sqlparser.v1.ErwinSqlParserExtererFinancev1.dbVendor;
import static com.ads.sqlparser.v1.ErwinSqlParserExtererFinancev1.sqlparser;
import static com.ads.sqlparser.v1.QueryParserForStarV1.getTableNameFromAliasMap;
import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TGSqlParser;
import java.io.File;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

public class QueryParserForStar {

    public static String getChangedQuery(String query) {
        try {
            Map<String, List<String>> tablecolMap = ErwinSqlParserExtererFinancev1.getTableColumnsInfoFromxml(query, "mssql");
            Map<String, String> queriesMap = new LinkedHashMap<>();

            Set<String> querylist = ErwinSqlParserExtererFinancev1.getStarSelectQuery(query);
            if (!querylist.isEmpty()) {
                for (String selectquery : querylist) {
                    if (selectquery.startsWith("(") && selectquery.endsWith(")")) {
                        selectquery = selectquery.substring(1, selectquery.length() - 1);

                    }

                    String slctQuery = removecommentedline(selectquery);
                    if (slctQuery.contains("*")) {
                        List<Integer> starcount = getCount(slctQuery);
                        if (starcount.size() == 1) {
                            String tableName = getfromtableName(selectquery.split("\n"));
                            tableName = tableName.trim().split(" ")[0];
                            Set<String> columns = null;
                            if (tablecolMap.get(tableName.trim().toUpperCase()) != null) {
                                columns = new LinkedHashSet(tablecolMap.get(tableName.trim().toUpperCase()));
                            }

                            if (columns != null) {
                                String columnString = StringUtils.join(columns, ",\n");

                                String modifiedquery = selectquery.replaceFirst("\\*", columnString);
                                queriesMap.put(selectquery, modifiedquery);

                            } else {
                                // System.out.println("query----"+selectquery);
                            }

                        }

                    }

                }

                for (Map.Entry<String, String> entry : queriesMap.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    query = query.replace(key, value);
                }
                return query;
            } else {
                return query;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        //String query = FileUtils.readFileToString(new File("C:\\Users\\Uma\\Downloads\\bi_CertificateReportingAccountMonthEndLoad.txt0 (1).sql"));

        //  System.out.println("query--------" + query);
        return "";
    }

    public static String getChangedQuery(String query, Map<String, List<String>> tablecolMap) {
        try {

            Map<String, String> queriesMap = new LinkedHashMap<>();

            Set<String> querylist = ErwinSqlParserExtererFinancev1.getStarSelectQuery(query);
            if (!querylist.isEmpty()) {
                for (String selectquery : querylist) {
                    if (selectquery.startsWith("(") && selectquery.endsWith(")")) {
                        selectquery = selectquery.substring(1, selectquery.length() - 1);

                    }

                    String slctQuery = removecommentedline(selectquery);
                    if (slctQuery.contains("*")) {
                        List<Integer> starcount = getCount(slctQuery);
                        if (starcount.size() == 1) {
                            String tableName = getfromtableName(selectquery.split("\n"));
                            tableName = tableName.trim().split(" ")[0];
                            Set<String> columns = null;
                            if (tablecolMap.get(tableName.trim().toUpperCase()) != null) {
                                columns = new LinkedHashSet(tablecolMap.get(tableName.trim().toUpperCase()));
                            }

                            if (columns != null) {
                                String columnString = StringUtils.join(columns, ",\n");

                                String modifiedquery = selectquery.replaceFirst("\\*", columnString);
                                queriesMap.put(selectquery, modifiedquery);

                            } else {
                                // System.out.println("query----"+selectquery);
                            }

                        }

                    }

                }

                for (Map.Entry<String, String> entry : queriesMap.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    query = query.replace(key, value);
                }
                return query;
            } else {
                return query;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        //String query = FileUtils.readFileToString(new File("C:\\Users\\Uma\\Downloads\\bi_CertificateReportingAccountMonthEndLoad.txt0 (1).sql"));

        //  System.out.println("query--------" + query);
        return "";
    }

    public static String getChangedQuery(String query, Map<String, List<String>> tablecolMap, Map<String, List<String>> metadataMap, Map<String, String> tablealiasmap) {
        try {

            Map<String, String> queriesMap = new LinkedHashMap<>();
            Set<String> querylist = new LinkedHashSet<>(ErwinSqlParserExtererFinancev1.getStarSelectQuery(query));
            if (!querylist.isEmpty()) {
                for (String selectquery : querylist) {
                    if (selectquery.startsWith("(") && selectquery.endsWith(")")) {
                        selectquery = selectquery.substring(1, selectquery.length() - 1);

                    }

                    String slctQuery = removecommentedline(selectquery);
                    if (slctQuery.contains("*")) {
                        boolean staralias = false;
                        boolean aliasTabName = false;
                        List<Integer> starcount = getCount(slctQuery);
                        if (starcount.size() == 1) {
                            String tabName = "";
                            String aliasName = getAliasNameForstar(slctQuery);
                            String tableName = "";
                            if (selectquery.split("\n").length > 1) {
                                tableName = getfromtableName(selectquery.split("\n"));
                            } else {
                                tableName = selectquery.toLowerCase().split("from")[1].trim();

                            }
                            if (tableName.contains(aliasName)) {
                                aliasTabName = true;
                            }
                            tableName = tableName.trim().split(" ")[0];
                            if (tableName.contains("#") && tableName.contains(".")) {

                                tableName = tableName.substring(tableName.indexOf(".") + 1, tableName.length());
                            }
                            if (!"".equals(aliasName) && !aliasTabName) {
                                tabName = getTableNameFromAliasMap(tablealiasmap, aliasName);
                                staralias = true;
                            } else {

                            }
                            if (!"".equalsIgnoreCase(tabName)) {
                                tableName = tabName;
                            }

                            Set<String> columns = null;
                            if (metadataMap != null) {
                                if (metadataMap.get(tableName.trim().toUpperCase()) != null) {
                                    columns = new LinkedHashSet(metadataMap.get(tableName.trim().toUpperCase()));

                                }
                            }
                            if (columns == null || columns.isEmpty()) {
                                if (tablecolMap.get(tableName.trim().toUpperCase()) != null) {
                                    columns = new LinkedHashSet(tablecolMap.get(tableName.trim().toUpperCase()));
                                }
                            }
                            if (columns == null && !"".equals(aliasName)) {
                                tableName = "RESULT_OF_" + aliasName.toUpperCase();
                                if (tablecolMap.get(tableName.trim().toUpperCase()) != null) {
                                    columns = new LinkedHashSet(tablecolMap.get(tableName.trim().toUpperCase()));
                                }
                            }
                            if (columns != null) {
                                columns.remove("*");
                                String columnString = StringUtils.join(columns, ",\n");
                                if (!"".equals(aliasName)&&!"".equals(columnString)) {
                                    List<String> columnList = changeappendedStr(columnString, aliasName);
                                    columnString = StringUtils.join(columnList, "\n");
                                }
                                String modifiedquery = "";
                                if (!"".equals(aliasName) && !"".equals(columnString)) {
                                    modifiedquery = selectquery.replace(aliasName + ".*", columnString);
                                } else {
                                    if (!"".equals(columnString)) {
                                        modifiedquery = selectquery.replaceFirst("\\*", columnString);
                                    }

                                }

                                //modifiedquery = changeWithoutStar(modifiedquery);
                                if(!"".equals(modifiedquery)){
                                query = query.replace(selectquery, modifiedquery);
                                }
                                
                                Map<String, List<String>> tablecolupdatedMap = ErwinSqlParserExtererFinancev1.getTableColumnsInfoFromxml(query, "mssql");
                                tablecolMap.putAll(tablecolupdatedMap);
                            } else {
                                // System.out.println("query----"+selectquery);
                            }

                        }

                    }

                }

//                for (Map.Entry<String, String> entry : queriesMap.entrySet()) {
//                    String key = entry.getKey();
//                    String value = entry.getValue();
//                    query = query.replace(key, value);
//                }
                return query;
            } else {
                return query;
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("tableMap" + tablecolMap.toString());
        }
        //String query = FileUtils.readFileToString(new File("C:\\Users\\Uma\\Downloads\\bi_CertificateReportingAccountMonthEndLoad.txt0 (1).sql"));

        //  System.out.println("query--------" + query);
        return "";
    }

    public static String getChangedForAliasQuery(String query) {
        try {
            Map<String, List<String>> tablecolMap = ErwinSqlParserExtererFinancev1.getTableColumnsInfoFromxml(query, "mssql");
            Map<String, String> queriesMap = new LinkedHashMap<>();

            Set<String> querylist = ErwinSqlParserExtererFinancev1.getStarSelectQuery(query);
            if (!querylist.isEmpty()) {
                for (String selectquery : querylist) {
                    if (selectquery.startsWith("(") && selectquery.endsWith(")")) {
                        selectquery = selectquery.substring(1, selectquery.length() - 1);

                    }

                    String slctQuery = removecommentedline(selectquery);
                    if (slctQuery.contains("*")) {
                        List<Integer> starcount = getCount(slctQuery);
                        if (starcount.size() == 1) {
                            if (changeAliasStar(slctQuery)) {
                                String aliasName = getAliasNameForstar(slctQuery);
                                String tableName = getfromtableName(selectquery.split("\n"));
                                tableName = tableName.trim().split(" ")[0];
                                Set<String> columns = null;
                                if (tablecolMap.get(tableName.trim().toUpperCase()) != null) {
                                    columns = new LinkedHashSet(tablecolMap.get(tableName.trim().toUpperCase()));
                                }

                                if (columns != null) {
                                    String columnString = StringUtils.join(columns, ",\n");
//                                     List<String> columnList = changeappendedStr(columnString,aliasName);
//                                     columnString = StringUtils.join(columnList, ",\n");
                                    String modifiedquery = selectquery.replaceFirst("\\*", columnString);
                                    queriesMap.put(selectquery, modifiedquery);

                                } else {
                                    // System.out.println("query----"+selectquery);
                                }

                            }
                        }
                    }

                }

                for (Map.Entry<String, String> entry : queriesMap.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    query = query.replace(key, value);
                }
                return query;
            } else {
                return query;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        //String query = FileUtils.readFileToString(new File("C:\\Users\\Uma\\Downloads\\bi_CertificateReportingAccountMonthEndLoad.txt0 (1).sql"));

        //  System.out.println("query--------" + query);
        return "";
    }

    public static String getChangedForwithCteQuery(String Orginalquery, String withCtequery) {
        //String query = FileUtils.readFileToString(new File("C:\\Users\\Uma\\Downloads\\bi_CertificateReportingAccountMonthEndLoad.txt0 (1).sql"));
        try {
            Map<String, List<String>> tablecolMap = ErwinSqlParserExtererFinancev1.getTableColumnsInfoFromxml(withCtequery, "mssql");
            Map<String, String> queriesMap = new LinkedHashMap<>();
            Set<String> querylist = ErwinSqlParserExtererFinancev1.getStarSelectQuery(withCtequery);
            for (String selectquery : querylist) {
                if (selectquery.startsWith("(") && selectquery.endsWith(")")) {
                    selectquery = selectquery.substring(1, selectquery.length() - 1);

                }

                String slctQuery = removecommentedline(selectquery);
                if (slctQuery.contains("*")) {
                    List<Integer> starcount = getCount(slctQuery);
                    if (starcount.size() == 1) {
                        String tableName = getfromtableName(selectquery.split("\n"));
                        tableName = tableName.trim().split(" ")[0];
                        List<String> columns = tablecolMap.get(tableName.trim().toUpperCase());
                        String columnString = StringUtils.join(columns, ",\n");
                        String modifiedquery = selectquery.replace("*", columnString);
                        queriesMap.put(selectquery, modifiedquery);

                    }

                }

            }

            for (Map.Entry<String, String> entry : queriesMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                Orginalquery = Orginalquery.replace(key, value);
            }
            return Orginalquery;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return "";

    }

    public static String getfromtableName(String[] selectqueries) {

        for (String selectLine : selectqueries) {
            if (selectLine.toLowerCase().contains("from") && selectLine.contains("(")) {
                continue;
            }
            if (selectLine.trim().toLowerCase().contains("from") && selectLine.toLowerCase().trim().split(" ").length == 1) {
                continue;
            }
            if (selectLine.toLowerCase().contains("from")) {

                String table = selectLine.trim().toLowerCase().split("from")[1];
                return table;

            }
        }

        return null;
    }

    public static List<String> getStatements(String query) {
        List<String> querylists = new LinkedList();
        try {
            sqlparser = new TGSqlParser(EDbVendor.dbvmssql);
            sqlparser.sqltext = query;
            int ret = sqlparser.parse();
            if (ret == 0) {
                for (int i = 0; i < sqlparser.sqlstatements.size(); i++) {
                    TCustomSqlStatement stmnt = sqlparser.sqlstatements.get(i);
                    if (stmnt.toString().contains("*")) {
                        if (!query.equalsIgnoreCase(stmnt.toString())) {
                            querylists.add(stmnt.toString());
                        }

                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return querylists;
    }

    public static String removecommentedline(String selectQuery) {
        String[] linearr = selectQuery.split("\n");
        StringBuilder sb = new StringBuilder();
        for (String selectline : linearr) {

            if (selectline.trim().startsWith("--")) {
                continue;
            }
            if (selectline.trim().contains("--")) {
                selectline = selectline.substring(0, selectline.indexOf("--"));
            }
            sb.append(selectline).append("\n");
        }
        return sb.toString();
    }

    public static List<Integer> getCount(String query) {
        List<Integer> starcount = new LinkedList<>();
        try {
            String[] lines = query.split("\n");
            int i = 0;
            for (String line : lines) {

                if (line.contains("*") && !line.contains("case when") && !line.contains("(") && !line.contains("[0-9]") && !line.contains(")") && !line.contains("+") && !line.contains("-")&&!line.contains("then")) {
                    System.out.println("---" + line);
                    starcount.add(i);
                    break;
                }
                i++;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return starcount;
    }

    public static boolean changeAliasStar(String query) {
        String[] starline = query.split("\n");

        for (String line : starline) {

            if (line.trim().contains(".*")) {
                return true;
            }

        }

        return false;
    }

    public static String getAliasNameForstar(String query) {

        String aliasname = "";
        String[] starline = query.split("\n");

        for (String line : starline) {

            if (line.trim().contains(".*")) {

                if (line.trim().split(" ").length > 1) {

                    String[] linearr = line.trim().split(" ");
                    for (String lines : linearr) {
                        if (line.trim().contains(".*")) {
                            aliasname = lines.replace(".*", "");

                        }
                    }

                } else {

                    aliasname = line.replace(".*", "");
                }

            }

        }
        if (aliasname.contains(",")) {
            aliasname = aliasname.replace(",", "");
        }

        return aliasname;
    }

    public static List<String> changeappendedStr(String columns, String aliasname) {
        List<String> columnslist = new LinkedList<>();
        String[] strarr = columns.split("\n");

        for (String column : strarr) {
//            if(column.contains(aliasname)||column.contains("*")){
//            continue;
//            }
            if (!column.contains("*")) {
                if (column.contains(".")) {
                    //column = column.substring(column.indexOf("."))
                    columnslist.add(column);
                } else {
                    columnslist.add(aliasname + "." + column);
                }
            }

        }

        return columnslist;
    }

    public static String changeWithoutStar(String query) {
        StringBuilder sb = new StringBuilder();
        String[] queryarr = query.split("\n");

        for (String queryline : queryarr) {

            if (queryline.trim().startsWith("*,")) {

                continue;
            } else if (queryline.trim().contains("*") && queryline.trim().split(" ").length > 1 && !queryline.trim().contains("casewhen")) {
                String line = "";
                if (queryline.trim().split(" ").length > 1) {
                    String[] linearr = queryline.split(" ");

                    for (String star : linearr) {
                        if (line.contains("*") && !line.contains("case when") && !line.contains("(") && !line.contains("[0-9]") && !line.contains(")")) {
                            continue;
                        }

                        line = line + " " + star;
                        sb.append(line + "\n");
                    }
                }

            } else {
                sb.append(queryline + "\n");
            }

        }

        return sb.toString();

    }

}
