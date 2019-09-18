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

public class QueryParserForStarV1 {

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

    public static String getChangedForAliasQuery(String query, Map<String, List<String>> metadatamap) {
        try {
            Map<String, List<String>> tablecolMap = ErwinSqlParserExtererFinancev1.getTableColumnsInfoFromxml(query, "mssql");
            Map<String, String> tableAliasMap = ErwinSqlParserExtererFinancev1.getTablealiasMapfromquery(query, "mssql");
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

                                String tabName = getTableNameFromAliasMap(tableAliasMap, aliasName);
                                tableName = tableName.trim().split(" ")[0];
                                if (!"".equals(tabName)&&tabName!=null) {
                                    tableName = tabName;
                                }
                                Set<String> columns = new LinkedHashSet();
                                if(metadatamap!=null){
                                if (metadatamap.get(tableName.trim().toUpperCase()) != null) {

                                    columns = new LinkedHashSet<>(metadatamap.get(tableName.trim().toUpperCase()));
                                }
                                }
                                
                                if (tablecolMap.get(tableName.trim().toUpperCase()) != null) {
                                    if (columns.isEmpty()) {
                                        columns = new LinkedHashSet(tablecolMap.get(tableName.trim().toUpperCase()));
                                    }

                                }

                                if (columns != null) {
                                    String columnString = StringUtils.join(columns, ",\n");
                                    List<String> columnList = changeappendedStr(columnString, aliasName);
                                    columnString = StringUtils.join(columnList, ",\n");
                                    String modifiedquery = selectquery.replaceFirst("\\*", columnString);
                                    selectquery = selectquery.replace(aliasName + ".*", "");
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

            if (selectline.trim().startsWith("--") || selectline.trim().contains("--")) {
                continue;
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
                if (line.contains("*") && !line.contains("case when") && !line.contains("(") && !line.contains("[0-9]") && !line.contains(")")) {
                    starcount.add(i);
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

        return aliasname;
    }

    public static List<String> changeappendedStr(String columns, String aliasname) {
        List<String> columnslist = new LinkedList<>();
        String[] strarr = columns.split("\n");

        for (String column : strarr) {
            columnslist.add(aliasname + "." + column);

        }

        return columnslist;
    }

    public static String getTableNameFromAliasMap(Map<String, String> aliasMap, String aliasName) {
       try{
       for (Map.Entry<String, String> entry : aliasMap.entrySet()) {
              String key = entry.getKey();
           if(key!=null){
            String value = entry.getValue();
            if (key.equalsIgnoreCase(aliasName)) {
                return value;
            }
           
           }
         
           
        }
       }catch(Exception e){
           System.out.println("...."+aliasMap);
           System.out.println("aliasname"+aliasName);
       }
        
        return null;
    }
}
