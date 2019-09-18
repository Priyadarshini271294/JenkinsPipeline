/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goldparser;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author sashikant D
 */
public class TableBrTree {

    public static List<Map<String, List<String>>> sourceTableMap(String BusinessRule, String columnName, String egtfilepath) {
        List<Map<String, List<String>>> maplist = null;
        try {

            Goldparser parser = new Goldparser();
            boolean wantTree = true;
            String tree = parser.executeProgram(BusinessRule, wantTree, egtfilepath);
            //  System.out.println("======" + tree);
            // getting tree
            String valuestring = getXmlTree(tree, columnName);
            //get Map from tree
            maplist = tablecolumnMap(valuestring.toUpperCase());

        } catch (Exception e) {
        }

        return maplist;

    }

    public static String getXmlTree(String tree, String columnName) {
        String values = "";
        try {
            int i = 0;
            StringBuilder sb = new StringBuilder();
            String[] data = tree.split("\n");
            for (String valuedata : data) {

                if (valuedata.contains("<Value> ::= Id")) {
                    String vdata = data[i + 1];
                    //for null values
//                    if (vdata.contains(",")) {
//                        vdata = "";
//                    }
                    sb.append(vdata + "\n");

                }
                i++;
            }
            values = sb.toString().replaceAll("(?m)^[ \t]*\r?\n", "").replaceAll("\\|", "").replace("+", "").replace("-", "");
            // System.out.println(values);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return values;
    }

    public static List<Map<String, List<String>>> tablecolumnMap(String tree) {
        List<Map<String, List<String>>> tableColumnmapList = null;
        try {
            Map<String, String> tablecolumnMap = new LinkedHashMap<>();
            Map<String, List<String>> tabcollist = new LinkedHashMap<>();
            tableColumnmapList = new LinkedList<>();
            String[] tablecolumndata = tree.split("\n");
            for (String tabcol : tablecolumndata) {
                if (!tabcol.contains(".")) {
                    String orphantableName = "Missing TableName";
                    if (tabcol.contains(",")) {
                        tabcol = tabcol.split(",")[0].trim();
                    }
                    tabcol = orphantableName + "." + tabcol;

                }
                if (tabcol.contains(",") && tabcol.split(",").length > 1) {
                    String[] tabcolarr = tabcol.split(",");
                    for (String mtcl : tabcolarr) {
                         String columnName ="";
                        String tableName = mtcl.split("\\.")[0].trim();
                        if(mtcl.split("\\.").length>1){
                          columnName = mtcl.split("\\.")[1].trim();
                        }
                       

                        if (tablecolumnMap.get(tableName) == null) {
                            tablecolumnMap.put(tableName, columnName);
                        } else {
                            String value = tablecolumnMap.get(tableName);
                            tablecolumnMap.put(tableName, value + "~" + columnName);
                        }

                    }

                } else {
                     String columnName ="";
                    String tableName = tabcol.split("\\.")[0].trim();
                    if(tabcol.split("\\.").length>1){
                     columnName = tabcol.split("\\.")[1].trim();
                    }
                    

                    if (tablecolumnMap.get(tableName) == null) {
                        tablecolumnMap.put(tableName, columnName);
                    } else {
                        String value = tablecolumnMap.get(tableName);
                        tablecolumnMap.put(tableName, value + "~" + columnName);
                    }

                }

            }
            for (Map.Entry<String, String> entrySet : tablecolumnMap.entrySet()) {
                String key = entrySet.getKey();
                String value = entrySet.getValue();
                List<String> valueslist = null;
                if (value.contains("~")) {
                    String[] values = value.split("~");
                    valueslist = Arrays.asList(values);
                    tabcollist.put(key, valueslist);
                } else {
                    valueslist = new LinkedList<>();
                    valueslist.add(value);
                    tabcollist.put(key, valueslist);
                }
            }
            tableColumnmapList.add(tabcollist);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return tableColumnmapList;
    }

}
