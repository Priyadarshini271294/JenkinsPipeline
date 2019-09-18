package demos.columnAnalyze;

import demos.columnImpact.ColumnImpact;
import demos.columnImpact.ColumnImpact.TColumn;
import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.ESqlClause;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.TStatementList;
import gudusoft.gsqlparser.nodes.TObjectName;
import gudusoft.gsqlparser.nodes.TObjectNameList;
import gudusoft.gsqlparser.nodes.TTable;
import gudusoft.gsqlparser.nodes.TTableList;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ColumnAnalyze {

    private StringBuffer buffer = new StringBuffer();
    private String errorMessage;
    public static Map<String, List<String>> tablecolalias = new LinkedHashMap<>();
    public static Map<String, String> tablecolaliasMap = new LinkedHashMap<>();
    public static Map<String, String> tablecolumnMap = new LinkedHashMap<>();
    public static HashSet<String> columns = new HashSet();
    public static HashSet<String> columnsalias = new HashSet();
    public static HashSet<String> tableColumnFromSqlImpact = new HashSet<>();
    public static Map<String, String> tablecolumnwithoutwhere = new LinkedHashMap<>();
    public static Map<String, Map<String, List<ESqlClause>>> tableColumns = new LinkedHashMap<String, Map<String, List<ESqlClause>>>();

    public ColumnAnalyze(File file) {
        TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvnetezza);
        sqlparser.sqlfilename = file.getAbsolutePath();
        impactSQL(sqlparser);
    }

    public ColumnAnalyze(File file, EDbVendor dbvendor) {
        TGSqlParser sqlparser = new TGSqlParser(dbvendor);
        sqlparser.sqlfilename = file.getAbsolutePath();
        impactSQL(sqlparser);
    }

    public ColumnAnalyze(String file, EDbVendor dbvendor) {
        TGSqlParser sqlparser = new TGSqlParser(dbvendor);
        sqlparser.sqlfilename = file;
        impactSQL(sqlparser);
    }

    public ColumnAnalyze(String sql) {
        TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvmssql);
        sqlparser.sqltext = sql;
        impactSQL(sqlparser);
    }

    private void impactSQL(TGSqlParser sqlparser) {

        buffer = new StringBuffer();
        tableColumns.clear();
        columns.clear();
        tablecolalias.clear();
        tablecolaliasMap.clear();
        tablecolumnMap.clear();
        columnsalias.clear();
        tableColumnFromSqlImpact.clear();
        tablecolumnwithoutwhere.clear();
        errorMessage = null;
        int ret = sqlparser.parse();

        if (ret != 0) {
            errorMessage = sqlparser.getErrormessage();
        } else {
            TStatementList stmts = sqlparser.sqlstatements;
            for (int i = 0; i < stmts.size(); i++) {
                TCustomSqlStatement stmt = stmts.get(i);
                impactStatement(stmt);
                //System.out.println("tablecolumnfrom----" + tableColumns);
            }

            ColumnImpact impact = null;

            if (sqlparser.sqlfilename != null && !"".equals(sqlparser.sqlfilename)) {
                impact = new ColumnImpact(new File(sqlparser.sqlfilename),
                        sqlparser.getDbVendor(),
                        true,
                        false);
            } else {
                impact = new ColumnImpact(sqlparser.sqltext,
                        sqlparser.getDbVendor(),
                        true,
                        false);
            }
            impact.impactSQL();
            TColumn[] columnInfos = impact.getColumnInfos();
            for (int i = 0; i < columnInfos.length; i++) {

                TColumn column = columnInfos[i];
                for (int j = 0; j < column.tableNames.size(); j++) {
                    List<String> columnnamewithaliasName = new LinkedList<>();
                    if (column.tableNames.get(j) == null) {
                        continue;
                    }
                    String tableName = "";
                    String tablefullName = "";
                    String tablefullName2 = "";
                    String columnAliasName = "";
                    String columnName = "";
//                    column.getOrigName();
                    try {
                        tableName = column.tableNames.get(j).toUpperCase();
                        tablefullName = column.tableFullNames.get(j).toUpperCase();
                        tablefullName2 = column.tableFullNames.get(j).toUpperCase();
                        columnAliasName = column.alias;
                        columnName = column.columnName;
                    } catch (Exception e) {

                    }
                    //  System.out.println("column:- " + column.columnName + "columnalias:- " + columnAliasName + "    expr:- " + column.expression + "tableName:-- " + tablefullName);
                    // alias name
//                    if (!tablecolumnMap.containsKey(tablefullName)) {
//                        tablecolumnMap.put(tablefullName, columnAliasName);
//                    } else {
//                        String col = tablecolumnMap.get(tablefullName) + "#" + columnAliasName;
//                        tablecolumnMap.put(tablefullName, col);
//
//                    }

                    // forcolumn
                    column.columnName = column.columnName.replace("[", "").replace("]", "");
                    if ("".equalsIgnoreCase(columnName)) {
                        columnName = columnAliasName;
                    }
                    if(!"".equalsIgnoreCase(columnAliasName)){
                    columns.add(tablefullName + "#" + column.columnName);
                    columnsalias.add(tablefullName + "#" + column.columnName+"$$"+columnAliasName);
                    }
                    
                    if (!tablecolumnMap.containsKey(tablefullName)) {
                        tablecolumnMap.put(tablefullName, column.columnName);
                    } else {
                        String col = tablecolumnMap.get(tablefullName) + "#" + column.columnName;
                        tablecolumnMap.put(tablefullName, col);

                    }

                    if (tableColumns.containsKey(tableName)) {
                        updateClauseType(column, tableName);
                    } else {
                        Iterator<String> iter = tableColumns.keySet()
                                .iterator();
                        while (iter.hasNext()) {
                            updateClauseType(column, iter.next());
                        }
                    }
                    if (columnAliasName == null) {
                        columnAliasName = columnName;
                    }

                    if (tablecolaliasMap.get(tablefullName + "#" + tableName) == null) {
                        tablecolaliasMap.put(tablefullName + "#" + tableName, columnName + "#" + columnAliasName);
                    } else {
                        String value = tablecolaliasMap.get(tablefullName + "#" + tableName);
                        tablecolaliasMap.put(tablefullName + "#" + tableName, value + "," + columnName + "#" + columnAliasName);
                    }

                }
            }

            if (!tableColumns.isEmpty()) {
                buffer.append("TABLE_NAME,COLUMN_NAME,PROJECTION_FLAG,RESTRICTION_FLAG,JOIN_FLAG,GROUP_BY_FLAG,ORDER_BY_FLAG\n");
                Iterator<String> tableIter = tableColumns.keySet().iterator();
                while (tableIter.hasNext()) {

                    String tableName = tableIter.next();
                    Map<String, List<ESqlClause>> columns = tableColumns.get(tableName);
                    Iterator<String> columnIter = columns.keySet().iterator();
                    while (columnIter.hasNext()) {
                        String columnName = columnIter.next();
                        buffer.append(tableName)
                                .append(",")
                                .append(columnName)
                                .append(",");
                        List<ESqlClause> locations = columns.get(columnName);

                        if (locations.contains(ESqlClause.resultColumn)) {
                            buffer.append(1).append(",");
                        } else {
                            buffer.append(0).append(",");
                        }

                        if (locations.contains(ESqlClause.where)) {
                            buffer.append(1).append(",");
                        } else {
                            buffer.append(0).append(",");
                        }

                        if (locations.contains(ESqlClause.joinCondition)
                                || locations.contains(ESqlClause.join)) {
                            buffer.append(1).append(",");
                        } else {
                            buffer.append(0).append(",");
                        }

                        if (locations.contains(ESqlClause.groupby)) {
                            buffer.append(1).append(",");
                        } else {
                            buffer.append(0).append(",");
                        }

                        if (locations.contains(ESqlClause.orderby)) {
                            buffer.append(1);
                        } else {
                            buffer.append(0);
                        }
                        if (locations.contains(ESqlClause.having)) {
                            buffer.append(1);
                        } else {
                            buffer.append(0);
                        }

                        buffer.append("\n");
                    }
                }
            }
        }

        // System.out.println("----column----" + columns);
    }

    private void updateClauseType(TColumn column, String tableName) {
        Map<String, List<ESqlClause>> columns = tableColumns.get(tableName);
        List<ESqlClause> clauses = columns.get(column.columnName.toUpperCase());
        if (clauses != null) {
            switch (column.clauseType) {
                case select:
                    if (!clauses.contains(ESqlClause.resultColumn)) {
                        clauses.add(ESqlClause.resultColumn);
                    }
                    break;
                case join:
                    if (!clauses.contains(ESqlClause.joinCondition)) {
                        clauses.add(ESqlClause.joinCondition);
                    }
                    break;
                case orderby:
                    if (!clauses.contains(ESqlClause.orderby)) {
                        clauses.add(ESqlClause.orderby);
                    }
                    break;
                case groupby:
                    if (!clauses.contains(ESqlClause.groupby)) {
                        clauses.add(ESqlClause.groupby);
                    }
                    break;
                case where:
                    if (!clauses.contains(ESqlClause.where)) {
                        clauses.add(ESqlClause.where);
                    }
                    break;
            }
        }
    }

    public String getResult() {
        return buffer.toString();
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    private void impactStatement(TCustomSqlStatement stmt) {
        TTableList tables = stmt.tables;
        for (int i = 0; i < tables.size(); i++) {
            TTable table = tables.getTable(i);
            if (table.isBaseTable()) {
                String tableName = table.getTableName()
                        .toString()
                        .toUpperCase();

                Map<String, List<ESqlClause>> columnMaps = null;
                if (!tableColumns.containsKey(tableName)) {
                    tableColumns.put(tableName,
                            new LinkedHashMap<String, List<ESqlClause>>());
                    columnMaps = tableColumns.get(tableName);
                } else {
                    columnMaps = tableColumns.get(tableName);
                }

                TObjectNameList columnNames = table.getLinkedColumns();
                for (int j = 0; j < columnNames.size(); j++) {
                    TObjectName columnName = columnNames.getObjectName(j);
                    String column = columnName.getColumnNameOnly()
                            .toUpperCase();

                    if (column.equals("*")) {
                        continue;
                    }

                    List<ESqlClause> columnLocations = null;
                    if (!columnMaps.containsKey(column)) {
                        columnMaps.put(column, new ArrayList<ESqlClause>());
                        columnLocations = columnMaps.get(column);
                    } else {
                        columnLocations = columnMaps.get(column);
                    }
                    if (!columnLocations.contains(columnName.getLocation())) {
                        if (columnName.getLocation().toString().contains("select")) {
                            columnLocations.add(columnName.getLocation());
                            if (tablecolumnwithoutwhere.containsKey(tableName)) {
                                String col = tablecolumnwithoutwhere.get(tableName);
                                tablecolumnwithoutwhere.put(tableName, col + "#" + column);
                            }
                            tablecolumnwithoutwhere.put(tableName, column);
                        }
                     //   System.out.println("whereeee" + columnName.getLocation().toString());

                    }
                }
            }
        }

        TStatementList stmts = stmt.getStatements();
        for (int i = 0; i < stmts.size(); i++) {
            impactStatement(stmts.get(i));
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
         //   System.out.println("Usage: java ColumnAnalyze <input files directory> <output files directory>");
            return;
        }

        File inputDir = new File(args[0]);
        File outputDir = new File(args[1]);

        if (!inputDir.isDirectory()) {
            System.out.println(inputDir + " is not a valid directory.");
            return;
        }

        if (outputDir.isFile()) {
            System.out.println(outputDir + " is not a valid directory.");
            return;
        } else if (!outputDir.exists() && !outputDir.mkdirs()) {
            System.out.println(outputDir + " is not a valid directory.");
            return;
        }

        File[] files = inputDir.listFiles();
        for (int i = 0; i < files.length; i++) {
            if (files[i].isFile()) {
                ColumnAnalyze analyze = new ColumnAnalyze(files[i]);
                if (analyze.getErrorMessage() != null) {
               //     System.out.println(analyze.getErrorMessage());
                } else {
                    int index = files[i].getName().lastIndexOf('.');
                    String outputFileName;
                    if (index == -1) {
                        outputFileName = files[i].getName() + ".txt";
                    } else {
                        outputFileName = files[i].getName().substring(0,
                                index)
                                + ".txt";
                    }
                    try {
                        FileOutputStream fos = new FileOutputStream(new File(outputDir,
                                outputFileName));
                        fos.write(analyze.getResult().getBytes());
                        fos.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public static void getcolumnanalyze(String sqltext) {
        // EDbVendor dbvendor = getEDbVendorType(dbvendors);
        ColumnAnalyze analyze = new ColumnAnalyze(sqltext);
     //   System.out.println("columnanalyze" + analyze.getResult());
    }

    public static void getcolumnanalyze(String sqltext, String dbvender) {
        EDbVendor dbvendor = getEDbVendorType(dbvender);
        ColumnAnalyze analyze = new ColumnAnalyze(sqltext);
      //  System.out.println("columnanalyze" + analyze.getResult());
    }

    public static EDbVendor getEDbVendorType(String dbVender) {
        EDbVendor dbVendor = EDbVendor.dbvpostgresql;

        if (dbVender != null) {
            if (dbVender.equalsIgnoreCase("mssql")) {
                dbVendor = EDbVendor.dbvmssql;
            } else if (dbVender.equalsIgnoreCase("db2")) {
                dbVendor = EDbVendor.dbvdb2;
            } else if (dbVender.equalsIgnoreCase("mysql")) {
                dbVendor = EDbVendor.dbvmysql;
            } else if (dbVender.equalsIgnoreCase("postgresql")) {
                dbVendor = EDbVendor.dbvpostgresql;
            } else if (dbVender.equalsIgnoreCase("mssql")) {
                dbVendor = EDbVendor.dbvmssql;
            } else if (dbVender.equalsIgnoreCase("oracle")) {
                dbVendor = EDbVendor.dbvoracle;
            } else if (dbVender.equalsIgnoreCase("netezza")) {
                dbVendor = EDbVendor.dbvnetezza;
            } else if (dbVender.equalsIgnoreCase("teradata")) {
                dbVendor = EDbVendor.dbvteradata;
            }
        }
        return dbVendor;
    }
}
