package com.ads.sqlparser.v1;

import static com.ads.sqlparser.v1.ErwinSQLParser.dbVendor;
import static com.ads.sqlparser.v1.ErwinSQLParser.getEDbVendorType;
import static com.ads.sqlparser.v1.ErwinSQLParser.sqlparser;

import demos.getstatement.getstatement;
import gudusoft.gsqlparser.TGSqlParser;

import java.io.*;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

/**
 * @author sashikant D
 */
public class StroreProcedureParsermultipleStoreproc {

    public static void main(String[] args) throws Exception {
        // List<String> storeproclist=   getstatement.getallstatement("C:\\Users\\sashikant D\\Desktop\\teststoreprocedure.sql","mssql");
        parseStoreprocintomultiplefile("E:\\sqlfiles\\test\\storeproctest", "mssql", null);
/*
        String sql = "";
        sql = extractDynamicSQLs(sql);
        sql = changeWithClause(sql);
        sql = extractUtilProcSQLs(sql);
        sql = validateSQLForCTE(sql);
        System.out.println(sql);
*/
    }

    public static String parseStoreprocintomultiplefile(String storeProcFilePath, String dbVender, Map<String, List<String>> metadataMap) {
        Set<String> storeproclist = null;
        File storeProcFile = null;
        String inputFileName = "";
        StringBuilder sb = new StringBuilder();
        try {
            StringBuilder sb1 = new StringBuilder();
            storeProcFile = new File(storeProcFilePath);
            if (storeProcFile.isDirectory()) {
                File[] listofstoreprocFiles = storeProcFile.listFiles();
                File outputDirectory = new File(storeProcFile.getAbsolutePath() + File.separator + "output");
                if (outputDirectory.exists()) {
                    FileUtils.deleteDirectory(outputDirectory);
                }
                for (File listofstoreprocFile : listofstoreprocFiles) {
                    String fileextension = FilenameUtils.getExtension(listofstoreprocFile.getName());

                    inputFileName = listofstoreprocFile.getName();

                    storeproclist = getstatement.getallstatement(listofstoreprocFile.getAbsolutePath(), dbVender);

                    int i = 1;
                    for (String storeproc : storeproclist) {
                        File sqlfilepath = new File(listofstoreprocFile.getAbsolutePath());
                        String parentpath = sqlfilepath.getAbsoluteFile().getParent();
                        File sqlfile = new File(parentpath);

                        File directory = new File(parentpath + "/output/");
                        if (!directory.exists()) {
                            directory.mkdir();
                        }
                        if (storeproclist.size() == 1) {
                            sqlfile = new File(parentpath + "/output/" + inputFileName.replace(".sql", "") + "" + ".sql");
                        } else {
                            sqlfile = new File(parentpath + "/output/" + inputFileName.replace(".sql", "") + i + ".sql");
                            i++;
                        }
                        String updatedSQL = "";
                        String dynamicquery = extractDynamicSQLs(storeproc);
                        updatedSQL = changeWithClause(dynamicquery);
                        String utilprocs = extractUtilProcSQLs(updatedSQL);
                        updatedSQL = validateSQLForCTE(utilprocs);

                        if (!checkfileparsing(updatedSQL, "mssql")) {
                            updatedSQL = utilprocs;
                        }
                        Map<String, List<String>> tablecolMap = ErwinSqlParserExtererFinancev1.getTableColumnsInfoFromxml(updatedSQL, "mssql");
                        Map<String, String> tableAliasMap = ErwinSqlParserExtererFinancev1.getTablealiasMapfromquery(updatedSQL, "mssql");
                        Set<String> craetequery = ErwinSqlParserExtererFinancev1.getcreateTable(updatedSQL);
                        int j = 0;
                        for (String createtableQueries : craetequery) {
                            File sqlfile2 = new File(sqlfile.getParent() + "/" + sqlfile.getName().replace(".sql", "") + "_output/" + sqlfile.getName().replace(".sql", "") + j + ".sql");
                            String createqueries = QueryParserForStar.getChangedQuery(createtableQueries, tablecolMap, metadataMap, tableAliasMap);
                            if ("".equals(createqueries)) {
                                createqueries = createtableQueries;
                            }
                            FileUtils.writeStringToFile(sqlfile2, createqueries);
                            sb1.append(createqueries + "\n");
                            j++;
                        }

                    }

                }
            }
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            sb.append(exceptionAsString);
        } finally {
            System.gc();
        }
        return sb.toString();
    }

    public static String changeWithClause(String filecontent) {

        String renameFromTbl = "";
        String renameToTbl = "";
        StringBuffer strbf = new StringBuffer();
        try {
            String[] filearr = filecontent.split("\n");
            for (String withfile : filearr) {
                int leftSpaceCount = 0;
                for (int i = 0; i < withfile.length(); i++) {
                    if (withfile.charAt(i) == ' ') {
                        leftSpaceCount++;
                    } else {
                        break;
                    }
                    ;
                }
                withfile = withfile.trim().replaceAll("( )+", " ");//replace multiple spaces with single space between words
                String leftSpace = "";
                for (int i = 0; i < leftSpaceCount; i++) {
                    leftSpace += " ";
                }
                withfile = leftSpace + withfile; //append leftSpace
                withfile = withfile.replace("with (", "WITH(");
                withfile = withfile.replace("WITH (", "WITH(");
                withfile = withfile.replace("With (", "WITH(");
                withfile = withfile.replace("over (", "OVER(");
                withfile = withfile.replace("OVER (", "OVER(");
                withfile = withfile.replace("Over (", "OVER(");
                int firstLeftBrace = 0;
                int leftBraceCount = 1;
                int rightBraceCount = 0;
                String part1 = "";
                String part2 = "";

                if (withfile.toLowerCase().contains("with(") && withfile.toLowerCase().contains("distribution")) {
                    String tmpLine1 = withfile.substring(0, withfile.toLowerCase().indexOf("with("));
                    String tmpLine2 = withfile.substring(withfile.toLowerCase().indexOf("with("));
                    firstLeftBrace = tmpLine1.length() + tmpLine2.indexOf("(");
                    for (int i = firstLeftBrace + 1; i < withfile.length(); i++) {
                        if (withfile.charAt(i) == '(') {
                            leftBraceCount++;
                        }
                        if (withfile.charAt(i) == ')') {
                            rightBraceCount++;
                        }
                        if (leftBraceCount - rightBraceCount == 0) {
                            if (firstLeftBrace > 4) {
                                part1 = withfile.substring(0, firstLeftBrace - 4);
                            }
                            if (i + 1 < withfile.length()) {
                                part2 = withfile.substring(i + 1);
                            }
                            withfile = part1 + part2;
                            break;
                        }
                    }
                } else if (withfile.toLowerCase().contains("over(") && (withfile.toLowerCase().contains("rows ") || withfile.toLowerCase().contains("range "))) {
                    firstLeftBrace = withfile.toLowerCase().indexOf("over(") + 4;

                    for (int i = firstLeftBrace + 1; i < withfile.length(); i++) {
                        if (withfile.charAt(i) == '(') {
                            leftBraceCount++;
                        }
                        if (withfile.charAt(i) == ')') {
                            rightBraceCount++;
                        }
                        if (leftBraceCount - rightBraceCount == 0) {
                            int rowsRangePos = 0;
                            if (withfile.toLowerCase().contains("rows ")) {
                                rowsRangePos = withfile.toLowerCase().indexOf("rows ");
                            } else if (withfile.toLowerCase().contains("range ")) {
                                rowsRangePos = withfile.toLowerCase().indexOf("range ");
                            }
                            part1 = withfile.substring(0, rowsRangePos - 1);
                            part2 = withfile.substring(i);
                            withfile = part1 + " " + part2;
                            break;
                        }
                    }
                } else if (withfile.trim().toLowerCase().contains("rename object") && !withfile.trim().toLowerCase().contains("'rename object")) {
                    withfile = withfile.trim();
                    if (withfile.trim().startsWith("--")) {
                        withfile = "";
                    } else {
                        withfile = withfile.trim().substring(13);
                        withfile = withfile.replace(" TO ", " to ");
                        withfile = withfile.replace(" To ", " to ");
                        renameFromTbl = withfile.substring(0, withfile.toLowerCase().indexOf(" to ")).trim().replaceAll(":", "");
                        renameToTbl = withfile.substring(withfile.toLowerCase().indexOf(" to ") + 4).trim().replace(";", "");
                        if (renameFromTbl.length() > 0 && renameToTbl.length() > 0) {
                            String schmaNm = "";
                            if (renameFromTbl.contains(".")) {
                                schmaNm = renameFromTbl.substring(0, renameFromTbl.indexOf("."));
                            }
                            if (!renameToTbl.contains(".") && schmaNm.length() > 0) {
                                renameToTbl = schmaNm + "." + renameToTbl;
                            }
                            withfile = "INSERT INTO " + renameToTbl + " SELECT * FROM " + renameFromTbl + ";";
                        } else {
                            withfile = "";
                        }
                    }
                }
                strbf.append(withfile + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return strbf.toString();
    }

    public static String extractUtilProcSQLs(String sql) throws Exception {

        String fileContent = getFileContent(sql);
        String appendSQL = "";
        appendSQL = getSQLfromUtilProc(fileContent, "util_AppendToTable", appendSQL);
        appendSQL = getSQLfromUtilProc(fileContent, "util_CreateTableAs", appendSQL);
        //appendSQL = getSQLfromUtilProc(fileContent, "rename object", appendSQL);
        sql += appendSQL;
        return sql;
    }

    public static String getFileContent(String sql) throws Exception {

        sql = sql.replaceAll("'--'", "'-'").replaceAll("'---'", "'-'").replaceAll("'----'", "'-'");
        sql = sql.replaceAll("\r\n", "\n");
        String[] lineArr = sql.split("\n");
        String uncommentedLine = "";
        boolean isCommentedLine = false;
        String fileContent = "";
        try {
            for (String line : lineArr) {
                if (isCommentedLine) {
                    if (line.contains("*/")) {
                        //if "*/" exists in the middle of the line, then extract right part excluding commented part
                        if (uncommentedLine.length() > 0) {
                            line = uncommentedLine + " " + line.substring(line.indexOf("*/") + 2);
                        } else {
                            line = line.substring(line.indexOf("*/") + 2);
                        }
                        if (line.contains("--")) {
                            line = line.substring(0, line.indexOf("--"));
                        }
                        isCommentedLine = false;
                        uncommentedLine = "";
                    } else {
                        continue;
                    }
                }
                if (line.contains("--")) {
                    if (line.trim().startsWith("--")) {
                        line = "--";
                    }
                    //if "--" exists in the middle of the line, then extract left part excluding commented part
                    if ((line.contains("/*") && line.indexOf("--") < line.indexOf("/*"))
                            || (!line.contains("/*") && !line.contains("*/"))
                            || (line.contains("/*") && line.contains("*/") && line.indexOf("--") > line.indexOf("*/"))) {
                        line = line.substring(0, line.indexOf("--"));
                    }
                }
                if (line.contains("/*") && line.contains("*/")) {
                    line = line.substring(0, line.indexOf("/*")) + line.substring(line.indexOf("*/") + 2);
                }
                if (line.contains("/*") && !line.contains("*/")) {
                    //if "/*" starts in the middle of the line, then extract left part excluding commented part
                    uncommentedLine = line.substring(0, line.indexOf("/*"));
                    isCommentedLine = true;
                    continue;
                }
                if (line.trim().startsWith("--")) {
                    line = "";
                }
                line = line.replaceAll("\t", " ");
                line = line.trim().replaceAll("( )+", " ");//replace multiple spaces with single space between words
                if (line.length() == 0) {
                    continue;
                } else {
                    fileContent += " " + line;
                }
            }
            fileContent = fileContent.replaceAll("' \\(", "'\\(");
            fileContent = fileContent.replaceAll(" \\+", "+").replaceAll("\\+ ", "+");
            fileContent = fileContent.replaceAll(" \\(", "(");
            fileContent = fileContent.replaceAll("\t", " ");
            fileContent = fileContent.replaceAll("=", " = ");
            fileContent = fileContent.replaceAll(" REMOTE ", " ").replaceAll(" Remote ", " ").replaceAll(" remote ", " ");
            fileContent = fileContent.replaceAll("' CREATE ", "'create ").replaceAll("' Create ", "'create ").replaceAll("' create ", "'create ");
            fileContent = fileContent.replaceAll("' DELETE ", "'delete ").replaceAll("' Delete ", "'delete ").replaceAll("' delete ", "'delete ");
            fileContent = fileContent.replaceAll("' INSERT ", "'insert ").replaceAll("' Insert ", "'insert ").replaceAll("' insert ", "'insert ");
            fileContent = fileContent.replaceAll(" EXECUTE", " exec").replaceAll(" Execute", " exec").replaceAll(" execute", " exec");
            fileContent = fileContent.replaceAll(" EXEC", " exec").replaceAll(" Exec", " exec");
            fileContent = fileContent.replaceAll(" exec \\(", " exec\\(");
            fileContent = fileContent.replaceAll(" SP_EXECUTESQL", " ").replaceAll(" SP_Executesql", " ").replaceAll(" SP_ExecuteSQL", " ").replaceAll(" SP_executesql", " ").replaceAll(" Sp_executesql", " ").replaceAll(" sp_executesql", " ");
            fileContent = fileContent.replaceAll(" WHERECAST", " where cast").replaceAll(" WhereCast", " where cast").replaceAll(" wherecast", " where cast");
            fileContent = fileContent.replaceAll("'\\( select ", "'\\(select ");
            fileContent = fileContent.replaceAll("' RENAME ", "'rename ").replaceAll("' Rename ", "'rename ").replaceAll("' rename ", "'rename ");
            fileContent = fileContent.trim().replaceAll("( )+", " ");//replace multiple spaces with single space between words
            fileContent = fileContent.replaceAll(" = N'", " = '").replaceAll(" = n'", " = '");

        } catch (Exception e) {
            e.printStackTrace();
        }
        return fileContent;
    }

    private static String getSQLfromUtilProc(String sql, String utilProcName, String appendSQL) {

        HashMap<String, String> sqlVarMap;
        try {
            sqlVarMap = getSqlVariablesMap(sql);
            String sql2 = sql.replaceAll(", '", ",'");
            sql2 = sql2.replaceAll("''", "!");
            sql2 = sql2.replaceAll(" =", "=");
            sql2 = sql2.replaceAll("= ", "=");
            sql2 = sql2.replaceAll(", ", ",");
            sql2 = sql2.trim().replaceAll("( )+", " ");//replace multiple spaces with single space between words
            String SchemaName = "";
            String tgtTblName = "";
            String selectQuery = "";
            utilProcName = utilProcName.toLowerCase();

            if (sql2.toLowerCase().contains(utilProcName) && !sql2.toLowerCase().contains(utilProcName + "within")) {
                sql2 = sql2.substring(sql2.toLowerCase().indexOf(utilProcName)).trim();
                if (utilProcName.equalsIgnoreCase("rename object")) {
                    sql2 = sql2.substring(utilProcName.length()).trim();
                    sql2 = sql2.replace(" TO ", " to ");
                    sql2 = sql2.replace(" To ", " to ");
                    String fromTbl = sql2.substring(0, sql2.toLowerCase().indexOf(" to ")).trim().replaceAll(":", "");
                    sql2 = sql2.substring(sql2.toLowerCase().indexOf(" to ") + 4).trim();
                    tgtTblName = sql2.substring(0, sql2.indexOf(" ")).trim().replace(";", "");
                    if (fromTbl.length() > 0 && tgtTblName.length() > 0) {
                        appendSQL += "\nINSERT INTO " + tgtTblName + "\nSELECT * FROM " + fromTbl + ";\n";
                    }
                } else {
                    sql2 = sql2.substring(sql2.indexOf(",") + 1).trim();
                    SchemaName = sql2.substring(0, sql2.indexOf(",")).trim().replaceAll("'", "");
                    if (SchemaName.startsWith("@")) {
                        if (SchemaName.contains("=")) {
                            SchemaName = SchemaName.split("=")[1].trim();
                        } else {
                            SchemaName = getSQLVariableValue(sqlVarMap, SchemaName);
                            if(SchemaName.length() == 0){
                                SchemaName = "TargetSchemaName";
                            }
                        }
                    }
                    sql2 = sql2.substring(sql2.indexOf(",") + 1).trim();
                    tgtTblName = sql2.substring(0, sql2.indexOf(",")).trim().replaceAll("'", "");
                    if (tgtTblName.startsWith("@")) {
                        if (tgtTblName.contains("=")) {
                            tgtTblName = tgtTblName.split("=")[1].trim();
                        } else {
                            tgtTblName = getSQLVariableValue(sqlVarMap, tgtTblName);
                            if(tgtTblName.length() == 0){
                                tgtTblName = "TargetTableName";
                            }
                        }
                    }
                    sql2 = sql2.substring(sql2.indexOf(",") + 1).trim();
                    if (sql2.startsWith("@")) {
                        sql2 = sql2.substring(sql2.indexOf("=") + 1).trim();
                    }
                    sql2 = sql2.substring(1).trim();
                    sql2 = sql2.substring(sql2.indexOf("'") + 1).trim();
                    sql2 = sql2.substring(sql2.indexOf(",") + 1).trim();
                    if (utilProcName.equalsIgnoreCase("util_AppendToTable")) {
                        sql2 = sql2.substring(sql2.indexOf(",") + 2).trim();
                    }
                    String lastParam = ",0";
                    int lastParamInd = 0;
                    lastParamInd = sql2.replaceAll(", 0", ",0").indexOf(lastParam);
                    if(lastParamInd == -1){
                        lastParam = ",@";
                        lastParamInd = sql2.replaceAll(", @", ",@").indexOf(lastParam);
                    }
                    if(lastParamInd > -1){
                        selectQuery = sql2.substring(0, lastParamInd).trim();
                        if (selectQuery.startsWith("@")) {
                            if (selectQuery.contains("=")) {
                                selectQuery = selectQuery.substring(selectQuery.indexOf("=") + 2).trim();
                            } else {
                                selectQuery = getSQLVariableValue(sqlVarMap, selectQuery);
                            }
                        }
                        selectQuery = selectQuery.trim().replaceAll("'", "").replaceAll("!", "'");
                        if (!(tgtTblName.contains("@")) && selectQuery.toLowerCase().contains(" from ")) {
                            appendSQL += "\nINSERT INTO " + SchemaName + "." + tgtTblName + "\n" + selectQuery + ";\n";
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return appendSQL;
    }

    public static String validateSQLForCTE(String sql) throws Exception {

        sql = sql.replaceAll("'--'", "'-'").replaceAll("'---'", "'-'").replaceAll("'----'", "'-'");
        try {
            String[] lineArr = sql.split("\n");
            String uncommentedLine = "";
            boolean isCommentedLine = false;
            String fileContent = "";

            for (String line : lineArr) {
                if (isCommentedLine) {
                    if (line.contains("*/")) {
                        //if "*/" exists in the middle of the line, then extract right part excluding commented part
                        if (uncommentedLine.length() > 0) {
                            line = uncommentedLine + " " + line.substring(line.indexOf("*/") + 2);
                        } else {
                            line = line.substring(line.indexOf("*/") + 2);
                        }
                        if (line.contains("--")) {
                            line = line.substring(0, line.indexOf("--"));
                        }
                        isCommentedLine = false;
                        uncommentedLine = "";
                    } else {
                        continue;
                    }
                }
//                if (line.startsWith("Print '")) {
//                    line = line;
//                }
                if (line.contains("--") && !line.contains("'--") && !line.startsWith("--Dynamic Queries")) {
                    if (line.trim().startsWith("--")) {
                        line = "--";
                    }
                    //if "--" exists in the middle of the line, then extract left part excluding commented part
                    if ((line.contains("/*") && line.indexOf("--") < line.indexOf("/*"))
                            || (!line.contains("/*") && !line.contains("*/"))
                            || (line.contains("/*") && line.contains("*/") && line.indexOf("--") > line.indexOf("*/"))) {
                        line = line.substring(0, line.indexOf("--"));
                    }
                }
                if (line.contains("/*") && line.contains("*/")) {
                    line = line.substring(0, line.indexOf("/*")) + line.substring(line.indexOf("*/") + 2);
                }
                if (line.contains("/*") && !line.contains("*/")) {
                    //if "/*" starts in the middle of the line, then extract left part excluding commented part
                    uncommentedLine = line.substring(0, line.indexOf("/*"));
                    isCommentedLine = true;
                    continue;
                }
                if (line.trim().startsWith("--") && !line.startsWith("--Dynamic Queries")) {
                    line = "";
                }
                //line = line.replaceAll("\t", " ");
                //line = line.trim().replaceAll("( )+", " ");//replace multiple spaces with single space between words
                if (line.length() == 0) {
                    continue;
                } else {
                    fileContent += "\n" + line;
                }
            }
            String part1 = "";
            String part2 = fileContent.replaceAll(", '", ",'");

            while (true) {
                boolean createFlag = false;
                if (part2.toLowerCase().contains("create table ")) {
                    //&& !part2.toLowerCase().replaceAll("'\n", "' ").replaceAll("( )+", " ").replaceAll("' create ", "'create ").contains("'create table ")
                    part1 += part2.substring(0, part2.toLowerCase().indexOf("create table "));
                    part2 = part2.substring(part2.toLowerCase().indexOf("create table "));
                    if (part2.toLowerCase().replaceAll("\n", " ").replaceAll("\r", " ").contains(" as ")) {
                        String sqlTmp = part2;
                        sqlTmp = sqlTmp.substring(13);
                        String tgtTbl = sqlTmp.substring(0, sqlTmp.toLowerCase().replaceAll("\n", " ").replaceAll("\r", " ").indexOf(" as ")).trim();
                        sqlTmp = sqlTmp.substring(sqlTmp.replaceAll("\n", " ").replaceAll("\r", " ").toLowerCase().indexOf(" as ") + 4).trim();

                        if (sqlTmp.replaceAll("\n", " ").replaceAll("\r", " ").toLowerCase().startsWith("with ")) {
                            String sql1 = sqlTmp;
                            int closingBraceIndex = 0;

                            while (true) {
                                HashMap<Integer, String> hm = new HashMap<Integer, String>();
                                hm = skipStringWithinBraces(sql1);
                                for (Map.Entry m : hm.entrySet()) {
                                    closingBraceIndex += (Integer) m.getKey();
                                    sql1 = (String) m.getValue();
                                }
                                if (sql1.trim().charAt(0) != ',') {
                                    break;
                                }
                            }
                            part1 += ";" + sqlTmp.substring(0, closingBraceIndex + 1);
                            part1 += "\nINSERT INTO " + tgtTbl + " ";
                            part2 = sql1;
                        } else {
                            part1 += part2.substring(0, 13);
                            part2 = part2.substring(13);
                        }
                        createFlag = true;
                    }
                }
                if (!createFlag) {
                    break;
                }
            }
            sql = part1 + "\n" + part2;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sql;
    }

    private static HashMap skipStringWithinBraces(String sql) {
        int closingBraceIndex = 0;
        HashMap<Integer, String> returnVal = new HashMap<Integer, String>();
        int firstLeftBrace = sql.indexOf("(");
        String sql1 = sql.substring(firstLeftBrace + 1);
        int leftBraceCount = 1;
        int rightBraceCount = 0;

        for (int i = 0; i < sql1.length(); i++) {
            if (sql1.charAt(i) == '(') {
                leftBraceCount++;
            }
            if (sql1.charAt(i) == ')') {
                rightBraceCount++;
            }
            if (leftBraceCount - rightBraceCount == 0) {
                sql1 = sql1.substring(i + 1);
                closingBraceIndex = firstLeftBrace + i + 2;
                break;
            }
        }
        returnVal.put(closingBraceIndex, sql1);
        return returnVal;
    }

    public static boolean checkfileparsing(String query, String dbvender) {
        try {
            dbVendor = getEDbVendorType(dbvender);

            sqlparser = new TGSqlParser(dbVendor);
            sqlparser.sqltext = query;
            int ret = sqlparser.parse();
            if (ret == 0) {
                return true;
            }
        } catch (Exception e) {

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

    public static String extractDynamicSQLs(String sql) throws Exception {
/*
        //File file = new File("D:\\ExeterFinance\\DynamicProcs_in_1450\\bi_bureau_efx_InquiryLoad.sql");
        //File file = new File("D:\\ExeterFinance\\FromClient\\QA_1450\\rpt_ExeterBPA_POALetterCRTAS.sql");
        //D:\ExeterFinance\FromClient\QA_1450\bi_AspectVia_ac_uip_timezone_Load.sql
        //D:\ExeterFinance\FromClient\QA_1450\bi_AspectVia_cb_appointment_Load.sql
        File file = new File("D:\\ExeterFinance\\DynamicProcs_in_1450\\rpt_ventureencoding_Lab_DeclineLettersStatementCRTAS.sql");
        FileReader fileReader = new FileReader(file);
        BufferedReader br = new BufferedReader(fileReader);
        FileInputStream fis = new FileInputStream(file);
        byte[] byteArray = new byte[(int) file.length()];
        fis.read(byteArray);
        sql = new String(byteArray);
*/
        try {
            String fileContent = getFileContent(sql);
            String part1 = "";
            String part2 = fileContent;
            String appendSQL = "--Dynamic Queries Start-----";
//            while (true) {
//                String ketyWord = "exec(";
//                int keWordInd = -1;
//                if (!part2.toLowerCase().contains(ketyWord)) {
//                    ketyWord = "exec ";
//                    if (part2.toLowerCase().contains(ketyWord)) {
//                        keWordInd = part2.toLowerCase().indexOf(ketyWord);
//                    }
//                }
//                if (keWordInd > -1) {
//                    part1 += part2.substring(0,keWordInd + 5);
//                    part2 = part2.substring(keWordInd + 5);
//                    String execString = "";
//                    if (ketyWord.contains("(") && part2.contains(")")) {
//                        execString = part2.substring(0, part2.indexOf(")"));
//                        part1 += part2.substring(0, part2.indexOf(")"));
//                        part2 = part2.substring(part2.indexOf(")"));
//                    }
//                    else if( part2.contains(" ")){
//                        execString = part2.substring(0, part2.trim().indexOf(" "));
//                        part1 += part2.substring(0, part2.trim().indexOf(" "));
//                        part2 = part2.substring(part2.trim().indexOf(" "));
//                    }
//                    if(execString.length() > 0){
//                        HashMap<String, String> sqlVarMap;
//                        sqlVarMap = getSqlVariablesMap(part1);
//                        String sqlVars[] = execString.split("\\+");
//                        for (String sqlVar : sqlVars) {
//                            appendSQL += "\n" + getSQLVariableValue(sqlVarMap,sqlVar.trim());
//                        }
//                    }
//                } else break;
//            }

            appendSQL += getDynamicSQLs(fileContent, " = 'create table ");
            appendSQL += getDynamicSQLs(fileContent, " = 'delete ");
            appendSQL += getDynamicSQLs(fileContent, " = 'insert ");
            appendSQL += getDynamicSQLs(fileContent, " = '(select ");
            appendSQL += parseRenameTableObject(fileContent, " = 'rename object");
            appendSQL += "\n--Dynamic Queries End-----";
            sql += "\n" + appendSQL;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sql;
    }

    private static String getDynamicSQLs(String sql, String keyWord) {

        String appendSQL = "";
        String part2 = sql;
        while (true) {
            int firsQuote = -1;
            if (part2.toLowerCase().contains(keyWord)) {
                firsQuote = part2.toLowerCase().indexOf(keyWord);
            }
            if (firsQuote >= 0) {
                part2 = part2.substring(firsQuote + 1);
                int quotesCount = 1;
                boolean isConcat = false;
                int leftBraceCount = 0;
                int rightBraceCount = 0;
                String dynamicSQL = "";
                boolean skipQuote = false;
                for (int i = 4; i < part2.length(); i++) {
                    boolean skipChar = false;
                    if ((quotesCount % 2 == 1 && part2.charAt(i - 1) == '\'' && skipQuote)
                            || (quotesCount % 2 == 0 && part2.charAt(i - 1) == '\'' && part2.charAt(i) == '+')
                            || (quotesCount % 2 == 0 && part2.charAt(i - 1) == '+')) {
                        skipChar = true;
                        skipQuote = false;
                    }
                    if (!skipChar) {
                        dynamicSQL += part2.charAt(i - 1);
                    }
                    if (quotesCount % 2 == 0) {
                        if (leftBraceCount > 0) {
                            if (part2.charAt(i) == '(') leftBraceCount++;
                            if (part2.charAt(i) == ')') rightBraceCount++;
                            if (leftBraceCount - rightBraceCount == 0) {
                                leftBraceCount = 0;
                                continue;
                            }
                        } else if (part2.charAt(i) == '\'') {
                            quotesCount = 1;
                            isConcat = false;
                            skipQuote = true;
                            continue;
                        } else if (part2.charAt(i) == '+') {
                            isConcat = true;
                            continue;
                        } else if (isConcat && part2.charAt(i) == '(') {
                            leftBraceCount = 1;
                            rightBraceCount = 0;
                            continue;
                        } else if (isConcat && part2.charAt(i) != ' ') {
                            isConcat = true;
                            continue;
                        } else {
                            part2 = part2.substring(i);
                            break;
                        }
                    }
                    if (part2.charAt(i) == '\'') quotesCount++;
                }
                if (dynamicSQL.charAt(dynamicSQL.length() - 1) == '\'') {
                    dynamicSQL = dynamicSQL.substring(0, dynamicSQL.length() - 1);
                }
                dynamicSQL = cleanUpCreateQuery(dynamicSQL);
                appendSQL += "\n" + dynamicSQL + "\n";
            } else break;
        }
        return appendSQL;
    }

    private static HashMap getSqlVariablesMap(String sql) {

        String part2 = sql;
        HashMap<String, String> sqlVarMap = new HashMap<String, String>();

        while (true) {
            if (part2.toLowerCase().contains("set @")) {
                part2 = part2.substring(part2.toLowerCase().indexOf("set @") + 4);
                String varName = "";
                if (part2.contains(" = ")) {
                    varName = part2.substring(0, part2.indexOf(" = "));
                    part2 = part2.substring(part2.indexOf(" = ") + 3);
                }
                int quotesCount = 1;
                boolean isConcat = false;
                int leftBraceCount = 0;
                int rightBraceCount = 0;
                String dynamicSQL = "";
                boolean skipQuote = false;
                if (part2.startsWith("'")) {
                    for (int i = 1; i < part2.length(); i++) {
                        boolean skipChar = false;
                        if ((quotesCount % 2 == 1 && part2.charAt(i - 1) == '\'' && skipQuote)
                                || (quotesCount % 2 == 0 && part2.charAt(i - 1) == '\'' && part2.charAt(i) == '+')
                                || (quotesCount % 2 == 0 && part2.charAt(i - 1) == '+')) {
                            skipChar = true;
                            skipQuote = false;
                        }
                        if (!skipChar && i > 1) {
                            dynamicSQL += part2.charAt(i - 1);
                        }
                        if (quotesCount % 2 == 0) {
                            if (leftBraceCount > 0) {
                                if (part2.charAt(i) == '(') leftBraceCount++;
                                if (part2.charAt(i) == ')') rightBraceCount++;
                                if (leftBraceCount - rightBraceCount == 0) {
                                    leftBraceCount = 0;
                                    continue;
                                }
                            } else if (part2.charAt(i) == '\'') {
                                quotesCount = 1;
                                isConcat = false;
                                skipQuote = true;
                                continue;
                            } else if (part2.charAt(i) == '+') {
                                isConcat = true;
                                continue;
                            } else if (isConcat && part2.charAt(i) == '(') {
                                leftBraceCount = 1;
                                rightBraceCount = 0;
                                continue;
                            } else if (isConcat && part2.charAt(i) != ' ') {
                                isConcat = true;
                                continue;
                            } else {
                                part2 = part2.substring(i);
                                break;
                            }
                        }
                        if (part2.charAt(i) == '\'') {
                            quotesCount++;
                        }
                    }
                    if (dynamicSQL.charAt(dynamicSQL.length() - 1) == '\'') {
                        dynamicSQL = dynamicSQL.substring(0, dynamicSQL.length() - 1);
                    }
                    dynamicSQL = cleanUpCreateQuery(dynamicSQL);
                    sqlVarMap.put(varName, dynamicSQL);
                }
            } else break;
        }
        return sqlVarMap;
    }

    private static String getSQLVariableValue(HashMap<String, String> sqlVarMap, String variableName) {

        String retValue = "";

        for (Map.Entry m : sqlVarMap.entrySet()) {
            String sqlVar = (String) m.getKey();
            if (sqlVar.equalsIgnoreCase(variableName)) {
                retValue = (String) m.getValue();
                break;
            }
        }
        return retValue;
    }

    private static String cleanUpCreateQuery(String sql) {
        //sql = sql.replaceAll("\\+'","'").replaceAll("'\\+","'");
        //sql = sql.replaceAll("''''", "@@@@");
        //sql = sql.replaceAll("'''", "").replaceAll("''", " ").replaceAll("'", "");
        //sql = sql.replaceAll("''", "'");
        //sql = sql.replaceAll("@@@@", "''");
        //sql = sql.replaceAll("\\[", "").replaceAll("\\]", "");
        sql = sql.trim().replaceAll("( )+", " ");//replace multiple spaces with single space between words

        sql = validateTableNames(sql, "create table ");
        sql = validateTableNames(sql, "delete ");
        sql = validateTableNames(sql, "insert into ");
        sql = validateTableNames(sql, "insert ");
        sql = validateTableNames(sql, " from ");
        sql = validateTableNames(sql, " join ");
        sql = validateTableNamesInWhereClause(sql);
        sql = validateColumnNames(sql);

        //remove at(Data Source = @TargetServerName; User ID = @TargetUserID; Password = @TargetPassword;)
        if (sql.toLowerCase().contains("create table ") && sql.toLowerCase().contains("at(")) {
            int closingBraceIndex = 0;
            int firstLeftBrace = sql.indexOf("at(");
            String sql1 = sql.substring(firstLeftBrace + 3);
            int leftBraceCount = 1;
            int rightBraceCount = 0;

            for (int i = 0; i < sql1.length(); i++) {
                if (sql1.charAt(i) == '(') leftBraceCount++;
                if (sql1.charAt(i) == ')') rightBraceCount++;
                if (leftBraceCount - rightBraceCount == 0) {
                    sql1 = sql1.substring(i + 1);
                    closingBraceIndex = firstLeftBrace + i + 4;
                    break;
                }
            }
            String part2 = sql.substring(closingBraceIndex + 1);
            sql = sql.substring(0, firstLeftBrace);
            sql += " " + part2;
        }
        if (sql.toLowerCase().contains("create table ") && !sql.toLowerCase().contains("select ")) {
            sql = "";
        }
        sql = sql.replaceAll("\\)CONVERT",")+CONVERT").replaceAll("\\)Convert",")+CONVERT").replaceAll("\\)convert",")+CONVERT");
        sql = sql.replaceAll("\\)CHAR\\(","\\)+CHAR\\(").replaceAll("\\)Char\\(","\\)+CHAR\\(").replaceAll("\\)char\\(","\\)+CHAR\\(");
        sql = sql.replaceAll(" exec @"," exec ");
        sql = sql.replaceAll("DROP TABLE ","drop table ");
        sql = sql.replaceAll("Drop Table ","drop table ");
        sql = sql.replaceAll("drop table @","drop table ");
        sql = sql.replaceAll("RENAME OBJECT","rename object");
        sql = sql.replaceAll("Rename Object","rename object");
        sql = sql.replaceAll("::"," ");
        sql = sql.replaceAll("rename object @","\nrename object ");

        return sql;
    }

    private static String validateTableNames(String sql, String keyWord) {
        String part1 = "";
        String part2 = sql;
        String tblNm = "";

        while (true) {
            if (part2.toLowerCase().contains(keyWord)) {
                part1 += part2.substring(0, part2.toLowerCase().indexOf(keyWord) + keyWord.length());
                part2 = part2.substring(part2.toLowerCase().indexOf(keyWord) + keyWord.length());
                if (part2.indexOf(" ") > 0) {
                    tblNm = part2.substring(0, part2.indexOf(" "));
                } else {
                    tblNm = part2;
                }
                if (tblNm.contains(".")) {
                    String[] parts = tblNm.split("\\.");
                    tblNm = parts[parts.length - 2] + "." + parts[parts.length - 1];
                }
                part1 += tblNm.replaceAll("@", "").replaceAll("\\[", "").replaceAll("\\]", "");
                if (part2.indexOf(" ") > 0) {
                    part2 = part2.substring(part2.indexOf(" "));
                } else part2 = "";
            } else {
                break;
            }
        }
        part1 += part2;
        return part1;
    }

    private static String validateColumnNames(String sql) {
        sql = sql.replaceAll("DISTINCT "," ").replaceAll("Distinct "," ").replaceAll("distinct "," ");
        String part1 = "";
        String part2 = sql;
        String colNames = "";
        String keyWord = "select ";

        while (true) {
            if (part2.toLowerCase().contains(keyWord)) {
                part1 += part2.substring(0, part2.toLowerCase().indexOf(keyWord) + keyWord.length());
                part2 = part2.substring(part2.toLowerCase().indexOf(keyWord) + keyWord.length());
                if (part2.indexOf(" from ") > 0) {
                    colNames = part2.substring(0, part2.indexOf(" from "));
                    if (colNames.contains("=")) {
                        List<String> colList = new ArrayList();
                        String colNm = "";
                        int leftBraceCount = 0;
                        int rightBraceCount = 0;
                        boolean skipChar = false;
                        for (int i = 1; i < colNames.length(); i++) {
                            if (!skipChar) {
                                colNm += colNames.charAt(i - 1);
                            } else {
                                skipChar = false;
                            }
                            if (i == colNames.length() - 1) {
                                colNm += colNames.charAt(i);
                                colList.add(colNm.trim());
                            } else if (leftBraceCount > 0) {
                                if (colNames.charAt(i) == '(') leftBraceCount++;
                                if (colNames.charAt(i) == ')') rightBraceCount++;
                                if (leftBraceCount - rightBraceCount == 0) {
                                    leftBraceCount = 0;
                                    continue;
                                }
                            } else if (colNames.charAt(i) == ',') {
                                colList.add(colNm.trim());
                                colNm = "";
                                skipChar = true;
                                continue;
                            } else if (colNames.charAt(i) == '(') {
                                leftBraceCount = 1;
                                rightBraceCount = 0;
                                continue;
                            }
                        }
                        colNames = "";
                        for (String col : colList) {
                            col = col.trim();
                            boolean splitCol = false;
                            if (col.contains("=")) {
                                if (!col.toLowerCase().contains("case ")) {
                                    splitCol = true;
                                } else {
                                    if (col.toLowerCase().indexOf("case ") > col.indexOf("=")) {
                                        splitCol = true;
                                    }
                                }
                                if (col.toLowerCase().contains("row_number()")) {
                                    splitCol = false;
                                }
                            }
                            if (splitCol) {
                                String colPart1 = col.substring(0, col.indexOf("=")).trim();
                                String colPart2 = col.substring(col.indexOf("=") + 1).trim();
                                col = colPart2 + " AS " + colPart1;
                            }
                            colNames += col + ",";
                        }
                        colNames = colNames.substring(0, colNames.length() - 1);
                    }
                } else {
                    break;
                }
                part1 += colNames.replaceAll("@", "");
                if (part2.indexOf(" from ") > 0) {
                    part2 = part2.substring(part2.indexOf(" from "));
                }
            } else {
                break;
            }
        }
        part1 += part2;
        return part1;
    }

    private static String validateTableNamesInWhereClause(String sql) {
        String part1 = "";
        String part2 = sql;
        String keyWord = " where ";

        while (true) {
            if (part2.toLowerCase().contains(keyWord)) {
                part1 += part2.substring(0, part2.toLowerCase().indexOf(keyWord) + keyWord.length());
                part2 = part2.substring(part2.toLowerCase().indexOf(keyWord) + keyWord.length());
                String[] parts = part2.split(" ");
                if (parts.length > 2 && parts[1].equals("=")) {
                    if (parts[0].contains(".")) {
                        String[] parts0 = parts[0].split("\\.");
                        parts[0] = parts0[parts0.length - 2] + "." + parts0[parts0.length - 1];
                        parts[0] = parts[0].replaceAll("@", "");
                    }
                    if (parts[2].contains(".")) {
                        String[] parts2 = parts[2].split("\\.");
                        parts[2] = parts2[parts2.length - 2] + "." + parts2[parts2.length - 1];
                        parts[2] = parts[2].replaceAll("@", "");
                    }
                    part1 += parts[0] + " = " + parts[2] + " ";
                    part2 = "";
                    if (parts.length > 3) {
                        for (int j = 3; j < parts.length; j++) {
                            part2 += parts[j] + " ";
                        }
                    }
                }
            } else {
                break;
            }
        }
        part1 += part2;
        return part1;
    }

    private static String parseRenameTableObject(String sql, String keyWord) {
        sql = sql.replaceAll(" \\+", "").replaceAll("\\+ ", "");
        sql = sql.replaceAll("::", "");
        sql = sql.trim().replaceAll("( )+", " ");//replace multiple spaces with single space between words

        String part1 = "";
        String part2 = sql;
        String dynamicSQL = "";

        while (true) {
            if (part2.toLowerCase().contains(keyWord)) {
                part1 += part2.substring(0, part2.toLowerCase().indexOf(keyWord) + keyWord.length());
                part2 = part2.substring(part2.toLowerCase().indexOf(keyWord) + keyWord.length());
                String renameFrom = part2.substring(0, part2.indexOf(" to "));
                part1 += part2.substring(0, part2.toLowerCase().indexOf(" to ") + 4);
                part2 = part2.substring(part2.toLowerCase().indexOf(" to ") + 4);
                String renameTo = "";
                renameTo = part2.substring(0, part2.indexOf("'"));
                if (renameFrom.contains(".")) {
                    String[] parts = renameFrom.split("\\.");
                    renameFrom = parts[parts.length - 2] + "." + parts[parts.length - 1];
                }
                part1 += " to " + renameTo;
                part2 = part2.substring(part2.indexOf("'"));
                dynamicSQL += "\nrename object " + renameFrom + " to " + renameTo + "\n";
            } else {
                break;
            }
        }
        return dynamicSQL;
    }

}
