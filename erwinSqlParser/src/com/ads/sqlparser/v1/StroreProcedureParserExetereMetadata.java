package com.ads.sqlparser.v1;

import static com.ads.sqlparser.v1.ErwinSQLParser.dbVendor;
import static com.ads.sqlparser.v1.ErwinSQLParser.getEDbVendorType;
import static com.ads.sqlparser.v1.ErwinSQLParser.sqlparser;
import demos.getstatement.getstatement;
import gudusoft.gsqlparser.TGSqlParser;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

/**
 *
 * @author sashikant D
 */
public class StroreProcedureParserExetereMetadata { 

    public static void main(String[] args) throws Exception {
        // List<String> storeproclist=   getstatement.getallstatement("C:\\Users\\sashikant D\\Desktop\\teststoreprocedure.sql","mssql");
        parseStoreprocintomultiplefile("E:\\sqlfiles\\test\\ExeterProcs\\Exeter_9_Procs", "mssql",null);
        //String in = "with(distribution = hash(ApplicationNumber), clustered columnstore index)";
        String out = validateSQLForCTE("");
        //String out = extractUtilProcSQLs("");
        //String out = changeWithClause(in);
        //System.out.println("|"+out+"|");
    }

    public static String parseStoreprocintomultiplefile(String storeProcFilePath, String dbVender, Map<String, List<String>> tablecolfrommetadata) {
        Set<String> storeproclist = null;
        File storeProcFile = null;
        String inputFileName = "";
        StringBuilder sb = new StringBuilder();
        try {
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
                        updatedSQL = changeWithClause(storeproc);
                        String utilprocs = extractUtilProcSQLs(updatedSQL);
                        updatedSQL = validateSQLForCTE(utilprocs);
                        if (!checkfileparsing(updatedSQL, "mssql")) {
                            updatedSQL = utilprocs;
                        }
                        String aliasstar = QueryParserForStarV1.getChangedForAliasQuery(updatedSQL,tablecolfrommetadata);
                        String updatedqueryForStar = QueryParserForStarV1.getChangedQuery(aliasstar);
                        if ("".equalsIgnoreCase(updatedqueryForStar)) {
                            updatedqueryForStar = updatedSQL;
                        }
                        if (!checkfileparsing(updatedqueryForStar, "mssql")) {
                            updatedqueryForStar = updatedSQL;
                        }

                        //updatedqueryForStar = QueryParserForStar.getChangedQuery(updatedSQL);
                        BufferedWriter bw = new BufferedWriter(new FileWriter(sqlfile));
                        bw.write(updatedqueryForStar);
                        bw.flush();
                        bw.close();
                        // FileUtils.writeStringToFile(sqlfile, removedstr);
                    }
//            File deletefile = new File(listofstoreprocFile.getAbsolutePath());
//            FileUtils.forceDelete(deletefile);
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
                    };
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
                } else if (withfile.trim().toLowerCase().contains("rename object")) {
                    withfile = withfile.trim();
                    if (withfile.trim().startsWith("--")) {
                        withfile = "";
                    } else {
                        withfile = withfile.trim().substring(13);
                        withfile = withfile.replace(" TO ", " to ");
                        withfile = withfile.replace(" To ", " to ");
                        String fromTbl = withfile.substring(0, withfile.toLowerCase().indexOf(" to ")).trim().replaceAll(":", "");
                        String toTbl = withfile.substring(withfile.toLowerCase().indexOf(" to ") + 4).trim().replace(";", "");
                        if (fromTbl.length() > 0 && toTbl.length() > 0) {
                            String schmaNm = "";
                            if (fromTbl.contains(".")) {
                                schmaNm = fromTbl.substring(0, fromTbl.indexOf("."));
                            }
                            if (!toTbl.contains(".") && schmaNm.length() > 0) {
                                toTbl = schmaNm + "." + toTbl;
                            }
                            withfile = "INSERT INTO " + toTbl + " SELECT * FROM " + fromTbl + ";";

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
        /*
        File file = new File("D:\\ExeterFinance\\FromClient\\test11.sql");
        FileReader fileReader = new FileReader(file);
        BufferedReader br = new BufferedReader(fileReader);
        FileInputStream fis = new FileInputStream(file);
        byte[] byteArray = new byte[(int) file.length()];
        fis.read(byteArray);
        sql = new String(byteArray);
         */
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
                line = line.trim().replaceAll("( )+", " ");//replace multiple spaces with single space between words
                if (line.length() == 0) {
                    continue;
                }
                fileContent += " " + line;
            }
            String appendSQL = "";
            appendSQL = getSQLfromUtilProc(fileContent, "util_AppendToTable", appendSQL);
            appendSQL = getSQLfromUtilProc(fileContent, "util_CreateTableAs", appendSQL);
            //appendSQL = getSQLfromUtilProc(fileContent, "rename object", appendSQL);
            sql += appendSQL;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sql;
    }

    private static String getSQLfromUtilProc(String sql, String utilProcName, String appendSQL) {

        String sqlTmp = sql.replaceAll(", '", ",'");
        sqlTmp = sqlTmp.replaceAll("''", "!");
        String SchemaName = "";
        String tgtTblName = "";
        utilProcName = utilProcName.toLowerCase();

        if (sqlTmp.toLowerCase().contains(utilProcName)) {
            sqlTmp = sqlTmp.substring(sqlTmp.toLowerCase().indexOf(utilProcName)).trim();
            if (utilProcName.equalsIgnoreCase("rename object")) {
                sqlTmp = sqlTmp.substring(utilProcName.length()).trim();
                sqlTmp = sqlTmp.replace(" TO ", " to ");
                sqlTmp = sqlTmp.replace(" To ", " to ");
                String fromTbl = sqlTmp.substring(0, sqlTmp.toLowerCase().indexOf(" to ")).trim().replaceAll(":", "");
                //if (fromTbl.contains(".")) {
                //fromTbl = fromTbl.split("\\.")[1];
                //}
                sqlTmp = sqlTmp.substring(sqlTmp.toLowerCase().indexOf(" to ") + 4).trim();
                tgtTblName = sqlTmp.substring(0, sqlTmp.indexOf(" ")).trim().replace(";", "");

                if (fromTbl.length() > 0 && tgtTblName.length() > 0) {
                    appendSQL += "\nINSERT INTO " + tgtTblName + "\nSELECT * FROM " + fromTbl + ";\n";
                }
            } else {
                sqlTmp = sqlTmp.substring(sqlTmp.indexOf(",") + 1).trim();
                SchemaName = sqlTmp.substring(0, sqlTmp.indexOf(",")).trim().replaceAll("'", "");
                sqlTmp = sqlTmp.substring(sqlTmp.indexOf(",") + 1).trim();
                tgtTblName = sqlTmp.substring(0, sqlTmp.indexOf(",")).trim().replaceAll("'", "");
                sqlTmp = sqlTmp.substring(sqlTmp.indexOf(",") + 2).trim();
                sqlTmp = sqlTmp.substring(sqlTmp.indexOf("'") + 1).trim();
                sqlTmp = sqlTmp.substring(sqlTmp.indexOf(",") + 2).trim();
                if (utilProcName.equalsIgnoreCase("util_AppendToTable")) {
                    sqlTmp = sqlTmp.substring(sqlTmp.indexOf(",") + 2).trim();
                }
                if (sqlTmp.toLowerCase().contains(" from ")) {
                    sqlTmp = sqlTmp.substring(0, sqlTmp.indexOf("'")).trim();
                    sqlTmp = sqlTmp.replaceAll("!", "'");
                    appendSQL += "\nINSERT INTO " + SchemaName + "." + tgtTblName + "\n" + sqlTmp + ";\n";
                }
            }
        }
        return appendSQL;
    }

    public static String validateSQLForCTE(String sql) throws Exception {
        /*
        File file = new File("D:\\ExeterFinance\\FromClient\\1450\\edw_AccountInsuranceLoad.sql");
        FileReader fileReader = new FileReader(file);
        BufferedReader br = new BufferedReader(fileReader);
        FileInputStream fis = new FileInputStream(file);
        byte[] byteArray = new byte[(int) file.length()];
        fis.read(byteArray);
        sql = new String(byteArray);
         */
        try {
            String part1 = "";
            String part2 = sql.replaceAll(", '", ",'");
            while (true) {
                boolean createFlag = false;
                if (part2.toLowerCase().contains("create table ")) {
                    part1 += part2.substring(0, part2.toLowerCase().indexOf("create table "));
                    part2 = part2.substring(part2.toLowerCase().indexOf("create table "));
                    String sqlTmp = part2;
                    sqlTmp = sqlTmp.substring(13);
                    String tgtTbl = sqlTmp.substring(0, sqlTmp.toLowerCase().replaceAll("\n", " ").indexOf(" ")).trim();
                    sqlTmp = sqlTmp.substring(sqlTmp.replaceAll("\n", " ").replaceAll("\r", " ").toLowerCase().indexOf(" as ") + 4).trim();

                    if (sqlTmp.toUpperCase().startsWith("WITH ")) {
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
                if (!createFlag) {
                    break;
                }
            }
            sql = part1 + part2;
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
}
