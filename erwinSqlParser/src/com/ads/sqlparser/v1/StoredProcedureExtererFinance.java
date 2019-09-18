/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ads.sqlparser.v1;

import demos.getstatement.getstatement;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

/**
 *
 * @author sashikant D
 */
public class StoredProcedureExtererFinance {

    public static void main(String[] args) {
        // List<String> storeproclist=   getstatement.getallstatement("C:\\Users\\sashikant D\\Desktop\\teststoreprocedure.sql","mssql");
        //storeproclist.forEach(System.out::println);
        parseStoreprocintomultiplefile("E:\\sqlfiles", "sqlserver");
    }

    public static String parseStoreprocintomultiplefile(String storeProcFilePath, String dbVender) {
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
                            String tabname = getRenameObjTabName(storeproc);
                            String changeTabName = "";
                            String removedstr = changeWithClause(storeproc);
                            List<String> tableList = getRenameObjTabNameList(storeproc);
                            for (String tablename : tableList) {
                                if (!"".equals(tablename)) {
                                    changeTabName = tablename.split("##")[0].trim();
                                    if (tablename.split("##")[0].trim().contains(".")) {
                                        changeTabName = tablename.split("##")[0].trim().split("\\.")[1];
                                    }
                                    removedstr = removedstr.replace(changeTabName, tablename.split("##")[1].trim());
                                }
                                BufferedWriter bw = new BufferedWriter(new FileWriter(sqlfile));
                                bw.write(removedstr);
                                bw.flush();
                                bw.close();
                            }

                        } else {
                            sqlfile = new File(parentpath + "/output/" + inputFileName.replace(".sql", "") + i + ".sql");
                            String tabname = getRenameObjTabName(storeproc);
                            String changeTabName = "";
                            String removedstr = changeWithClause(storeproc);
                            List<String> tableList = getRenameObjTabNameList(storeproc);
                            for (String tablename : tableList) {
                                if (!"".equals(tablename)) {
                                    changeTabName = tablename.split("##")[0].trim();
                                    if (tablename.split("##")[0].trim().contains(".")) {
                                        changeTabName = tablename.split("##")[0].trim().split("\\.")[1];
                                    }
                                    removedstr = removedstr.replace(changeTabName, tablename.split("##")[1].trim());
                                }
                                BufferedWriter bw = new BufferedWriter(new FileWriter(sqlfile));
                                bw.write(removedstr);
                                bw.flush();
                                bw.close();
                                i++;
                            }
                        }
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
                withfile = withfile.trim().replaceAll("( )+", " ");//replace multiple spaces with single space between words
                withfile = withfile.replace("with (", "with(");
                withfile = withfile.replace("WITH (", "WITH(");
                withfile = withfile.replace("over (", "over(");
                withfile = withfile.replace("OVER (", "OVER(");
                int firstLeftBrace = 0;
                int leftBraceCount = 1;
                int rightBraceCount = 0;
                String part1 = "";
                String part2 = "";

                if (withfile.toLowerCase().contains("with(")
                        && withfile.toLowerCase().contains("distribution")) {

                    firstLeftBrace = withfile.indexOf("(");

                    for (int i = firstLeftBrace + 1; i < withfile.length(); i++) {
                        if (withfile.charAt(i) == '(') {
                            leftBraceCount++;
                        }
                        if (withfile.charAt(i) == ')') {
                            rightBraceCount++;
                        }
                        if (leftBraceCount - rightBraceCount == 0) {
                            if (firstLeftBrace > 5) {
                                part1 = withfile.substring(0, firstLeftBrace - 5);
                            }
                            if (i + 1 < withfile.length()) {
                                part2 = withfile.substring(i + 1);
                            }
                            withfile = part1 + " " + part2;
                            break;
                        }
                    }
                } else if (withfile.toLowerCase().contains("over(")
                        && (withfile.toLowerCase().contains("rows ") || withfile.toLowerCase().contains("range "))) {

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
                    withfile = "";
                }
                strbf.append(withfile + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return strbf.toString();
    }

    public static String getRenameObjTabName(String query) {
        String[] linearr = query.split("\n");
        for (String queryline : linearr) {
            if (queryline.startsWith("rename")) {
                queryline = queryline.replace("rename object", "");
                String tabname = queryline.split("to")[0] + "##" + queryline.split("to")[1];
                return tabname;
            }
        }
        return "";
    }

    public static List<String> getRenameObjTabNameList(String query) {
        List<String> tablelist = new ArrayList();
        String[] linearr = query.split("\n");
        for (String queryline : linearr) {
            if (queryline.startsWith("rename")) {
                queryline = queryline.replace("rename object", "");
                String tabname = queryline.split("to")[0] + "##" + queryline.split("to")[1];
                tablelist.add(tabname);
            }
        }
        return tablelist;
    }

}
