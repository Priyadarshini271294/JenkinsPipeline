/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ads.sqlparser.v1;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author sashikant D
 */
public class LogfileParser {

    private static int i = 0;

    public static void main(String[] args) {
//        File logfile = new File("E:\\ceasers\\Caesars\\ESP_All_Sub.03.13964.ESP_All_Sub_Populate_All_Sub.btq.log");
//        doparselogfile(logfile);
        Path path = Paths.get("E:\\EDFtest\\pkg_account_holder (1).pck");
        String logs = doparselogfilenio(path);
        
        
        System.out.println("-------"+logs); 
    }

    public static String doparselogfile(File logfile) {
        try {
            String logfilecontent = FileUtils.readFileToString(logfile);
            String[] filecontent = logfilecontent.split("\n");
            int i = 0;
            StringBuilder sb = new StringBuilder();
            for (String logfileline : filecontent) {
                if (logfileline.startsWith("/*") && i == 0) {
                    sb = sb.insert(0, "/*");
                    sb.append("*/");
                    sb.append(logfileline);
                    i++;
                } else if (logfileline.trim().startsWith("***") && i != 0) {
                    sb.append("/*").append(logfileline).append("*/");
                } else if (logfileline.trim().startsWith("+---------") && i != 0) {
                    sb.append("/*").append(logfileline).append("*/");
                } else if (logfileline.trim().startsWith(".IF ERRORCODE <> ") && i != 0) {
                    sb.append("/*").append(logfileline).append("*/");
                } else {
                    sb.append(logfileline);
                }
                return sb.toString();
            }
            System.out.println("-----" + sb.toString());
        } catch (Exception e) {

        }
        return "";
    }

    public static String doparselogfilenio(Path logfile) {
        try {

            Stream<String> stringcontent = Files.readAllLines(logfile).stream();
            final StringBuilder sb = new StringBuilder();

            stringcontent.forEach((x) -> {

                if (x.startsWith("/*") && i == 0) {
                    sb.insert(0, "/*");
                    sb.append("*/");
                    sb.append(x);
                    i++;

                } else if (x.trim().startsWith("***") && i != 0) {
                    sb.append("/*").append(x).append("*/");
                } else if (x.trim().startsWith("+---------") && i != 0) {
                    sb.append("/*").append(x).append("*/");
                } else if (x.trim().startsWith(".IF ERRORCODE <> ") && i != 0) {
                    sb.append("/*").append(x).append("*/");
                } else {
                    sb.append(x).append("\n");
                }

            });

            return sb.toString();
        } catch (Exception e) {

        }
        return "";
    }

}
