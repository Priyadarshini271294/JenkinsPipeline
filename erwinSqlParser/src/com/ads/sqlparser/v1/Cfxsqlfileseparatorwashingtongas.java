/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ads.sqlparser.v1;

import java.io.File;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

/**
 *
 * @author sashikant D
 */
public class Cfxsqlfileseparatorwashingtongas {

    public static void main(String[] args) {
        File sqlfiles = new File("E:\\EDFtest");
        writefiles(sqlfiles);
    }

    public static void writefiles(File sqlfiles) {
        try {
            String sqlfileContent = "";
            File[] sqlfilearr = sqlfiles.listFiles();
            for (File fgh : sqlfilearr) {
                String extension = FilenameUtils.getExtension(fgh.getName());
                if (extension.equalsIgnoreCase("log") || extension.equalsIgnoreCase("pck")) {
                    sqlfileContent = LogfileParser.doparselogfilenio(Paths.get(fgh.getAbsolutePath()));
                } else {
                    sqlfileContent = FileUtils.readFileToString(fgh).trim();
                }

                sqlfileContent = sqlfileContent.replaceAll("\\/\\*[\\s\\S]*?\\*\\/|([^:]|^)\\/\\/.*$", "");
                if (sqlfileContent.trim().split(";").length > 1) {
                    String multiplesqlfiles[] = sqlfileContent.split(";");
                    int i = 0;
                    for (int j = 0; j < multiplesqlfiles.length; j++) {
                        String multiplesqlfile = multiplesqlfiles[j].trim();
                        if (multiplesqlfile.contains("-")) {
                            multiplesqlfile = multiplesqlfile.replaceFirst("-", "_");
                        }
                        File sqlfile = new File(fgh.getAbsolutePath() + "" + i + ".sql");
                        FileUtils.writeStringToFile(sqlfile, multiplesqlfile);

                        i++;
                        fgh.delete();

                    }
                } else if (sqlfileContent.trim().contains("-")) {

                    sqlfileContent = sqlfileContent.replaceFirst("-", "_");
                    File newsqlfile = new File(fgh.getAbsolutePath().replace("sql", "").replace(".", "") + "c" + ".sql");
                    FileUtils.writeStringToFile(newsqlfile, sqlfileContent);
                   

                }
            }

        } catch (Exception e) {
          e.printStackTrace();
        } 

    }

}
