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
public class Cfxsqlfileseparator {

    public static void main(String[] args) {
        File sqlfiles = new File("D:\\Issue");
        writefiles(sqlfiles);
    }

    public static void writefiles(File sqlfiles) {
        try {
            String sqlfileContent = "";
            File[] sqlfilearr = sqlfiles.listFiles(); 
            for (File fgh : sqlfilearr) { 
                String extension = FilenameUtils.getExtension(fgh.getName());
                if (extension.equalsIgnoreCase("log")) {
                    sqlfileContent = LogfileParser.doparselogfilenio(Paths.get(fgh.getAbsolutePath()));
                } else {
                    sqlfileContent = FileUtils.readFileToString(fgh).trim();
                }
                 if(extension.equalsIgnoreCase("dtsx")){
                 sqlfileContent = Ssisqueryextractor.getquery(fgh.getAbsolutePath());
                 
                 } 
                sqlfileContent = sqlfileContent.replaceAll("\\/\\*[\\s\\S]*?\\*\\/|([^:]|^)\\/\\/.*$", "");
                if (sqlfileContent.trim().split(";").length >= 1) {
                    String multiplesqlfiles[] = sqlfileContent.split(";");
                    int i = 0;
                    for (int j = 0; j < multiplesqlfiles.length; j++) {
                        String multiplesqlfile = multiplesqlfiles[j].trim();

                        File sqlfile = new File(fgh.getAbsolutePath().replace(extension,"") + "" + i + ".sql");
                        FileUtils.writeStringToFile(sqlfile, multiplesqlfile);

                        i++;
                        fgh.delete();

                    }
                }
            }

        } catch (Exception e) {

        }

    }

}
