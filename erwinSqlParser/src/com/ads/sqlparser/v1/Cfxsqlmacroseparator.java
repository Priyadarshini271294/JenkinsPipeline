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
public class Cfxsqlmacroseparator { 

    public static void main(String[] args) {
        File sqlfiles = new File("E:\\sqlfiles");
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
                sqlfileContent = getMacroquery(sqlfileContent);
                sqlfileContent = sqlfileContent.replaceAll("\\/\\*[\\s\\S]*?\\*\\/|([^:]|^)\\/\\/.*$", "");
                if (sqlfileContent.trim().split(";").length > 1) {
                    String multiplesqlfiles[] = sqlfileContent.split(";");
                    int i = 0;
                    for (int j = 0; j < multiplesqlfiles.length; j++) {
                        String multiplesqlfile = multiplesqlfiles[j].trim();

                        File sqlfile = new File(fgh.getAbsolutePath() + "" + i + ".sql");
                        FileUtils.writeStringToFile(sqlfile, multiplesqlfile);

                        i++;
                        fgh.delete();

                    }
                }
            }

        } catch (Exception e) {

        }

    }

    public static String getMacroquery(String sqlstring) {
        // TODO code application logic here
        StringBuilder sb1 = new StringBuilder();
        StringBuilder macrofilequery = new StringBuilder();
         String dbName ="";
         boolean asflag = false ;
        try {

            String[] macrolines = sqlstring.split("\n");
            int a = 0;
            int f = 0;
            for (String macroline : macrolines) {
                 if(macroline.toUpperCase().contains("DATABASE")){
                  dbName = macroline.split("\\s+")[1];
                 }
                if (macroline.toUpperCase().contains("DATABASE") ) {
                    continue;
                }
                if(macroline.toUpperCase().contains("REPLACE MACRO")&&macroline.toUpperCase().contains("AS")){
                asflag=true;
                }
                if(macroline.toUpperCase().contains("REPLACE MACRO")){ 
                continue;
                }
                
                if (a == 0 && macroline.toUpperCase().contains("AS")&&!asflag) {
                    a++;
                    continue;

                }
                if (f == 0 && macroline.contains("(")) {
                    f++;
                    continue;
                }
                if(macroline.toUpperCase().startsWith("QUALIFY")||macroline.toUpperCase().contains("QUALIFY"))
                {
                    continue;
                }

                sb1.append(macroline).append("\n");

            }
            
            String line = sb1.toString().trim();
            String firstline = line.split("\n")[0];
            String querybeforeappend = line.replace(firstline, "");
            String viewstring = firstline.trim().replace("REPLACE VIEW","");
            
            String firstlinequery = dbName.replace(";","").trim()+"."+viewstring.trim();
            
            String finalquery = "REPLACE VIEW "+firstlinequery.trim()+"\n"+querybeforeappend;
                    //
                 macrofilequery.append(finalquery);
            System.out.println("string is " + sb1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
       
        
        
        return macrofilequery.toString();
    }

}
