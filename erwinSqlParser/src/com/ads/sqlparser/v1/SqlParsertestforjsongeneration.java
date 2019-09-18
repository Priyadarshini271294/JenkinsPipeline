/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ads.sqlparser.v1;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author ads
 */
public class SqlParsertestforjsongeneration {

    public static void main(String[] args) {
        getJson();
    }

    public static void getJson() {
        List<String> jsonlist = new LinkedList<>();
        try {
            File sqlfiles = new File("E:\\EDFtest");
            File[] sqlfilearr = sqlfiles.listFiles();
            for (File fgh : sqlfilearr) {
                CfxSqltoJsonv8Edflambda sqltojsonobj = new CfxSqltoJsonv8Edflambda();
                String sqlfileContent = FileUtils.readFileToString(fgh).trim();
                sqlfileContent = sqlfileContent.replaceAll("\\/\\*[\\s\\S]*?\\*\\/|([^:]|^)\\/\\/.*$", "");
                if (sqlfileContent.trim().split(";").length > 1) {
                    String multiplesqlfiles[] = sqlfileContent.split(";");
                    int i = 0;
                    for (int j=0;j<multiplesqlfiles.length;j++) {
                        String multiplesqlfile = multiplesqlfiles[j];
                        
                        File sqlfile = new File(fgh.getAbsolutePath() + "" + i + ".sql");
                        FileUtils.writeStringToFile(sqlfile, multiplesqlfile);
                   //    String jsonn1 = sqltojsonobj.sqlToDataflow("E:\\JsonFile", sqlfile.getAbsolutePath(), "Redshift", "EDFenv", "Redshift", "EdfEnv", "teradata", "E:\\Goldparser\\files\\grammerBr.egt", "tab");
                       // jsonlist.add(jsonn1);
                        i++;
                        fgh.delete();
                        sqltojsonobj.getClear();
                    }
                } else {
                   // String jsonn1 = sqltojsonobj.sqlToDataflow("E:\\JsonFile", fgh.getAbsolutePath(), "Redshift", "EDFenv", "Redshift", "EdfEnv", "teradata", "E:\\Goldparser\\files\\grammerBr.egt", "tab");
                    sqltojsonobj.getClear();
                }

                Map<String, String> keyvalue = new LinkedHashMap<>(sqltojsonobj.keyvaluepair);
                sqltojsonobj.getClear();
//                System.out.println("=====" + jsonn1);
            }
//            sqlToDataflow("C:\\Users\\ads\\Desktop\\newgspjar\\sql\\RaS_CommunicationPreferences_delete.txt","C:\\Users\\ads\\Desktop\\New folder\\New folder", "Redshift", "EDFenv", "Redshift", "EdfEnv");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
