/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ads.sqlparser.v1;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author ads
 */
public class SQLToJSONGenerationteststoreproc {

    public static void main(String[] args) {
        getJson();
    }

      public static void getJson() {

        try {
            File sqlfiles = new File("E:\\EDFtest\\output");
            File[] sqlfilearr = sqlfiles.listFiles();
            for (File fgh : sqlfilearr) {
                CfxSqltoJsonv8Edflambda sqltojsonobj = new CfxSqltoJsonv8Edflambda();
                 String fileName = FileUtils.readFileToString(fgh);
//        String name = fileName.split("\n")[0].trim();
//        String procedurename =name.split("].")[1].replace("[", "") ;
//        procedurename=fileName;
       // System.out.println("===="+fileName);
                String jsonn1 = sqltojsonobj.sqlToDataflow("E:\\JsonFile",fgh.getAbsolutePath(), "Redshift", "EDFenv", "Redshift", "EdfEnv", "teradata", "E:\\Goldparser\\files\\grammerBr.egt","tab",fileName);
                Map<String, String> keyvalue = new LinkedHashMap<>(sqltojsonobj.keyvaluepair);
                sqltojsonobj.getClear();              
                System.out.println("=====" + jsonn1);
            }
//            sqlToDataflow("C:\\Users\\ads\\Desktop\\newgspjar\\sql\\RaS_CommunicationPreferences_delete.txt","C:\\Users\\ads\\Desktop\\New folder\\New folder", "Redshift", "EDFenv", "Redshift", "EdfEnv");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
