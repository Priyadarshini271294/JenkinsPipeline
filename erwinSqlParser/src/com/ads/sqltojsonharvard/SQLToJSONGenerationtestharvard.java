/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ads.sqltojsonharvard;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;


/**
 *
 * @author ads
 */
public class SQLToJSONGenerationtestharvard {

    public static void main(String[] args) {
        getJson();
    }

      public static void getJson() {

        try {
            File sqlfiles = new File("E:\\Edfquery\\edfqueryyy");
            File[] sqlfilearr = sqlfiles.listFiles();
            for (File fgh : sqlfilearr) {
          //      CfxSqltoJsonv8Edf sqltojsonobj = new CfxSqltoJsonv8Edf();
//                String jsonn1 = sqltojsonobj.sqlToDataflow("E:\\JsonFile",fgh.getAbsolutePath(), "Redshift", "EDFenv", "Redshift", "EdfEnv", "teradata", "E:\\Goldparser\\files\\grammerBr.egt","tab");
//                Map<String, String> keyvalue = new LinkedHashMap<>(sqltojsonobj.keyvaluepair);
//                sqltojsonobj.getClear();              
//                System.out.println("=====" + jsonn1);
            }
//            sqlToDataflow("C:\\Users\\ads\\Desktop\\newgspjar\\sql\\RaS_CommunicationPreferences_delete.txt","C:\\Users\\ads\\Desktop\\New folder\\New folder", "Redshift", "EDFenv", "Redshift", "EdfEnv");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
