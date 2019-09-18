/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sqlToJsonenterprise;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author ads
 */
public class SQLToJSONGeneration {
    public static void main(String[] args) {
        getJson();
    }
    public static void getJson(){
       
        try {
            File sqlfiles = new File("E:\\Sqlfile\\sql\\sql");
            File[] sqlfilearr = sqlfiles.listFiles();
            for (File fgh : sqlfilearr) {
                SqlqueryGeneratorNAB sqltojsonobj = new  SqlqueryGeneratorNAB();
                String jsonn = sqltojsonobj.sqlToDataflow(fgh.getAbsolutePath(),"E:\\JsonFile","Redshift", "EDFenv", "Redshift", "EdfEnv","teradata");
                Map<String,String> keyvalue = new LinkedHashMap<>(sqltojsonobj.keyvaluepair);

                sqltojsonobj.getClear();
                System.out.println("====="+jsonn);
            }
//            sqlToDataflow("C:\\Users\\ads\\Desktop\\newgspjar\\sql\\RaS_CommunicationPreferences_delete.txt","C:\\Users\\ads\\Desktop\\New folder\\New folder", "Redshift", "EDFenv", "Redshift", "EdfEnv");
        } catch (Exception e) {
            e.printStackTrace();
        }
       
       
    }
}
