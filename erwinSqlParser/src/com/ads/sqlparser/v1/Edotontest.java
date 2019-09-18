/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ads.sqlparser.v1;

import java.io.File;
import org.apache.commons.io.FileUtils;
 
/**
 *
 * @author ads
 */
public class Edotontest {

    public static void main(String[] args) throws Exception {
        
        
           getJson();
    }

    public static void getJson() {

        try {
            //"E:\\JsonFile",
            long starttime = System.currentTimeMillis();

            File sqlfiles = new File("C:\\Users\\InkolluReddy\\Downloads\\Microsoft.SkypeApp_kzf8qxf38zg5c!App\\All"); 
            File[] sqlfilearr = sqlfiles.listFiles();
            for (File file : sqlfilearr) {
                        CfxSqltoJsonv8Edf sqltojsonobj = new CfxSqltoJsonv8Edf();
                        System.out.println("-----" + file.getName());
                        String jsonn1 = sqltojsonobj.sqlToDataflow("D:\\jsonfile",file.getAbsolutePath(), "Redshift", "EDFenv", "Redshift", "EdfEnv", "mssql","","",file.getName());
                        System.out.println(".........."+jsonn1);
                        
                    
             int i=0;
                

//                System.out.println("filesize"+file.getName());
//              System.out.println("jsonnn"+jsonn1);    
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static String changeWithoutStar(String query) {
        StringBuilder sb = new StringBuilder();
        String[] queryarr = query.split("\n");

        for (String queryline : queryarr) {

            if (queryline.trim().startsWith("*,")) {

                continue;
            } else if (queryline.trim().contains("*")) {
                String line = "";
                if (queryline.trim().split(" ").length > 1) {
                  String[] linearr = queryline.split(" ");
                  
                    for (String star : linearr) {
                        if(star.contains("*")){
                        continue;
                        }
                        
                        line = line +" "+star;
                          sb.append(line + "\n");
                    }
                }

            }

            sb.append(queryline + "\n");
        }

        return sb.toString();

    }
}
