/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ads.sqlparser.v1;

import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.sqltojsonharvard.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.codehaus.jackson.map.ObjectMapper;


/**
 *
 * @author ads
 */
public class EdftestClass {

    public static void main(String[] args) {
        getJson();
    }

      public static void getJson() {
        try {
            File sqlfiles = new File("D:\\Issue");
            File[] sqlfilearr = sqlfiles.listFiles();
            for (File fgh : sqlfilearr) { 
                modifyQuery(fgh.getAbsolutePath());
                CfxSqltoJsonv8Edf sqltojsonobj = new CfxSqltoJsonv8Edf();
                System.out.println("99999999999"+sqltojsonobj); 
                String jsonn1 = sqltojsonobj.sqlToDataflow("D:\\jsonfiles",fgh.getAbsolutePath(), "Redshift", "EDFenv", "Redshift", "EdfEnv", "oracle","","", fgh.getName());              
                Map<String, String> keyvalue =  sqltojsonobj.getKeyvalueJson();
               // ArrayList<MappingSpecificationRow> maprow = sqltojsonobj.getSpecification();
               // System.out.println(maprow);
                sqltojsonobj.getClear();               
                //System.out.println("=====" + jsonn1);
            }
//            sqlToDataflow("C:\\Users\\ads\\Desktop\\newgspjar\\sql\\RaS_CommunicationPreferences_delete.txt","C:\\Users\\ads\\Desktop\\New folder\\New folder", "Redshift", "EDFenv", "Redshift", "EdfEnv");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
      
    public static void modifyQuery(String sqlFilePath) {
        try {
            //File sqlfiles = new File("D:\\Issue");
            //sqlfiles.get
            String content = readAllBytesJava7(sqlFilePath);
            ObjectMapper objectMapper = new ObjectMapper();
            int j = 0;
            Matcher m = Pattern.compile("\\(([^)]+)\\)").matcher(content);
            while (m.find()) {
                if (j == 0) {
                    String sourcecolumnName = m.group(1);
                    String[] lines = sourcecolumnName.split("\\n");
                    StringBuilder sb = new StringBuilder();

                    for (String sourceColName : lines) {
                        sourceColName = sourceColName.trim();
                        if (sourceColName.contains(" ")) {
                            sourceColName = sourceColName.replace(",", "");
                            if (!sourceColName.contains("'")) {
                                sourceColName = "\'" + sourceColName + "\'" + ",";
                            }
                        }
                        sb.append(sourceColName);
                    }
                }
                j++;
            }
            objectMapper.writeValue(new File(sqlFilePath), content);

        } catch (Exception e) {
        }
    }
     private static String readAllBytesJava7(String sqlFilePath) {
        String content = "";
        try {
            content = new String(Files.readAllBytes(Paths.get(sqlFilePath)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return content;
    }
}
