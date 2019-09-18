/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ads.sqlToJsonenterprise.bnf;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author ads
 */
public class SQLToJSONGeneration {

    public static void main(String[] args) {
        getJson();
    }

    public static void getJson() {

        try {
            File sqlfiles = new File("E:\\Sqlfile\\sql\\sql");
            File[] sqlfilearr = sqlfiles.listFiles();
            for (File fgh : sqlfilearr) {
                CfxSqltoJsonv2 sqltojsonobj = new CfxSqltoJsonv2();
                CfxSqltoJsonSSISV1 cfxtest = new CfxSqltoJsonSSISV1();
                CfxSqltoJsonv2jsonfile cfxjsonfile = new CfxSqltoJsonv2jsonfile();

                List<String> analysetype = new LinkedList();
                 analysetype.add("dataflow");
              //  analysetype.add("impact");
//                  analysetype.add("dataflowandimpact");
                if (analysetype.size() == 1) {
                    String jsofile = cfxjsonfile.sqlToDataflow("E:\\JsonFile", fgh.getAbsolutePath(), "Redshift", "EDFenv", "Redshift", "EdfEnv", "teradata", "E:\\Goldparser\\files\\grammerBr.egt", analysetype.get(0));
                    cfxjsonfile.getKeyvalueJson();
                } else {
                    for (String type : analysetype) {
                        String jsofile = cfxjsonfile.sqlToDataflow("E:\\JsonFile", fgh.getAbsolutePath(), "Redshift", "EDFenv", "Redshift", "EdfEnv", "teradata", "E:\\Goldparser\\files\\grammerBr.egt", type);
                        cfxjsonfile.getKeyvalueJson();
                    }
                }
//                String jsonn = sqltojsonobj.sqlToDataflow(fgh.getAbsolutePath(), "Redshift", "EDFenv", "Redshift", "EdfEnv", "oracle", "E:\\Goldparser\\files\\grammerBr.egt");
//                String jsonn1 = cfxtest.sqlToDataflow(fgh.getAbsolutePath(), "Redshift", "EDFenv", "Redshift", "EdfEnv", "mssql", "E:\\Goldparser\\files\\grammerBr.egt");

                Map<String, String> keyvalue = new LinkedHashMap<>(cfxjsonfile.keyvaluepair);
                System.out.println("keyssss"+cfxjsonfile.keyvaluepair);
                sqltojsonobj.getClear();
                cfxtest.getClear();
                //System.out.println("=====" + jsofile);
            }
//            sqlToDataflow("C:\\Users\\ads\\Desktop\\newgspjar\\sql\\RaS_CommunicationPreferences_delete.txt","C:\\Users\\ads\\Desktop\\New folder\\New folder", "Redshift", "EDFenv", "Redshift", "EdfEnv");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
