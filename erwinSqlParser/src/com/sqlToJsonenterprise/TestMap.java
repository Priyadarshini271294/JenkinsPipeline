/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sqlToJsonenterprise;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author sashikant D
 */
public class TestMap {
    public static void main(String[] args) {
        Map<String,List<String>> tablewithcol = new HashMap();
        String tableName = "tableName";
        
        List<String> listtt= new ArrayList<>();
        listtt.add("N_RUN_SKEY");
        listtt.add("N_APRA_RATING_GRADE");
        
        tablewithcol.put(tableName, listtt);
        
        // HashMap <TargetTable~TargetColumnName, BR>  tgtBr---- ==  Specification count 
        
        String br1 = "SUM(CASE WHEN FRCC.N_APRA_RATING_GRADE = 1 THEN\n" +
"                     case when N_RUN_SKEY>10 than \"Hi\"  else \"bye\" \n" +
"                ELSE\n" +
"                       0\n" +
"                END)\n" +
"				";
        
        // for (tgtBr) == >BR
        //br1=Br
        for(String key: tablewithcol.keySet()) {
            
            List<String>  list  = tablewithcol.get(key);
            for(String data: list) {
                
                if(br1.contains(data)) {
                    System.out.println(" ====  "+true);
                } else {
                    System.out.println(" ====  "+false);
                }
                
            }
            
            
        }
        
    }
}
