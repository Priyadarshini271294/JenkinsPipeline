/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ads.sqlToJsonenterprise.bnf;

import com.ads.sqlparser.v1.ErwinSQLParser;
import demos.columnAnalyze.ColumnAnalyze;
import static demos.columnAnalyze.ColumnAnalyze.tablecolumnwithoutwhere;

/**
 *
 * @author sashikant D
 */
public class Columnanalysetestclass {
    public static void main(String[] args) {
//        String query = "select Country FROM SAMPLE_VAULT.dbo.Country_Sales where Country NOT IN('South Africa','Sri Lanka')";
        String query = "SELECT KEY_NAME FROM ADS_KEY_VALUE where OBJECT_TYPE_ID IN (11,12);";
        ColumnAnalyze.getcolumnanalyze(query,"mssql");
        System.out.println(ColumnAnalyze.tablecolumnMap);
        System.out.println("----columns without where "+ColumnAnalyze.tablecolumnwithoutwhere);
        System.out.println("columnsss---"+ColumnAnalyze.columns);
      String joinconditions=  ErwinSQLParser.querynoinType(query, "mssql");
        System.out.println("---"+joinconditions);
    }
    
    
}
