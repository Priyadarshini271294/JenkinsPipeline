/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sqlToJsonenterprise;

import demos.columnImpact.ColumnImpact;
import demos.dlineage.DataFlowAnalyzer;
import java.io.File;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author sashikant D
 */
public class SqlQuerycreator {

    public static void main(String[] args) {
        try {

            String sqlfileContent = FileUtils.readFileToString(new File("C:\\Users\\sashikant D\\Downloads\\KB40IX_PARTY_IDN_01D.sql"));
            String teradataxml = DataFlowAnalyzer.getanalyzeXmlfromString(sqlfileContent, "teradata");
//            System.out.println("====" + teradataxml);
            ColumnImpact.getImpactResultforQuery();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
