/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ads.sqlparser.v1;

import demos.getstatement.getstatement;
import java.util.List;
import java.util.Set;

/**
 *
 * @author sashikant D
 */
public class Storeproctextclass {
    
    public static void main(String[] args) {
        String path = "E:\\EDFtest\\dbo.Analytical_1.StoredProcedurezz.sql";
        
        Set<String> storeprocfiles = getstatement.getallstatement(path, "mssql");
    }
    
}
