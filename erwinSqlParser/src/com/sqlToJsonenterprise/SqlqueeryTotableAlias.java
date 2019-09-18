/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sqlToJsonenterprise;

/**
 *
 * @author sashikant D
 */
import demos.columnAnalyze.ColumnAnalyze;
import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class SqlqueeryTotableAlias {

    public static EDbVendor dbVendor = null;
    public static TGSqlParser sqlparser = null;
    public static EDbVendor dbvenderType = null;

    public static void main(String[] args) {
        String sqlString = new String("SELECT \n"
                    + "    \n"
                    + "dlice.ID_SRGT_LOAN_CURR,\n"
                    + "    \n"
                    + "dlice.ID_PRTTN_CONT,\n"
                    + "    \n"
                    + "dlice.ID_LOAN_SYST_GEND,\n"
                    + "    \n"
                    + "DM.DESC_MKTG as DESC_LOAN_MKTG \n"
                    + "\n"
                    + "FROM\n"
                    + "   \n"
                    + "udbadm.d_loan_i_curr_extn dlice \n"
                    + "   \n"
                    + "INNER JOIN udbadm.loan_fundd_i_rvsd lfi \n"
                    + "    \n"
                    + "ON dlice.id_loan_syst_gend=lfi.id_loan_syst_gend\n"
                    + "    \n"
                    + "INNER JOIN udbadm.loan_pch_cont_i_h lpi\n"
                    + "    \n"
                    + "ON          lfi.id_loan_cont_gend=lpi.id_loan_cont_gend \n"
                    + "    AND        lpi.DT_SRCE_END='9999-01-01-00.00.00.000000'\n"
                    + "    AND        lpi.FLAG_DEL='N'\n"
                    + "    AND        lpi.cd_execm=1 \n"
                    + "    AND        lpi.cd_int_rate=1\n"
                    + "INNER JOIN     (   /* for desception marcketing which is product type */\n"
                    + "     \n"
                    + "SELECT id_intgrtd_pe as id_intgrtd_pe, cd_exec_type, desc_mktg\n"
                    + "         \n"
                    + "FROM udbadm.ofr_pe_prodt_assn  \n"
                    + "inner join udbadm.cd_ofr_prodt on  udbadm.ofr_pe_prodt_assn.cd_ofr_prodt= udbadm.cd_ofr_prodt.cd_ofr_prodt \n"
                    + "       \n"
                    + "UNION\n"
                    + "     \n"
                    + "SELECT id_prodt_intgrtd as id_intgrtd_pe, cd_exec_type, desc_mktg\n"
                    + "         \n"
                    + "FROM udbadm.cd_ofr_prodt\n"
                    + "    ) DM \n"
                    + "    \n"
                    + "ON (lpi.id_prodt_intgrtd = dm.id_intgrtd_pe and lpi.cd_execm = dm.cd_exec_type)");

        sqlString = "E:\\shashi\\Projects\\Queries\\query3.txt";
//        wrtiteStringIntoFile();
        Map<String, Set<String>> columnmap = getTableColumnMap(sqlString, "oracle");
        System.out.println("====" + columnmap);
    }

    public static void wrtiteStringIntoFile(String filePath, String query) {
        try {
            File file = new File(filePath);
            FileOutputStream fot = new FileOutputStream(file);
            fot.write(query.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Map<String, Set<String>> getTableColumnMap(String FilePath, String dbvender) {
        Map<String, Set<String>> tableColumnAliasMap = null;
        try {
            String sqlfilepath = FilePath;
            File filessql = new File(sqlfilepath);
            dbvenderType = getEDbVendorType(dbvender);
            ColumnAnalyze.getcolumnanalyze(FilePath);
            tableColumnAliasMap = createTableAliasMap(ColumnAnalyze.tablecolaliasMap);
//            System.out.println("-----" + tableColumnAliasMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tableColumnAliasMap;
    }

    public static Map<String, Set<String>> createTableAliasMap(Map<String, String> columnImapctMap) {
        Map<String, Set<String>> columnaliasMap = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : columnImapctMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (value.contains(",")) {
                String[] columnarr = value.split(",");
                Set<String> columnset = new LinkedHashSet<>(Arrays.asList(columnarr));
                columnaliasMap.put(key, columnset);
            } else {
                Set<String> columnset = new LinkedHashSet<>();
                columnset.add(value);
                columnaliasMap.put(key, columnset);
            }

        }
        return columnaliasMap;
    }

    public static EDbVendor getEDbVendorType(String dbVender) {
        EDbVendor dbVendor = EDbVendor.dbvpostgresql;

        if (dbVender != null) {
            if (dbVender.equalsIgnoreCase("mssql")) {
                dbVendor = EDbVendor.dbvmssql;

            } else if (dbVender.equalsIgnoreCase("db2")) {
                dbVendor = EDbVendor.dbvdb2;
            } else if (dbVender.equalsIgnoreCase("mysql")) {
                dbVendor = EDbVendor.dbvmysql;
            } else if (dbVender.equalsIgnoreCase("postgresql")) {
                dbVendor = EDbVendor.dbvpostgresql;
            } else if (dbVender.equalsIgnoreCase("mssql")) {
                dbVendor = EDbVendor.dbvmssql;
            } else if (dbVender.equalsIgnoreCase("oracle")) {
                dbVendor = EDbVendor.dbvoracle;
            } else if (dbVender.equalsIgnoreCase("netezza")) {
                dbVendor = EDbVendor.dbvnetezza;
            } else if (dbVender.equalsIgnoreCase("teradata")) {
                dbVendor = EDbVendor.dbvteradata;
            }
        }
        return dbVendor;
    }

}
