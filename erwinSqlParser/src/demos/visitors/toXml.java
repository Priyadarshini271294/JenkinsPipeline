package demos.visitors;

import gudusoft.gsqlparser.*;
import java.io.*;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class toXml {

    public static String dbName = "";
    public static EDbVendor dbVendor = EDbVendor.dbvpostgresql;

    public static void main(String args[]) throws IOException {
        long t = System.currentTimeMillis();

        if (args.length < 1) {
            System.out.println("Usage: java toXml sqlfile.sql [/t <database type>]");
            System.out.println("/t: Option, set the database type. Support oracle, mysql, mssql and db2, the default type is mysql");
            return;
        }
        File file = new File(args[0]);
        if (!file.exists()) {
            System.out.println("File not exists:" + args[0]);
            return;
        }

        EDbVendor dbVendor = EDbVendor.dbvmssql;

        List<String> argList = Arrays.asList(args);
        int index = argList.indexOf("/t");

        if (index != -1 && args.length > index + 1) {
            if (args[index + 1].equalsIgnoreCase("mssql")) {
                dbVendor = EDbVendor.dbvmssql;
            } else if (args[index + 1].equalsIgnoreCase("db2")) {
                dbVendor = EDbVendor.dbvdb2;
            } else if (args[index + 1].equalsIgnoreCase("mysql")) {
                dbVendor = EDbVendor.dbvmysql;
            } else if (args[index + 1].equalsIgnoreCase("mssql")) {
                dbVendor = EDbVendor.dbvmssql;
            } else if (args[index + 1].equalsIgnoreCase("oracle")) {
                dbVendor = EDbVendor.dbvoracle;
            } else if (args[index + 1].equalsIgnoreCase("netezza")) {
                dbVendor = EDbVendor.dbvnetezza;
            } else if (args[index + 1].equalsIgnoreCase("teradata")) {
                dbVendor = EDbVendor.dbvteradata;
            }

        }
       // System.out.println("Selected SQL dialect: " + dbVendor.toString());

        TGSqlParser sqlparser = new TGSqlParser(dbVendor);
        sqlparser.sqlfilename = args[0];
        String xmlFile = args[0] + ".xml";

        int ret = sqlparser.parse();
        if (ret == 0) {
            String xsdfile = "file:/C:/prg/gsp_java/library/doc/xml/sqlschema.xsd";
            xmlVisitor xv2 = new xmlVisitor(xsdfile);
            xv2.run(sqlparser);
            xv2.validXml();
            xv2.writeToFile(xmlFile);
           // System.out.println(xmlFile + " was generated!");

        } else {
            System.out.println(sqlparser.getErrormessage());
        }

     //   System.out.println("Time Escaped: "
               // + (System.currentTimeMillis() - t));
    }

    public static void getXml(String sqlFile, String dbVender) throws IOException {

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
            } else if (dbVender.equalsIgnoreCase("oracle")) {
                dbVendor = EDbVendor.dbvoracle;
            } else if (dbVender.equalsIgnoreCase("netezza")) {
                dbVendor = EDbVendor.dbvnetezza;
            } else if (dbVender.equalsIgnoreCase("teradata")) {
                dbVendor = EDbVendor.dbvteradata;
            } else if (dbVender.equalsIgnoreCase("access")) {
                dbVendor = EDbVendor.dbvaccess;
            } else if (dbVender.equalsIgnoreCase("generic")) {
                dbVendor = EDbVendor.dbvgeneric;
            } else if (dbVender.equalsIgnoreCase("sybase")) {
                dbVendor = EDbVendor.dbvsybase;
            } else if (dbVender.equalsIgnoreCase("informix")) {
                dbVendor = EDbVendor.dbvinformix;
            } else if (dbVender.equalsIgnoreCase("firebird")) {
                dbVendor = EDbVendor.dbvfirebird;
            } else if (dbVender.equalsIgnoreCase("mdx")) {
                dbVendor = EDbVendor.dbvmdx;
            } else if (dbVender.equalsIgnoreCase("ansi")) {
                dbVendor = EDbVendor.dbvansi;
            } else if (dbVender.equalsIgnoreCase("odbc")) {
                dbVendor = EDbVendor.dbvodbc;
            } else if (dbVender.equalsIgnoreCase("hive")) {
                dbVendor = EDbVendor.dbvhive;
            } else if (dbVender.equalsIgnoreCase("greenplum")) {
                dbVendor = EDbVendor.dbvgreenplum;
            } else if (dbVender.equalsIgnoreCase("redshift")) {
                dbVendor = EDbVendor.dbvredshift;
            } else if (dbVender.equalsIgnoreCase("impala")) {
                dbVendor = EDbVendor.dbvimpala;
            } else if (dbVender.equalsIgnoreCase("hana")) {
                dbVendor = EDbVendor.dbvhana;
            } else if (dbVender.equalsIgnoreCase("dax")) {
                dbVendor = EDbVendor.dbvdax;
            } else if (dbVender.equalsIgnoreCase("vertica")) {
                dbVendor = EDbVendor.dbvvertica;
            } else if (dbVender.equalsIgnoreCase("openedge")) {
                dbVendor = EDbVendor.dbvopenedge;
            } else if (dbVender.equalsIgnoreCase("couchbase")) {
                dbVendor = EDbVendor.dbvcouchbase;
            } else if (dbVender.equalsIgnoreCase("snowflake")) {
                dbVendor = EDbVendor.dbvsnowflake;
            }
        }
       // System.out.println("Selected SQL dialect: " + dbVendor.toString());

        TGSqlParser sqlparser = new TGSqlParser(dbVendor);
        sqlparser.sqlfilename = sqlFile;
        String xmlFile = sqlFile + ".xml";

        int ret = sqlparser.parse();
        if (ret == 0) {
            String xsdfile = "file:/C:/prg/gsp_java/library/doc/xml/sqlschema.xsd";
            xmlVisitor xv2 = new xmlVisitor(xsdfile);
            xv2.run(sqlparser);
            // xv2.validXml();
            // xv2.writeToFile( xmlFile );
           // System.out.println(xmlFile + " was generated!");

        } else {
            System.out.println(sqlparser.getErrormessage());
        }

    }

    public static void getXml2(String sqlFile) throws IOException {

        TGSqlParser sqlparser = null;
        int ret = -1;
        dbVendor = EDbVendor.dbvteradata;
        for (int i = 1; i <= 25; i++) {
            try {
                 if (i == 1) {
                    dbVendor = EDbVendor.dbvmssql;
                    dbName = "mssql";
                } else if (i == 2) {
                    dbVendor = EDbVendor.dbvoracle;
                    dbName = "oracle";
                } else if (i == 3) {
                    dbVendor = EDbVendor.dbvpostgresql;
                    dbName = "postgresql";
                } else if (i == 4) {
                    dbVendor = EDbVendor.dbvredshift;
                    dbName = "redshift";
                } else if (i == 5) {
                    dbVendor = EDbVendor.dbvodbc;
                    dbName = "odbc";
                } else if (i == 6) {
                    dbVendor = EDbVendor.dbvmysql;
                    dbName = "mysql";
                } else if (i == 7) {
                    dbVendor = EDbVendor.dbvnetezza;
                    dbName = "netezza";
                } else if (i == 8) {
                    dbVendor = EDbVendor.dbvfirebird;
                    dbName = "firebird";
                } else if (i == 9) {
                    dbVendor = EDbVendor.dbvaccess;
                    dbName = "access";
                } else if (i == 10) {
                    dbVendor = EDbVendor.dbvansi;
                    dbName = "ansi";
                } else if (i == 11) {
                    dbVendor = EDbVendor.dbvgeneric;
                    dbName = "generic";
                } else if (i == 12) {
                    dbVendor = EDbVendor.dbvgreenplum;
                    dbName = "greenplum";
                } else if (i == 13) {
                    dbVendor = EDbVendor.dbvhive;
                    dbName = "hive";
                } else if (i == 14) {
                    dbVendor = EDbVendor.dbvsybase;
                    dbName = "sysbase";
                } else if (i == 15) {
                    dbVendor = EDbVendor.dbvhana;
                    dbName = "hana";
                } else if (i == 16) {
                    dbVendor = EDbVendor.dbvimpala;
                    dbName = "impala";
                } else if (i == 17) {
                    dbVendor = EDbVendor.dbvdax;
                    dbName = "dax";
                } else if (i == 18) {
                    dbVendor = EDbVendor.dbvvertica;
                    dbName = "vertica";
                } else if (i == 19) {
                    dbVendor = EDbVendor.dbvcouchbase;
                    dbName = "couchbase";
                } else if (i == 20) {
                    dbVendor = EDbVendor.dbvsnowflake;
                    dbName = "snowflake";
                } else if (i == 21) {
                    dbVendor = EDbVendor.dbvopenedge;
                    dbName = "openedge";
                } else if (i == 22) {
                    dbVendor = EDbVendor.dbvinformix;
                    dbName = "informix";
                } else if (i == 23) {
                    dbVendor = EDbVendor.dbvteradata;
                    dbName = "teradata";
                } else if (i == 24) {
                    dbVendor = EDbVendor.dbvmdx;
                    dbName = "mdx";
                } else if (i == 25) {
                    dbVendor = EDbVendor.dbvdb2;
                    dbName = "db2";
                }
//                if (i == 1) {
//                    
//                    dbVendor = EDbVendor.dbvredshift;
//                    dbName = "redshift";
//                } else if (i == 2) {
//                    dbVendor = EDbVendor.dbvoracle;
//                    dbName = "oracle";
//                } else if (i == 3) {
//                    dbVendor = EDbVendor.dbvpostgresql;
//                    dbName = "postgresql";
//                } else if (i == 4) {
//                     dbVendor = EDbVendor.dbvteradata;
//                    dbName = "teradata";
//                    
//                } else if (i == 5) {
//                     dbVendor = EDbVendor.dbvteradata;
//                    dbName = "teradata";
//                    
//                }
                sqlparser = new TGSqlParser(dbVendor);
                sqlparser.sqlfilename = sqlFile;
                ret = sqlparser.parse();
                if (ret == 0) {
//                    actualDBVendor = dbVendor.toString();
                    //.println(" This is suitable to : " + dbVendor);
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
              //  System.out.println(sqlparser.getErrormessage());
            }
        }
        if (ret == 0) {
            String xsdfile = "file:/C:/prg/gsp_java/library/doc/xml/sqlschema.xsd";
            xmlVisitor xv2 = new xmlVisitor(xsdfile);
            xv2.run(sqlparser);
            // xv2.validXml();
            //xv2.writeToFile( xmlFile );
          //  System.out.println("Xml File" + " was generated!");

        } else {
            System.out.println(sqlparser.getErrormessage());
        }
    }
    
     public static void getdbvenderforsqltext(String sqlFile) throws IOException {

        TGSqlParser sqlparser = null;
        int ret = -1;
        dbVendor = EDbVendor.dbvteradata;
        for (int i = 1; i <= 25; i++) {
            try {
                if (i == 1) {
                    dbVendor = EDbVendor.dbvmssql;
                    dbName = "mssql";
                } else if (i == 2) {
                    dbVendor = EDbVendor.dbvoracle;
                    dbName = "oracle";
                } else if (i == 3) {
                    dbVendor = EDbVendor.dbvpostgresql;
                    dbName = "postgresql";
                } else if (i == 4) {
                    dbVendor = EDbVendor.dbvredshift;
                    dbName = "redshift";
                } else if (i == 5) {
                    dbVendor = EDbVendor.dbvodbc;
                    dbName = "odbc";
                } else if (i == 6) {
                    dbVendor = EDbVendor.dbvmysql;
                    dbName = "mysql";
                } else if (i == 7) {
                    dbVendor = EDbVendor.dbvnetezza;
                    dbName = "netezza";
                } else if (i == 8) {
                    dbVendor = EDbVendor.dbvfirebird;
                    dbName = "firebird";
                } else if (i == 9) {
                    dbVendor = EDbVendor.dbvaccess;
                    dbName = "access";
                } else if (i == 10) {
                    dbVendor = EDbVendor.dbvansi;
                    dbName = "ansi";
                } else if (i == 11) {
                    dbVendor = EDbVendor.dbvgeneric;
                    dbName = "generic";
                } else if (i == 12) {
                    dbVendor = EDbVendor.dbvgreenplum;
                    dbName = "greenplum";
                } else if (i == 13) {
                    dbVendor = EDbVendor.dbvhive;
                    dbName = "hive";
                } else if (i == 14) {
                    dbVendor = EDbVendor.dbvsybase;
                    dbName = "sysbase";
                } else if (i == 15) {
                    dbVendor = EDbVendor.dbvhana;
                    dbName = "hana";
                } else if (i == 16) {
                    dbVendor = EDbVendor.dbvimpala;
                    dbName = "impala";
                } else if (i == 17) {
                    dbVendor = EDbVendor.dbvdax;
                    dbName = "dax";
                } else if (i == 18) {
                    dbVendor = EDbVendor.dbvvertica;
                    dbName = "vertica";
                } else if (i == 19) {
                    dbVendor = EDbVendor.dbvcouchbase;
                    dbName = "couchbase";
                } else if (i == 20) {
                    dbVendor = EDbVendor.dbvsnowflake;
                    dbName = "snowflake";
                } else if (i == 21) {
                    dbVendor = EDbVendor.dbvopenedge;
                    dbName = "openedge";
                } else if (i == 22) {
                    dbVendor = EDbVendor.dbvinformix;
                    dbName = "informix";
                } else if (i == 23) {
                    dbVendor = EDbVendor.dbvteradata;
                    dbName = "teradata";
                } else if (i == 24) {
                    dbVendor = EDbVendor.dbvmdx;
                    dbName = "mdx";
                } else if (i == 25) {
                    dbVendor = EDbVendor.dbvdb2;
                    dbName = "db2";
                }
                sqlparser = new TGSqlParser(dbVendor);
                sqlparser.sqltext = sqlFile;
                ret = sqlparser.parse();
                if (ret == 0) {
//                    actualDBVendor = dbVendor.toString();
                   // System.out.println(" This is suitable to : " + dbVendor);
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
               // System.out.println(sqlparser.getErrormessage());
            }
        }
        if (ret == 0) {
            String xsdfile = "file:/C:/prg/gsp_java/library/doc/xml/sqlschema.xsd";
            xmlVisitor xv2 = new xmlVisitor(xsdfile);
            xv2.run(sqlparser);
            // xv2.validXml();
            //xv2.writeToFile( xmlFile );
           // System.out.println("Xml File" + " was generated!");

        } else {
            System.out.println(sqlparser.getErrormessage());
        }
    }

    public static void checkdbtype(String sqlQuery) throws IOException {

        TGSqlParser sqlparser = null;
        int ret = -1;
        dbVendor = EDbVendor.dbvteradata;
        for (int i = 1; i <= 25; i++) {
            try {
                if (i == 1) {
                    dbVendor = EDbVendor.dbvmssql;
                    dbName = "mssql";

                } else if (i == 2) {
                    dbVendor = EDbVendor.dbvoracle;
                    dbName = "oracle";
                } else if (i == 3) {
                    dbVendor = EDbVendor.dbvpostgresql;
                    dbName = "postgresql";
                } else if (i == 4) {
                    dbVendor = EDbVendor.dbvredshift;
                    dbName = "redshift";
                } else if (i == 5) {
                    dbVendor = EDbVendor.dbvodbc;
                    dbName = "odbc";
                } else if (i == 6) {
                    dbVendor = EDbVendor.dbvmysql;
                    dbName = "mysql";
                } else if (i == 7) {
                    dbVendor = EDbVendor.dbvnetezza;
                    dbName = "netezza";
                } else if (i == 8) {
                    dbVendor = EDbVendor.dbvfirebird;
                    dbName = "firebird";
                } else if (i == 9) {
                    dbVendor = EDbVendor.dbvaccess;
                    dbName = "access";
                } else if (i == 10) {
                    dbVendor = EDbVendor.dbvansi;
                    dbName = "ansi";
                } else if (i == 11) {
                    dbVendor = EDbVendor.dbvgeneric;
                    dbName = "generic";
                } else if (i == 12) {
                    dbVendor = EDbVendor.dbvgreenplum;
                    dbName = "greenplum";
                } else if (i == 13) {
                    dbVendor = EDbVendor.dbvhive;
                    dbName = "hive";
                } else if (i == 14) {
                    dbVendor = EDbVendor.dbvsybase;
                    dbName = "sysbase";
                } else if (i == 15) {
                    dbVendor = EDbVendor.dbvhana;
                    dbName = "hana";
                } else if (i == 16) {
                    dbVendor = EDbVendor.dbvimpala;
                    dbName = "impala";
                } else if (i == 17) {
                    dbVendor = EDbVendor.dbvdax;
                    dbName = "dax";
                } else if (i == 18) {
                    dbVendor = EDbVendor.dbvvertica;
                    dbName = "vertica";
                } else if (i == 19) {
                    dbVendor = EDbVendor.dbvcouchbase;
                    dbName = "couchbase";
                } else if (i == 20) {
                    dbVendor = EDbVendor.dbvsnowflake;
                    dbName = "snowflake";
                } else if (i == 21) {
                    dbVendor = EDbVendor.dbvopenedge;
                    dbName = "openedge";
                } else if (i == 22) {
                    dbVendor = EDbVendor.dbvinformix;
                    dbName = "informix";
                } else if (i == 23) {
                    dbVendor = EDbVendor.dbvteradata;
                    dbName = "teradata";
                } else if (i == 24) {
                    dbVendor = EDbVendor.dbvmdx;
                    dbName = "mdx";
                } else if (i == 25) {
                    dbVendor = EDbVendor.dbvdb2;
                    dbName = "db2";
                }
                sqlparser = new TGSqlParser(dbVendor);
                sqlparser.sqltext = sqlQuery;
                ret = sqlparser.parse();
                if (ret == 0) {
//                    actualDBVendor = dbVendor.toString();
                  //  System.out.println(" This is suitable to : " + dbVendor);
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
              //  System.out.println(sqlparser.getErrormessage());
            }
        }
        if (ret == 0) {
            String xsdfile = "file:/C:/prg/gsp_java/library/doc/xml/sqlschema.xsd";
            xmlVisitor xv2 = new xmlVisitor(xsdfile);
            xv2.run(sqlparser);
            // xv2.validXml();
            //xv2.writeToFile( xmlFile );
          //  System.out.println("Xml File" + " was generated!");

        } else {
          //  System.out.println(sqlparser.getErrormessage());
        }
    }

    public static void getdbvendor(String dbvendorname) {
        List<EDbVendor> dbvendorlist = new LinkedList(Arrays.asList(EDbVendor.values()));

    }

}
