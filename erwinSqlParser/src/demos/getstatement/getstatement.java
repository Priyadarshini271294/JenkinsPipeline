package demos.getstatement;

import gudusoft.gsqlparser.*;
import gudusoft.gsqlparser.stmt.TCreateIndexSqlStatement;
import gudusoft.gsqlparser.stmt.TCreateTableSqlStatement;
import gudusoft.gsqlparser.stmt.mssql.TMssqlCreateProcedure;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlCreateFunction;
import gudusoft.gsqlparser.stmt.oracle.TPlsqlCreateProcedure;
import gudusoft.gsqlparser.stmt.teradata.TTeradataLock;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class getstatement {

    private static EDbVendor dbVendor1 = EDbVendor.dbvoracle;
    private static TGSqlParser sqlparser1 = null;
    private static EDbVendor srcdbvender = null;

    public static EDbVendor dbss = null;

    public static void main(String args[]) {
        long t = System.currentTimeMillis();

        if (args.length != 1) {
            System.out.println("Usage: java getstatement sqlfile.sql");
            return;
        }
        File file = new File(args[0]);
        if (!file.exists()) {
            System.out.println("File not exists:" + args[0]);
            return;
        }

        EDbVendor dbVendor = EDbVendor.dbvoracle;
        String msg = "Please select SQL dialect: 1: SQL Server, 2: Oralce, 3: MySQL, 4: DB2, 5: PostGRESQL, 6: Teradta, default is 2: Oracle";
        System.out.println(msg);

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try {
            int db = Integer.parseInt(br.readLine());
            if (db == 1) {
                dbVendor = EDbVendor.dbvmssql;
            } else if (db == 2) {
                dbVendor = EDbVendor.dbvoracle;
            } else if (db == 3) {
                dbVendor = EDbVendor.dbvmysql;
            } else if (db == 4) {
                dbVendor = EDbVendor.dbvdb2;
            } else if (db == 5) {
                dbVendor = EDbVendor.dbvpostgresql;
            } else if (db == 6) {
                dbVendor = EDbVendor.dbvteradata;
            }
        } catch (IOException i) {
        } catch (NumberFormatException numberFormatException) {
        }

        System.out.println("Selected SQL dialect: " + dbVendor.toString());

        TGSqlParser sqlparser = new TGSqlParser(dbVendor);

        sqlparser.sqlfilename = args[0];

        int ret = sqlparser.getrawsqlstatements();
        if (ret == 0) {
            TSourceToken endToken, nextToken;

            for (int i = 0; i < sqlparser.sqlstatements.size(); i++) {
                System.out.println(sqlparser.sqlstatements.get(i).sqlstatementtype.toString());
                System.out.print(sqlparser.sqlstatements.get(i).toString());
                endToken = sqlparser.sqlstatements.get(i).getEndToken();
                for (int j = endToken.posinlist + 1; j < sqlparser.sourcetokenlist.size(); j++) {
                    nextToken = sqlparser.sourcetokenlist.get(j);
                    if ((nextToken.tokencode == TBaseType.cmtslashstar)
                            || (nextToken.tokencode == TBaseType.cmtdoublehyphen)
                            || (nextToken.tokencode == TBaseType.lexspace)
                            || (nextToken.tokencode == TBaseType.lexnewline)) {
                        System.out.print(nextToken.toString());
                    } else {
                        break;
                    }
                }
                System.out.println();
            }
        } else {
            System.out.println(sqlparser.getErrormessage());
        }

        System.out.println("Time Escaped: " + (System.currentTimeMillis() - t));
    }

       public static Set<String> getallstatement(String inputfile, String dbVender) {
        Set<String> storeprocfile = null;
        long t = System.currentTimeMillis();

        File file = new File(inputfile);

        EDbVendor dbVendor = null;
        List<EDbVendor> dbvenderlist = Arrays.asList(EDbVendor.values());
        for (EDbVendor vendor : dbvenderlist) {

            TGSqlParser sqlparser2 = new TGSqlParser(vendor);
            sqlparser2.sqlfilename = inputfile;
            if (sqlparser2.getrawsqlstatements() == 0) {
                dbVendor = vendor;
                break;

            }

        }


        TGSqlParser sqlparser = new TGSqlParser(dbVendor);

        sqlparser.sqlfilename = inputfile;

        int ret = sqlparser.getrawsqlstatements();
        if (ret == 0) {
            TSourceToken endToken, nextToken;
            storeprocfile = new LinkedHashSet();
            for (int i = 0; i < sqlparser.sqlstatements.size(); i++) {
                // System.out.println("test"+sqlparser.sqlstatements.get(i).sqlstatementtype.toString());
                //  System.out.print("test1"+sqlparser.sqlstatements.get(i).toString());
                // System.out.println("-------"+sqlparser.sqlstatements.get(i));
                sqlparser.sqlstatements.get(i).getClass().getName(); // This will give us Appropriate instance Type this will decide the execution of quries.
                System.out.println("statement"+sqlparser.sqlstatements.get(i).getClass().getName());
                if (sqlparser.sqlstatements.get(i) instanceof TMssqlCreateProcedure) {

                    // System.out.println("----createproc"+sqlparser.sqlstatements.get(i).toString());
                    storeprocfile.add(sqlparser.sqlstatements.get(i).toString());

                } else if (sqlparser.sqlstatements.get(i) instanceof TCreateTableSqlStatement) {
                    storeprocfile.add(sqlparser.sqlstatements.get(i).toString());

                } else if (sqlparser.sqlstatements.get(i) instanceof TTeradataLock) {

                    storeprocfile.add(sqlparser.sqlstatements.get(i).toString());
                } else if (sqlparser.sqlstatements.get(i) instanceof TPlsqlCreateFunction) {

                    storeprocfile.add(sqlparser.sqlstatements.get(i).toString());
                } else if (sqlparser.sqlstatements.get(i) instanceof TPlsqlCreateProcedure) {

                    storeprocfile.add(sqlparser.sqlstatements.get(i).toString());
                }else if (sqlparser.sqlstatements.get(i) instanceof TCreateTableSqlStatement) {
                       for (int s = 0; s < sqlparser.sqlstatements.get(i).getStatements().size(); i++) {
                           if(sqlparser.sqlstatements.get(i).getStatements().get(s) instanceof TCreateTableSqlStatement){
                           
                           storeprocfile.add(sqlparser.sqlstatements.get(i).getStatements().get(s).toString());
                           } 
                       }
                       storeprocfile.add(sqlparser.sqlstatements.get(i).toString());
                   
                } else if (sqlparser.sqlstatements.get(i) instanceof TCreateIndexSqlStatement) {
                       for (int s = 0; s < sqlparser.sqlstatements.get(i).getStatements().size(); i++) {
                           if(sqlparser.sqlstatements.get(i).getStatements().get(s) instanceof TCreateTableSqlStatement){
                           
                           storeprocfile.add(sqlparser.sqlstatements.get(i).getStatements().get(s).toString());
                           }
                       }
                    storeprocfile.add(sqlparser.sqlstatements.get(i).toString());
                }   else {
                    storeprocfile.add(sqlparser.sqlstatements.get(i).toString());
                    System.out.println("typessssss" + sqlparser.sqlstatements.get(i).getClass().getName());
                }
                // System.out.print("test1"+sqlparser.sqlstatements.get(i).getEndToken());
                endToken = sqlparser.sqlstatements.get(i).getEndToken();
                for (int j = endToken.posinlist + 1; j < sqlparser.sourcetokenlist.size(); j++) {
                    nextToken = sqlparser.sourcetokenlist.get(j);
                    if ((nextToken.tokencode == TBaseType.cmtslashstar)
                            || (nextToken.tokencode == TBaseType.cmtdoublehyphen)
                            || (nextToken.tokencode == TBaseType.lexspace)
                            || (nextToken.tokencode == TBaseType.lexnewline)) {
                        //  System.out.print("storeprocedureeeeee----------"+nextToken.toString());
                    } else {
                        break;
                    }
                }
                System.out.println();
            }
        } else {
            System.out.println("error----" + sqlparser.getErrormessage());

        }

        // System.out.println("Time Escaped: "+ (System.currentTimeMillis() - t) );
        return storeprocfile;
    }
       
        public static Set<String> getallStatementForQueryString(String inputfile, String dbVender) {
        Set<String> storeprocfile = null;
        long t = System.currentTimeMillis();

        File file = new File(inputfile);

        EDbVendor dbVendor = null;
        List<EDbVendor> dbvenderlist = Arrays.asList(EDbVendor.values());
        for (EDbVendor vendor : dbvenderlist) {

            TGSqlParser sqlparser2 = new TGSqlParser(vendor);
            sqlparser2.sqltext = inputfile;
            if (sqlparser2.getrawsqlstatements() == 0) {
                dbVendor = vendor;
                break;

            }

        }


        TGSqlParser sqlparser = new TGSqlParser(dbVendor);

        sqlparser.sqltext = inputfile;

        int ret = sqlparser.getrawsqlstatements();
        if (ret == 0) {
            TSourceToken endToken, nextToken;
            storeprocfile = new LinkedHashSet();
            for (int i = 0; i < sqlparser.sqlstatements.size(); i++) {
                // System.out.println("test"+sqlparser.sqlstatements.get(i).sqlstatementtype.toString());
                //  System.out.print("test1"+sqlparser.sqlstatements.get(i).toString());
                // System.out.println("-------"+sqlparser.sqlstatements.get(i));
                sqlparser.sqlstatements.get(i).getClass().getName(); // This will give us Appropriate instance Type this will decide the execution of quries.
                System.out.println("statement"+sqlparser.sqlstatements.get(i).getClass().getName());
                if (sqlparser.sqlstatements.get(i) instanceof TMssqlCreateProcedure) {

                    // System.out.println("----createproc"+sqlparser.sqlstatements.get(i).toString());
                    storeprocfile.add(sqlparser.sqlstatements.get(i).toString());

                } else if (sqlparser.sqlstatements.get(i) instanceof TCreateTableSqlStatement) {
                    storeprocfile.add(sqlparser.sqlstatements.get(i).toString());

                } else if (sqlparser.sqlstatements.get(i) instanceof TTeradataLock) {

                    storeprocfile.add(sqlparser.sqlstatements.get(i).toString());
                } else if (sqlparser.sqlstatements.get(i) instanceof TPlsqlCreateFunction) {

                    storeprocfile.add(sqlparser.sqlstatements.get(i).toString());
                } else if (sqlparser.sqlstatements.get(i) instanceof TPlsqlCreateProcedure) {

                    storeprocfile.add(sqlparser.sqlstatements.get(i).toString());
                }else if (sqlparser.sqlstatements.get(i) instanceof TCreateTableSqlStatement) {
                       for (int s = 0; s < sqlparser.sqlstatements.get(i).getStatements().size(); i++) {
                           if(sqlparser.sqlstatements.get(i).getStatements().get(s) instanceof TCreateTableSqlStatement){
                           
                           storeprocfile.add(sqlparser.sqlstatements.get(i).getStatements().get(s).toString());
                           } 
                       }
                       storeprocfile.add(sqlparser.sqlstatements.get(i).toString());
                   
                } else if (sqlparser.sqlstatements.get(i) instanceof TCreateIndexSqlStatement) {
                       for (int s = 0; s < sqlparser.sqlstatements.get(i).getStatements().size(); i++) {
                           if(sqlparser.sqlstatements.get(i).getStatements().get(s) instanceof TCreateTableSqlStatement){
                           
                           storeprocfile.add(sqlparser.sqlstatements.get(i).getStatements().get(s).toString());
                           }
                       }
                    storeprocfile.add(sqlparser.sqlstatements.get(i).toString());
                }   else {
                    storeprocfile.add(sqlparser.sqlstatements.get(i).toString());
                    System.out.println("typessssss" + sqlparser.sqlstatements.get(i).getClass().getName());
                }
                // System.out.print("test1"+sqlparser.sqlstatements.get(i).getEndToken());
                endToken = sqlparser.sqlstatements.get(i).getEndToken();
                for (int j = endToken.posinlist + 1; j < sqlparser.sourcetokenlist.size(); j++) {
                    nextToken = sqlparser.sourcetokenlist.get(j);
                    if ((nextToken.tokencode == TBaseType.cmtslashstar)
                            || (nextToken.tokencode == TBaseType.cmtdoublehyphen)
                            || (nextToken.tokencode == TBaseType.lexspace)
                            || (nextToken.tokencode == TBaseType.lexnewline)) {
                        //  System.out.print("storeprocedureeeeee----------"+nextToken.toString());
                    } else {
                        break;
                    }
                }
                System.out.println();
            }
        } else {
            System.out.println("error----" + sqlparser.getErrormessage());

        }

        // System.out.println("Time Escaped: "+ (System.currentTimeMillis() - t) );
        return storeprocfile;
    }
}
