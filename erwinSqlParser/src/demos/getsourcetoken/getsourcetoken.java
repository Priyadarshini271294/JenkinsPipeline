package demos.getsourcetoken;


import gudusoft.gsqlparser.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class getsourcetoken {
 public static void main(String args[])
 {
    long t = System.currentTimeMillis();

     if (args.length != 1){
         System.out.println("Usage: java getsourcetoken sqlfile.sql");
         return;
     }
     File file=new File(args[0]);
     if (!file.exists()){
         System.out.println("File not exists:"+args[0]);
         return;
     }

     EDbVendor dbVendor = EDbVendor.dbvoracle;
     String msg = "Please select SQL dialect: 1: SQL Server, 2: Oralce, 3: MySQL, 4: DB2, 5: PostGRESQL, 6: Teradta, default is 2: Oracle";
     System.out.println(msg);

     BufferedReader br=new   BufferedReader(new InputStreamReader(System.in));
     try{
         int db = Integer.parseInt(br.readLine());
         if (db == 1){
             dbVendor = EDbVendor.dbvmssql;
         }else if(db == 2){
             dbVendor = EDbVendor.dbvoracle;
         }else if(db == 3){
             dbVendor = EDbVendor.dbvmysql;
         }else if(db == 4){
             dbVendor = EDbVendor.dbvdb2;
         }else if(db == 5){
             dbVendor = EDbVendor.dbvpostgresql;
         }else if(db == 6){
             dbVendor = EDbVendor.dbvteradata;
         }
     }catch(IOException i) {
     }catch (NumberFormatException numberFormatException){
     }

     System.out.println("Selected SQL dialect: "+dbVendor.toString());

    TGSqlParser sqlparser = new TGSqlParser(dbVendor);

     sqlparser.sqlfilename  = args[0];


    int ret = sqlparser.parse();
    if (ret == 0){
        // get source tokens of whole script

        for(int i=0;i<sqlparser.sourcetokenlist.size();i++){
            TSourceToken st =  sqlparser.sourcetokenlist.get(i);
            System.out.println("token code:"+st.tokencode+" ,token type: "+st.tokentype.toString()+" ,text:"+st.toString());
        }


       // get source token of each statement

        for(int i=0; i<sqlparser.sqlstatements.size();i++){
            TCustomSqlStatement stmt = sqlparser.sqlstatements.get(i);
            System.out.println(stmt.sqlstatementtype.toString());
            for(int j=0;j<stmt.sourcetokenlist.size();j++){
                TSourceToken st =  stmt.sourcetokenlist.get(j);
                System.out.println("token code:"+st.tokencode+" ,token type: "+st.tokentype.toString()+" ,text:"+st.toString());
            }
        }
    }else{
        System.out.println(sqlparser.getErrormessage());
    }

    System.out.println("Time Escaped: "+ (System.currentTimeMillis() - t) );
}

}