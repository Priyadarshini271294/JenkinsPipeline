package demos.formatsql;

/*
 * Date: 2010-11-9
 * Time: 9:38:43
 */

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.pp.para.GFmtOpt;
import gudusoft.gsqlparser.pp.para.GFmtOptFactory;
import gudusoft.gsqlparser.pp.stmtformatter.FormatterFactory;
import java.io.File;

public class formatsql {
    
    public static void main(String[] args) {
       
    }

    public static String formatSqlforsqltext(String formatsql) {

       
       

        TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvmssql);
        sqlparser.sqltext = formatsql;

//        sqlparser.sqltext = "insert into emp(empno,empnm,deptnm,sal) select empno, empnm, dptnm, sal from emp where empno=:empno;\n" +
//                "\n" +
//                "select empno, empnm from (select empno, empnm from emp)";
//         TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvpostgresql);
//         sqlparser.sqltext ="WITH upd AS (\n" +
//                 "  UPDATE employees SET sales_count = sales_count + 1 WHERE id =\n" +
//                 "    (SELECT sales_person FROM accounts WHERE name = 'Acme Corporation')\n" +
//                 "    RETURNING *\n" +
//                 ")\n" +
//                 "INSERT INTO employees_log SELECT *, current_timestamp FROM upd;";
        int ret = sqlparser.parse();
        if (ret == 0) {
            GFmtOpt option = GFmtOptFactory.newInstance();
            // option.wsPaddingParenthesesInExpression = false;
            //option.selectColumnlistComma =     TLinefeedsCommaOption.LfBeforeComma;
            // umcomment next line generate formatted sql in html
            //option.outputFmt =  GOutputFmt.ofhtml;
            // option.removeComment = true;
            String result = FormatterFactory.pp(sqlparser, option);
            return result;
//            System.out.println(result);
        } else {
            System.out.println(sqlparser.getErrormessage());
        }
        return "";
    }

    public static String getSqlformatter(String query, String dbVender) {
          String result ="";
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
        
         TGSqlParser sqlparser = new TGSqlParser(dbVendor);
        sqlparser.sqlfilename = query;
        
        int ret = sqlparser.parse();
        if (ret == 0) {
            GFmtOpt option = GFmtOptFactory.newInstance();
            // option.wsPaddingParenthesesInExpression = false;
            //option.selectColumnlistComma =     TLinefeedsCommaOption.LfBeforeComma;
            // umcomment next line generate formatted sql in html
            //option.outputFmt =  GOutputFmt.ofhtml;
            // option.removeComment = true;
            result = FormatterFactory.pp(sqlparser, option);
            System.out.println(result);
        } else {
            System.out.println(sqlparser.getErrormessage());
        }
        
      return  result;
    }
}
