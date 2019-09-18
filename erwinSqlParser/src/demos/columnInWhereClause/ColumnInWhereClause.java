package demos.columnInWhereClause;

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;


public class ColumnInWhereClause
{

	public static void main(String[] args){
           String query = getquery();
		 TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvteradata);
//         sqlparser.sqltext = "Select firstname, lastname, age from Clients where State = \"CA\" and  City = \"Hollywood\"";
         sqlparser.sqltext = query;
         int i = sqlparser.parse( );
         if (i == 0)
         {
             TSelectSqlStatement stmt = (TSelectSqlStatement)sqlparser.sqlstatements.get( 0 );
             WhereCondition w = new WhereCondition(stmt.getWhereClause( ).getCondition( ));
             w.printColumn();
         }
         else
             System.out.println(sqlparser.getErrormessage( ));
	}
        
        public static String getquery(){
        
        
        
        
        return "locking table ${WORKDB}.gst_assoc_accts_TEMP for access\n" +
"locking table ${EDWDB}.gst_assoc_accts for access\n" +
"UPDATE ${EDWDB}.gst_assoc_accts\n" +
"FROM ${WORKDB}.gst_assoc_accts_temp TEMP\n" +
"SET i_primary_dmid = TEMP.i_primary_dmid\n" +
"WHERE ${EDWDB}.gst_assoc_accts.i_dmid = TEMP.i_dmid\n" +
"AND  ${EDWDB}.gst_assoc_accts.i_primary_dmid <> TEMP.I_PRIMARY_DMID\n" +
";";
        
        }
}
