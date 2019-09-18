package demos.traceColumn;


import gudusoft.gsqlparser.EDbVendor;

public class runTraceColumn {

    public static void main(String args[]) {
//        String sqltext = "SELECT a.deptno \"Department\", \n" +
//                "       a.num_emp/b.total_count \"Employees\", \n" +
//                "       a.sal_sum/b.total_sal \"Salary\"\n" +
//                "  FROM\n" +
//                "(SELECT deptno, COUNT(*) num_emp, SUM(SAL) sal_sum\n" +
//                "    FROM scott.emp\n" +
//                "    GROUP BY deptno) a,\n" +
//                "(SELECT COUNT(*) total_count, SUM(sal) total_sal\n" +
//                "    FROM scott.emp) b";

//        String sqltext = "create or replace view test\n" +
//                "(col1,col2)\n" +
//                "as \n" +
//                "select a, (select b from table2 where table2.c=table1.a) from table1\n" +
//                "union \n" +
//                "select d, e-g from table2";

            String sqltext = "SELECT a.Colum1, a.column2, b.column3+a.column4 as column3, c.col\n" +
                    "from table1 a join table2 b on a.id=b.id\n" +
                    "join table3 c on b.tid=c.id";
        TTraceColumn traceColumn = new TTraceColumn(EDbVendor.dbvoracle);
        traceColumn.runText(sqltext);
        System.out.print(traceColumn.getInfos().toString());
    }

}
