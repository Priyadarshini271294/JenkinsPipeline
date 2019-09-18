/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package goldparser;

import com.creativewidgetworks.goldparser.engine.ParserException;
import com.creativewidgetworks.goldparser.engine.Reduction;
import com.creativewidgetworks.goldparser.engine.Token;
import com.creativewidgetworks.goldparser.parser.GOLDParser;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 *
 * @author sashikant D
 */
public class Goldparser {

    private Document xmldoc = null;
    private Element e_sqlScript = null;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            Goldparser parser = new Goldparser();
           String source = "SUM (\n" +
"            CASE\n" +
"               WHEN ras.v_arf_113_asset_class IN (irojectFinance,objectFinance,incomeProducingRealestate,commodityFinance)\n" +
"                    AND DBM.V_BASEL_METHOD_CODE = 'NSAIRB' "
                   + "AND RAS.V_ACCT_FNCL_INST_CODE = 'BNZ'\n" +
"                   \n" +
"               THEN\n" +
"                  RAS.N_RWA_UL_AMT\n" +
"               ELSE\n" +
"                  0\n" +
"            END)";
            String sourcee = "SUM (\n"
                        + "            CASE\n"
                        + "               WHEN  IN (ProjectFinance,ObjectFinance,IncomeProducingRealestate,CommodityFinance)\n"
                        + "            AND DBM.V_BASEL_METHOD_CODE = 'NSAIRB' AND RAS.V_ACCT_FNCL_INST_CODE = 'BNZ'"
                        + "                   \n"
                        + "               THEN\n"
                        + "                  RAS.N_RWA_UL_AMT\n"
                        + "               ELSE\n"
                        + "                  0\n"
                        + "            END)";
            String source1 = "SUM (\n"
                        + "            CASE\n"
                        + "               WHEN SUM (\n"
                        + "            CASE\n"
                        + "               WHEN ras.v_arf_113_asset_class IN (ProjectFinance,ObjectFinance,IncomeProducingRealestate,CommodityFinance)\n"
                        + "                    AND DBM.V_BASEL_METHOD_CODE = 'NSAIRB'"
                        + " AND RAS.V_ACCT_FNCL_INST_CODE = 'BNZ'               "
                        + "THEN RAS.N_RWA_UL_AMT\n"
                        + "               ELSE\n"
                        + "                  0\n"
                        + "            END) IN (ProjectFinance,ObjectFinance,IncomeProducingRealestate,CommodityFinance)\n"
                        + "                    AND DBM.V_BASEL_METHOD_CODE = 'NSAIRB'"
                        + " AND RAS.V_ACCT_FNCL_INST_CODE = 'BNZ'               "
                        + "THEN RAS.N_RWA_UL_AMT\n"
                        + "               ELSE\n"
                        + "                  0\n"
                        + "            END)";

            String source2 = "  CASE\n"
                        + "                     WHEN DC.V_COUNTERPARTY_TYPE = 'QCCP'\n"
                        + "                          AND RAS.N_CCP_DF_RANK = '3'\n"
                        + "                          AND (DBPT.V_BASEL_PROD_TYPE_CODE_LEVEL1 IN\n"
                        + "                                  ('OTD', 'SFT')\n"
                        + "                               OR DBPT.V_BASEL_PROD_TYPE_CODE IN ('MADEAC', 'MABUAC', 'MVNAAC', 'MINAAC'))\n"
                        + "                     THEN\n"
                        + "                        RAS.N_EAD_POST_MITIGATION\n"
                        + "                     ELSE\n"
                        + "                        0\n"
                        + "                  END";
              String source66 = "NVL(O_COMP_1,0)-NVL(N_COMP_1,0)";
            //  String source = "SQRT(NVL(O_COMP_3,0)+NVL(N_COMP_3,0))";
            //   String source = "CASE state WHEN statement THEN 0 ELSE rev_amt_mth statement END";
            //  source = source.replaceAll("\\s+", " ");
            
            boolean wantTree = true;
            String tree = parser.executeProgram(source66, wantTree,"C:\\Users\\ShashikantaDandasena\\Downloads\\grammerBr.egt");
            System.out.println("======" + tree);
            String valuestring = getXmlTree(tree);
            List<Map<String, List<String>>> maplist = tablecolumnMap(valuestring.toUpperCase());

            System.out.println("----" + maplist);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static List<Map<String, List<String>>> sourceTableMap(String BusinessRule,String egtfilepath) {
        List<Map<String, List<String>>> maplist = null;
        try {

            Goldparser parser = new Goldparser();
            boolean wantTree = true;
            String tree = parser.executeProgram(BusinessRule, wantTree,egtfilepath);
           // System.out.println("======" + tree);
            String valuestring = getXmlTree(tree);
            maplist = tablecolumnMap(valuestring.toUpperCase());

        } catch (Exception e) {
        }

        return maplist;

    }

    public String executeProgram(String sourceCode, boolean wantTree,String egtfilepath) throws IOException {
        
        GOLDParser parser = new GOLDParser(new FileInputStream(egtfilepath), // compiled grammar table
                    "DS2PC", // rule handler package (fully qualified package)
                    true);
        // trim reductions

        // Controls whether or not a parse tree is returned or the program executed.
        parser.setGenerateTree(wantTree);

        String tree = null;
        try {
            // Parse the source statements to see if it is syntactically correct
            boolean parsedWithoutError = parser.parseSourceStatements(sourceCode);

            // Holds the parse tree if setGenerateTree(true) was called
            tree = parser.getParseTree();

            // Either execute the code or print any error message
            if (parsedWithoutError) {
                //parser.getCurrentReduction().execute();
                Reduction reduction = parser.getCurrentReduction();
                reduction.execute();

                for (Token token : reduction) {
                }
            } else {
                System.out.println(parser.getErrorMessage());
            }
        } catch (ParserException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

        return tree;
    }

    public static String getXmlTree(String tree) {
        int i = 0;
        StringBuilder sb = new StringBuilder();
        String[] data = tree.split("\n");
        for (String valuedata : data) {

            if (valuedata.contains("<Value> ::= Id")) {
                String vdata = data[i + 1];
                if (vdata.contains(",")) {
                    vdata = "";
                }
                sb.append(vdata + "\n");

            }
            i++;
        }
        String values = sb.toString().replaceAll("(?m)^[ \t]*\r?\n", "").replaceAll("\\|", "").replace("+", "").replace("-", "");
        // System.out.println(values);
        return values;
    }

    public static List<Map<String, List<String>>> tablecolumnMap(String tree) {
        Map<String, String> tablecolumnMap = new LinkedHashMap<>();
        Map<String, List<String>> tabcollist = new LinkedHashMap<>();
        List<Map<String, List<String>>> tableColumnmapList = new LinkedList<>();
        String[] tablecolumndata = tree.split("\n");
        for (String tabcol : tablecolumndata) {
            String tableName = tabcol.split("\\.")[0].trim();
            String columnName = tabcol.split("\\.")[1].trim();

            if (tablecolumnMap.get(tableName) == null) {
                tablecolumnMap.put(tableName, columnName);
            } else {
                String value = tablecolumnMap.get(tableName);
                tablecolumnMap.put(tableName, value + "~" + columnName);
            }

        }
        for (Map.Entry<String, String> entrySet : tablecolumnMap.entrySet()) {
            String key = entrySet.getKey();
            String value = entrySet.getValue();
            List<String> valueslist = null;
            if (value.contains("~")) {
                String[] values = value.split("~");
                valueslist = Arrays.asList(values);
                tabcollist.put(key, valueslist);
            } else {
                valueslist.add(value);
                tabcollist.put(key, valueslist);
            }
        }
        tableColumnmapList.add(tabcollist);

        return tableColumnmapList;
    }

}
