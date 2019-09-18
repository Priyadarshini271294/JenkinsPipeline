package com.ads.sqlparser.v1;

import com.ads.api.beans.mm.MappingSpecificationRow;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class OverviewMapping {
       List<String> sourceList = new LinkedList<>();
    public static ArrayList<String> getOverviewMappingArray(ArrayList<MappingSpecificationRow> listOfRows){

        Set<String> tableSet = new HashSet<>();
        Set<String> sourceSet = new HashSet<>();
        Set<String> targetSet = new HashSet<>();

        ArrayList<String> SrcTgtArray = new ArrayList();
        ArrayList<String> srcArray = new ArrayList();
        ArrayList<String> tgtArray = new ArrayList();

        for(MappingSpecificationRow row : listOfRows){

            String[] srcTbl = row.getSourceTableName().split("\n");
            for(int j = 0; j<srcTbl.length; j++) {
                if(srcTbl[j].length()>0) tableSet.add(srcTbl[j]);
            }
            String[] tgtTbl = row.getTargetTableName().split("\n");
            for(int k = 0; k<tgtTbl.length; k++) {
                if(tgtTbl[k].length()>0) tableSet.add(tgtTbl[k]);
            }
        }
        if(!tableSet.isEmpty()){
          for (String tbl : tableSet) {
            int srcfound=0;
            int tgtfound=0;
            for(int j = 0; j < listOfRows.size(); j++) {
                for(String row : listOfRows.get(j).getTargetTableName().split("\n")) {
                if(row.equalsIgnoreCase(tbl)){
                    srcfound = 1;
                    break;
                }
                }
            }
            if(srcfound==0){
                sourceSet.add(tbl);
            }
            for(int j2 = 0; j2 < listOfRows.size(); j2++) {
                for(String row : listOfRows.get(j2).getSourceTableName().split("\n")) {
                    if(row.equalsIgnoreCase(tbl)){
                        tgtfound = 1;
                        break;
                    }
                }
            }
            if(tgtfound==0){
                targetSet.add(tbl);
            }
        }
        }

        for (String tbl : sourceSet) {
            srcArray.add(tbl);
        }
        for (String tbl : targetSet) {
            tgtArray.add(tbl);
        }
       // System.out.println("tgtArray: " + tgtArray);
       // System.out.println("srcArray: " + srcArray);
        for (String tgtTblName : targetSet) {
            for(int j1 = 0; j1 < listOfRows.size(); j1++) {
                if(listOfRows.get(j1).getTargetTableName().equalsIgnoreCase(tgtTblName)){
                    String srcTbl = listOfRows.get(j1).getSourceTableName();
                    String srcCol = listOfRows.get(j1).getSourceColumnName();
                    String tgtCol = listOfRows.get(j1).getTargetColumnName();
                    String br = listOfRows.get(j1).getBusinessRule() + " ";
                    String srcs = srcTbl + "@@" + srcCol+"@@"+br;
                    iterateRowsFromTarget(srcs, listOfRows, tgtTblName, tgtCol, srcArray, SrcTgtArray);
                }
            }
        }
        for (String srcTblName : sourceSet) {
            for(int j3 = 0; j3 < listOfRows.size(); j3++) {
                String[] srcTbl2 = listOfRows.get(j3).getSourceTableName().split("\n");
                String[] srcCol2 = listOfRows.get(j3).getSourceColumnName().split("\n");
                for(int j4 = 0; j4<srcTbl2.length; j4++) {
                    if (srcTbl2[j4].equalsIgnoreCase(srcTblName)) {
                        String tgtTbl = listOfRows.get(j3).getTargetTableName();
                        String tgtCol1 = listOfRows.get(j3).getTargetColumnName();
                        String srcCol1 = srcCol2[j4];
                        String br1 = listOfRows.get(j3).getBusinessRule() + " ";
                        String trgts = tgtTbl + "@@" + tgtCol1 + "@@" + br1;
                        iterateRowsFromSource(trgts, listOfRows, srcTblName, srcCol1, tgtArray, SrcTgtArray);
                    }
                }
            }
        }
        Set<String> SrcTgtSet = new HashSet<>();
        for (String tbl : SrcTgtArray) {
            SrcTgtSet.add(tbl);
        }
        ArrayList<String> SrcTgtArray2 = new ArrayList();
        for (String tbl : SrcTgtSet) {
            SrcTgtArray2.add(tbl);
        }
        return SrcTgtArray2;
    }
    public static ArrayList<MappingSpecificationRow> generateOverViewMappings(ArrayList<MappingSpecificationRow> listOfRows){

        ArrayList<String> SrcTgtDistinctArray = getOverviewMappingArray(listOfRows);
        ArrayList<MappingSpecificationRow> mapSpecRows = new ArrayList();

        for (String record : SrcTgtDistinctArray) {
            MappingSpecificationRow mapSpecRow = new MappingSpecificationRow();
            String[] parts = record.split("@@");
            String targetTable = parts[0];
            String targetCol = parts[1];
            String sourceTable = parts[2];
            String sourceCol = parts[3];
            String businessRules = parts[4];

            mapSpecRow.setSourceSystemId(1);
            mapSpecRow.setSourceSystemName("SYS");
            mapSpecRow.setSourceSystemEnvironmentName("SQL");
            mapSpecRow.setSourceTableName(sourceTable);
            mapSpecRow.setSourceColumnName(sourceCol);
            mapSpecRow.setTargetSystemId(1);
            mapSpecRow.setTargetSystemName("SYS");
            mapSpecRow.setTargetSystemEnvironmentName("SQL");
            mapSpecRow.setTargetTableName(targetTable);
            mapSpecRow.setTargetColumnName(targetCol);
            mapSpecRow.setBusinessRule(businessRules.trim());

            mapSpecRows.add(mapSpecRow);
        }
/*
        String mapname = getMapping(vMappingId).getMappingName();
        Mapping mapping = new Mapping();
        mapping.setMappingName(mapname);
        mapping.setMappingDescription(mapname+" map created Successfully");
        mapping.setProjectId(PROJECTID);
        mapping.setSubjectId(subjectId);
        mapping.setMappingSpecifications(mapSpecRows);
        String req = createMapping(mapping,true);
        return req;
*/
        return mapSpecRows;
    }
    public static void iterateRowsFromTarget(String srcs, ArrayList<MappingSpecificationRow> listOfRows, String tgtTblName, String tgtCol, ArrayList<String> srcArray, ArrayList<String> SrcTgtArray){
        String[] sList = srcs.split("!!");
        int counter = 0;
try{
        for(int j = 0; j<sList.length; j++){
            String src = sList[j];
            String[] srcTbl = src.split("@@")[0].split("\n");
            String[] srcCol = src.split("@@")[1].split("\n");
            String br = src.split("@@")[2];
            for(int k = 0; k<srcTbl.length; k++) {
                src = srcTbl[k] + "@@" + srcCol[k] + "@@" + br;
                String prevSrc = src;
                srcs = getSourceColumn(listOfRows, src);
                if ("".equals(srcs)) {
                    int flag = 0;
                    for (int i = 0; i < srcArray.size(); i++) {
                        String extSrc = srcArray.get(i);
                        if (prevSrc.split("@@")[0].equalsIgnoreCase(extSrc)) {
                            flag = 1;
                            break;
                        }
                    }
                    if (flag == 0) {
                        SrcTgtArray.add(tgtTblName + "@@" + tgtCol + "@@" + "CONSTANT_VALUE" + "@@" + prevSrc.split("@@")[1] + "@@" + prevSrc.split("@@")[2]);
                    } else {
                        SrcTgtArray.add(tgtTblName + "@@" + tgtCol + "@@" + prevSrc.split("@@")[0] + "@@" + prevSrc.split("@@")[1] + "@@" + prevSrc.split("@@")[2]);
                    }
                    break;
                } else {
                     iterateRowsFromTarget(srcs, listOfRows, tgtTblName, tgtCol, srcArray, SrcTgtArray);

                }
            }
            counter ++;
            if(counter == 5000){
                break;
            }
        }
}
catch(Exception e){
e.printStackTrace();
}

    }
    public  static void iterateRowsFromSource(String srcs, ArrayList<MappingSpecificationRow> listOfRows, String srcTblName, String srcCol, ArrayList<String> tgtArray, ArrayList<String> SrcTgtArray){
        String[] sList = srcs.split("!!");
        int counter = 0;
try{
        for(int j = 0; j<sList.length; j++){
            String src = sList[j];
            String prevSrc = src;
            srcs = getTargetColumn(listOfRows, src);
            if("".equals(srcs)){ 
                int flag = 0;
                for(int i = 0; i < tgtArray.size(); i++) {
                    String extTgt = tgtArray.get(i);
                    if(prevSrc.split("@@")[0].equalsIgnoreCase(extTgt)){
                        flag = 1;
                        break;
                    }
                }
                if(flag == 0){
                    SrcTgtArray.add("CONSTANT_VALUE"+"@@"+ prevSrc.split("@@")[1]+"@@"+srcTblName + "@@" + srcCol + "@@" + prevSrc.split("@@")[2]);
                }
                else{
                    SrcTgtArray.add(prevSrc.split("@@")[0] + "@@" + prevSrc.split("@@")[1]+"@@"+srcTblName + "@@" + srcCol + "@@" + prevSrc.split("@@")[2]);
                }
                break;
            }
            else{
                iterateRowsFromSource(srcs, listOfRows, srcTblName, srcCol, tgtArray, SrcTgtArray);
            }
            counter ++;
            if(counter == 5000){
                break;
            }
        }
}
catch(Exception e){
e.printStackTrace();
}

    }
    private static String getSourceColumn(ArrayList<MappingSpecificationRow> listOfRows, String src){
        String srcTableCols = "";

        for(int i = 0; i < listOfRows.size(); i++) {
            if(listOfRows.get(i).getTargetTableName().equalsIgnoreCase(src.split("@@")[0]) && listOfRows.get(i).getTargetColumnName().equalsIgnoreCase(src.split("@@")[1])){
                String br = " ";
                if(listOfRows.get(i).getBusinessRule().length() > 0){
                    if(src.split("@@")[2].length() > 0)
                        br = listOfRows.get(i).getBusinessRule()+"\n" + src.split("@@")[2];
                    else
                        br = listOfRows.get(i).getBusinessRule();
                }
                String srcTableCol = listOfRows.get(i).getSourceTableName()+"@@"+listOfRows.get(i).getSourceColumnName()+"@@" + br;
                if(srcTableCols.length() > 0)
                    srcTableCols += "!!" + srcTableCol;
                else
                    srcTableCols = srcTableCol;
            }
        }
        return srcTableCols;
    }
    private static String getTargetColumn(ArrayList<MappingSpecificationRow> listOfRows, String tgt){
        String tgtTableCols = "";

        for(int i = 0; i < listOfRows.size(); i++) {
            String[] srcTbl = listOfRows.get(i).getSourceTableName().split("\n");
            String[] srcCol = listOfRows.get(i).getSourceColumnName().split("\n");
            for(int j = 0; j<srcTbl.length; j++) {
                if(srcTbl[j].equalsIgnoreCase(tgt.split("@@")[0]) && srcCol[j].equalsIgnoreCase(tgt.split("@@")[1])){
                    String br = " ";
                    if(listOfRows.get(i).getBusinessRule().length() > 0){
                        if(tgt.split("@@")[2].length() > 0)
                            br = tgt.split("@@")[2] + "\n" + listOfRows.get(i).getBusinessRule();
                        else
                            br = listOfRows.get(i).getBusinessRule();
                    }
                    String tgtTableCol = listOfRows.get(i).getTargetTableName()+"@@"+listOfRows.get(i).getTargetColumnName()+"@@" + br;
                    if(tgtTableCols.length() > 0)
                        tgtTableCols += "!!" + tgtTableCol;
                    else
                        tgtTableCols = tgtTableCol;
                }
            }
        }
        return tgtTableCols;
    }
}
