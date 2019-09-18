/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ads.sqlparser.v1;

import demos.getstatement.getstatement;
import java.io.*;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import java.util.*;

/**
 *
 * @author sashikant D
 */
public class StroreProcedureParserV1 {

    public static void main(String[] args) {
        // List<String> storeproclist=   getstatement.getallstatement("C:\\Users\\sashikant D\\Desktop\\teststoreprocedure.sql","mssql");
        //storeproclist.forEach(System.out::println);
        try{
            parseStoreprocintomultiplefile("E:\\sqlfiles", "sqlserver","C:\\MappingManager\\CATFX\\CATS\\3080\\Files\\Timestamp");
        }catch(Exception e){
            e.printStackTrace();

        }finally{
            StroreProcedureParserV1.updateCATTimeStamp("C:\\MappingManager\\CATFX\\CATS\\3080\\Files\\Timestamp");
        }
    }
    public static String getTimeStamp(String timeStampPropertiesDir) {
        Map<String, String> timeStampMap = new LinkedHashMap();
        File dirPath = null;
        try {
            dirPath = new File(timeStampPropertiesDir);
            File[] directoryListing = dirPath.listFiles();
            if (directoryListing == null) {
                return "";
            } else {
                for (int i = 0; i < directoryListing.length; i++) {
                    File propertyFilePath = directoryListing[i];
                    Properties dbProperties = new Properties();
                    FileInputStream fileInput = new FileInputStream(propertyFilePath);
                    dbProperties.load(fileInput);
                    fileInput.close();
                    Enumeration enuKeys = dbProperties.keys();
                    String key = "";
                    String value = "";
                    while (enuKeys.hasMoreElements()) {
                        key = (String) enuKeys.nextElement();
                        value = (String) dbProperties.getProperty(key);
                        timeStampMap.put(key, value);
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return timeStampMap.get("lastCATrun");
    }

    public static void parseStoreprocintomultiplefile(String storeProcFilePath, String dbVender,String catTimeStampPath) {
        Set<String> storeproclist = null;
        String inputFileName = "";
        try {
            String lastRun=getTimeStamp(catTimeStampPath);
            File storeProcFile = new File(storeProcFilePath);
            if (storeProcFile.isDirectory()) {
                if(storeProcFile.list().length > 0){
                //getFilesFromDirectories(storeProcFilePath);
                File[] listofstoreprocFiles = storeProcFile.listFiles();

                File outputDirectory = new File(storeProcFile.getAbsolutePath() + File.separator + "output");
                //if (outputDirectory.exists()) {
                    //FileUtils.deleteDirectory(outputDirectory);
                   
                //}
                for (File listofstoreprocFile : listofstoreprocFiles) {
                    if (!listofstoreprocFile.isDirectory()) {
                        String fileextension = FilenameUtils.getExtension(listofstoreprocFile.getName());

                        inputFileName = listofstoreprocFile.getName();
                        storeproclist = getstatement.getallstatement(listofstoreprocFile.getAbsolutePath(), dbVender);
                        //System.out.println(storeproclist);
                        int i = 0;
                        for (String storeproc : storeproclist) {
                            File sqlfilepath = new File(listofstoreprocFile.getAbsolutePath());
                            String parentpath = sqlfilepath.getAbsoluteFile().getParent();
                            File sqlfile = new File(parentpath + "/output/" + inputFileName.replace(".sql", "") + i + ".sql");

                            if (!"".equals(lastRun) && sqlfilepath.lastModified() < Long.parseLong(lastRun)) {
                                continue;
                            }
                          String modifiedsql =  withClause(storeproc);
                            FileUtils.writeStringToFile(sqlfile, modifiedsql);
                            i++;
                        }
//            File deletefile = new File(listofstoreprocFile.getAbsolutePath());
//            FileUtils.forceDelete(deletefile);
                    } else {
                        if (!listofstoreprocFile.getAbsolutePath().contains("output"))
                            parseStoreprocintomultiplefile(listofstoreprocFile.getAbsolutePath(), dbVender,catTimeStampPath);
                    }
                }
            }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        //return new LinkedList(storeproclist);
    }
    public static void updateCATTimeStamp(String timeStampPropertiesDir) {
        try {
            File dirPath = new File(timeStampPropertiesDir);
            File[] directoryListing = dirPath.listFiles();
            if (directoryListing == null) {
            } else {
                for (int i = 0; i < directoryListing.length; i++) {
                    File propertyFilePath = directoryListing[i];
                    Properties dbProperties = new Properties();
                    FileInputStream fileInput = new FileInputStream(propertyFilePath);
                    dbProperties.load(fileInput);
                    dbProperties.setProperty("lastCATrun", System.currentTimeMillis() + "");
                    FileOutputStream fos = new FileOutputStream(new File(timeStampPropertiesDir + "\\catTimeStamp.properties"));
                    dbProperties.store(fos, "");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
     public static String withClause(String filecontent) {
        StringBuffer strbf = new StringBuffer();
        try {
            

            String[] filearr = filecontent.split("\n");
            for (String withfile : filearr) {
                if (withfile.trim().contains("with (distribution=hash(AccountID), clustered columnstore index)")
                        ||withfile.trim().contains("with (distribution = hash(AccountID), clustered columnstore index)")
                        ||withfile.trim().contains("rename object")) {
                    continue;
                }
                strbf.append(withfile + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return strbf.toString();
    }
}
