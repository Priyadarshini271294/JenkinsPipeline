/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ads.sqlparser.v1;

import java.io.FileInputStream;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

/**
 *
 * @author Uma
 */
public class Ssisqueryextractor {

    public static void main(String[] args) {
        String query = getquery("C:\\Users\\Uma\\Desktop\\ssissql\\Pkg_LoadMemberData.dtsx");
    }
    
    
    
    public static String getquery(String inputpath) {
          String objectName = "";
          String query ="";
          
        try {
          
            FileInputStream file = new FileInputStream(inputpath);
            DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = builderFactory.newDocumentBuilder();
            Document xmlDocument = builder.parse(file);
            XPath xPath = XPathFactory.newInstance().newXPath();

            String controlFlowMissingCompExp = "//Executables/Executable";
//            NodeList controlFlowMissingCompNodeList = ((NodeList) xPath.compile(controlFlowMissingCompExp).evaluate(xmlDocument, XPathConstants.NODESET)).getLength();
            NodeList controlFlowMissingCompNodeList = (NodeList) xPath.compile(controlFlowMissingCompExp).evaluate(xmlDocument, XPathConstants.NODESET);
            for (int j = 0; j < controlFlowMissingCompNodeList.getLength(); j++) {
                if ("DTS:Executable".equalsIgnoreCase(controlFlowMissingCompNodeList.item(j).getNodeName())) {
                    String objectRefId = controlFlowMissingCompNodeList.item(j).getAttributes().getNamedItem("DTS:refId").getTextContent();
                    String componentType = controlFlowMissingCompNodeList.item(j).getAttributes().getNamedItem("DTS:CreationName").getTextContent();

                    if ("STOCK:FOREACHLOOP".equals(componentType)) {
                        NodeList executablesList = controlFlowMissingCompNodeList.item(j).getChildNodes();
                        for (int k = 0; k < executablesList.getLength(); k++) {
                            if ("DTS:Executables".equalsIgnoreCase(executablesList.item(k).getNodeName())) {
                                NodeList executableList = executablesList.item(k).getChildNodes();
                                for (int l = 0; l < executableList.getLength(); l++) {
                                    if ("DTS:Executable".equalsIgnoreCase(executableList.item(l).getNodeName())) {
                                        objectName = executableList.item(l).getAttributes().getNamedItem("DTS:ObjectName").getTextContent();

                                    }
                                }
                            }
                        }

                    } else if ("Microsoft.ExecuteSQLTask".equals(componentType)) {
                        NodeList executablesList = controlFlowMissingCompNodeList.item(j).getChildNodes();
                        for (int k = 0; k < executablesList.getLength(); k++) {
                            if ("DTS:ObjectData".equalsIgnoreCase(executablesList.item(k).getNodeName())) {
                                NodeList executableList = executablesList.item(k).getChildNodes();
                                for (int l = 0; l < executableList.getLength(); l++) {
                                    if ("SQLTask:SqlTaskData".equalsIgnoreCase(executableList.item(l).getNodeName())) {
                                         query = executableList.item(l).getAttributes().getNamedItem("SQLTask:SqlStatementSource").getTextContent();
                                        objectName = controlFlowMissingCompNodeList.item(j).getAttributes().getNamedItem("DTS:ObjectName").getTextContent();
                                        
                                        
                                    }
                                }
                            }
                        }

                    }
                }
            }
        } catch (Exception e) {

        }
        query = query.replace("[", "").replace("]", "");
        System.out.println("queryyyy----- "+query);
         return query; 
    }

}
