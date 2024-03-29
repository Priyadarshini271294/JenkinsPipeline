package demos.dlineage;

import demos.dlineage.dataflow.listener.DataFlowHandleListener;
import demos.dlineage.dataflow.model.AbstractRelation;
import demos.dlineage.dataflow.model.DataFlowRelation;
import demos.dlineage.dataflow.model.ImpactRelation;
import demos.dlineage.dataflow.model.ModelBindingManager;
import demos.dlineage.dataflow.model.ModelFactory;
import demos.dlineage.dataflow.model.QueryTable;
import demos.dlineage.dataflow.model.QueryTableRelationElement;
import demos.dlineage.dataflow.model.RecordSetRelation;
import demos.dlineage.dataflow.model.Relation;
import demos.dlineage.dataflow.model.RelationElement;
import demos.dlineage.dataflow.model.RelationType;
import demos.dlineage.dataflow.model.ResultColumn;
import demos.dlineage.dataflow.model.ResultColumnRelationElement;
import demos.dlineage.dataflow.model.ResultSet;
import demos.dlineage.dataflow.model.SelectResultSet;
import demos.dlineage.dataflow.model.SelectSetResultSet;
import demos.dlineage.dataflow.model.Table;
import demos.dlineage.dataflow.model.TableColumn;
import demos.dlineage.dataflow.model.TableColumnRelationElement;
import demos.dlineage.dataflow.model.TableRelationElement;
import demos.dlineage.dataflow.model.View;
import demos.dlineage.dataflow.model.ViewColumn;
import demos.dlineage.dataflow.model.ViewColumnRelationElement;
import demos.dlineage.dataflow.model.xml.dataflow;
import demos.dlineage.dataflow.model.xml.relation;
import demos.dlineage.dataflow.model.xml.sourceColumn;
import demos.dlineage.dataflow.model.xml.table;
import demos.dlineage.dataflow.model.xml.targetColumn;
import demos.dlineage.util.Pair;
import demos.dlineage.util.SQLUtil;
import demos.dlineage.util.XML2Model;
import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.EExpressionType;
import gudusoft.gsqlparser.ESetOperatorType;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.nodes.IExpressionVisitor;
import gudusoft.gsqlparser.nodes.TAliasClause;
import gudusoft.gsqlparser.nodes.TCTE;
import gudusoft.gsqlparser.nodes.TColumnDefinition;
import gudusoft.gsqlparser.nodes.TConstant;
import gudusoft.gsqlparser.nodes.TExpression;
import gudusoft.gsqlparser.nodes.TExpressionList;
import gudusoft.gsqlparser.nodes.TFunctionCall;
import gudusoft.gsqlparser.nodes.TGroupByItem;
import gudusoft.gsqlparser.nodes.TGroupByItemList;
import gudusoft.gsqlparser.nodes.TJoin;
import gudusoft.gsqlparser.nodes.TJoinItem;
import gudusoft.gsqlparser.nodes.TMergeInsertClause;
import gudusoft.gsqlparser.nodes.TMergeUpdateClause;
import gudusoft.gsqlparser.nodes.TMergeWhenClause;
import gudusoft.gsqlparser.nodes.TObjectName;
import gudusoft.gsqlparser.nodes.TObjectNameList;
import gudusoft.gsqlparser.nodes.TParseTreeNode;
import gudusoft.gsqlparser.nodes.TResultColumn;
import gudusoft.gsqlparser.nodes.TResultColumnList;
import gudusoft.gsqlparser.nodes.TTable;
import gudusoft.gsqlparser.nodes.TTableList;
import gudusoft.gsqlparser.nodes.TViewAliasItemList;
import gudusoft.gsqlparser.stmt.TCreateTableSqlStatement;
import gudusoft.gsqlparser.stmt.TCreateViewSqlStatement;
import gudusoft.gsqlparser.stmt.TInsertSqlStatement;
import gudusoft.gsqlparser.stmt.TMergeSqlStatement;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;
import gudusoft.gsqlparser.stmt.TUpdateSqlStatement;
import gudusoft.gsqlparser.util.functionChecker;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class DataFlowAnalyzer {

    public static List<String> xmlfiles = new LinkedList<>();
    private static final List<String> TERADATA_BUILTIN_FUNCTIONS = Arrays.asList(new String[]{
        "ACCOUNT",
        "CURRENT_DATE",
        "CURRENT_ROLE",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "CURRENT_USER",
        "DATABASE",
        "DATE",
        "PROFILE",
        "ROLE",
        "SESSION",
        "TIME",
        "USER",
        "SYSDATE",});

    private Stack<TCustomSqlStatement> stmtStack = new Stack<TCustomSqlStatement>();
    private List<ResultSet> appendResultSets = new ArrayList<ResultSet>();
    private List<TCustomSqlStatement> accessedStatements = new ArrayList<TCustomSqlStatement>();

    private File[] sqlFiles;
    private File sqlFile;
    private String sqlContent;
    private String[] sqlContents;
    private EDbVendor vendor;
    private String dataflowString;
    private dataflow dataflowResult;
    private DataFlowHandleListener handleListener;
    private boolean simpleOutput;
    private boolean textFormat = false;

    public DataFlowAnalyzer(String sqlContent, EDbVendor dbVendor,
                boolean simpleOutput) {
        this.sqlContent = sqlContent;
        this.vendor = dbVendor;
        this.simpleOutput = simpleOutput;
    }

    public DataFlowAnalyzer(String[] sqlContents, EDbVendor dbVendor,
                boolean simpleOutput) {
        this.sqlContents = sqlContents;
        this.vendor = dbVendor;
        this.simpleOutput = simpleOutput;
    }

    public DataFlowAnalyzer(File[] sqlFiles, EDbVendor dbVendor,
                boolean simpleOutput) {
        this.sqlFiles = sqlFiles;
        this.vendor = dbVendor;
        this.simpleOutput = simpleOutput;
    }

    public DataFlowAnalyzer(File sqlFile, EDbVendor dbVendor,
                boolean simpleOutput) {
        this.sqlFile = sqlFile;
        this.vendor = dbVendor;
        this.simpleOutput = simpleOutput;
    }

    public void setHandleListener(DataFlowHandleListener listener) {
        this.handleListener = listener;
    }

    public synchronized String generateDataFlow(StringBuffer errorMessage) {
        PrintStream pw = null;
        ByteArrayOutputStream sw = null;
        PrintStream systemSteam = System.err;;

        sw = new ByteArrayOutputStream();
        pw = new PrintStream(sw);
        System.setErr(pw);

        dataflowString = analyzeSqlScript();

        if (handleListener != null) {
            handleListener.endOutputDataFlowXML(dataflowString == null ? 0
                        : dataflowString.length());
            handleListener.endAnalyze();
        }

        if (pw != null) {
            pw.close();
        }

        System.setErr(systemSteam);

        if (sw != null) {
            if (errorMessage != null) {
                errorMessage.append(sw.toString().trim());
            }
        }

        return dataflowString;
    }

    public synchronized dataflow getDataFlow() {
        if (dataflowResult != null) {
            return dataflowResult;
        } else if (dataflowString != null) {
            dataflowResult = XML2Model.loadXML(dataflow.class, dataflowString);
            return dataflowResult;
        }
        return null;
    }

    private File[] listFiles(File sqlFiles) {
        List<File> children = new ArrayList<File>();
        if (sqlFiles != null) {
            listFiles(sqlFiles, children);
        }
        return children.toArray(new File[0]);
    }

    private void listFiles(File rootFile, List<File> children) {
        if (handleListener != null && handleListener.isCanceled()) {
            return;
        }

        if (rootFile.isFile()) {
            children.add(rootFile);
        } else {
            File[] files = rootFile.listFiles();
            if (files != null) {
                for (int i = 0; i < files.length; i++) {
                    listFiles(files[i], children);
                }
            }
        }
    }

    private synchronized String analyzeSqlScript() {
        init();

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.newDocument();
            doc.setXmlVersion("1.0");
            Element dlineageResult = doc.createElement("dlineage");

            if (sqlFile != null) {
                File[] children = listFiles(sqlFile);

                if (handleListener != null) {
                    if (sqlFile.isDirectory()) {
                        handleListener.startAnalyze(sqlFile,
                                    children.length,
                                    true);
                    } else {
                        handleListener.startAnalyze(sqlFile,
                                    sqlFile.length(),
                                    false);
                    }
                }

                for (int i = 0; i < children.length; i++) {
                    if (handleListener != null && handleListener.isCanceled()) {
                        break;
                    }

                    String content = SQLUtil.getFileContent(children[i].getAbsolutePath());
                    if (handleListener != null) {
                        handleListener.startParse(children[i],
                                    content.length(),
                                    i);
                    }

                    TGSqlParser sqlparser = new TGSqlParser(vendor);
                    sqlparser.sqltext = content.toUpperCase();
                    analyzeAndOutputResult(sqlparser, doc, dlineageResult);
                }
            } else if (sqlContent != null) {
                if (handleListener != null) {
                    handleListener.startAnalyze(null,
                                sqlContent.length(),
                                false);
                }

                if (handleListener != null) {
                    handleListener.startParse(null, sqlContent.length(), 0);
                }

                TGSqlParser sqlparser = new TGSqlParser(vendor);
                sqlparser.sqltext = sqlContent.toUpperCase();
                analyzeAndOutputResult(sqlparser, doc, dlineageResult);
            } else if (sqlContents != null) {
                if (handleListener != null) {
                    if (sqlContents.length == 1) {
                        handleListener.startAnalyze(null,
                                    sqlContents[0].length(),
                                    false);
                    } else {
                        handleListener.startAnalyze(null,
                                    sqlContents.length,
                                    true);
                    }
                }

                for (int i = 0; i < sqlContents.length; i++) {
                    if (handleListener != null && handleListener.isCanceled()) {
                        break;
                    }

                    String content = sqlContents[i];

                    if (handleListener != null) {
                        handleListener.startParse(null, content.length(), 0);
                    }

                    TGSqlParser sqlparser = new TGSqlParser(vendor);
                    sqlparser.sqltext = content.toUpperCase();
                    analyzeAndOutputResult(sqlparser, doc, dlineageResult);
                }
            } else if (sqlFiles != null) {
                if (handleListener != null) {
                    if (sqlFiles.length == 1) {
                        handleListener.startAnalyze(sqlFiles[0],
                                    sqlFiles[0].length(),
                                    false);
                    } else {
                        handleListener.startAnalyze(null,
                                    sqlFiles.length,
                                    true);
                    }
                }

                File[] children = sqlFiles;
                for (int i = 0; i < children.length; i++) {
                    if (handleListener != null && handleListener.isCanceled()) {
                        break;
                    }

                    String content = SQLUtil.getFileContent(children[i].getAbsolutePath());

                    if (handleListener != null) {
                        handleListener.startParse(children[i],
                                    content.length(),
                                    i);
                    }

                    TGSqlParser sqlparser = new TGSqlParser(vendor);
                    sqlparser.sqltext = content.toUpperCase();
                    analyzeAndOutputResult(sqlparser, doc, dlineageResult);
                }
            }

            doc.appendChild(dlineageResult);

            if (handleListener != null) {
                handleListener.endAnalyze();
                handleListener.startOutputDataFlowXML();
            }

            StringWriter sw = new StringWriter();
            com.sun.org.apache.xml.internal.serialize.OutputFormat format = new com.sun.org.apache.xml.internal.serialize.OutputFormat(doc);
            format.setIndenting(true);
            format.setIndent(2);
            format.setLineWidth(0);
            Writer output = new BufferedWriter(sw);
            com.sun.org.apache.xml.internal.serialize.XMLSerializer serializer = new com.sun.org.apache.xml.internal.serialize.XMLSerializer(output,
                        format);
            serializer.serialize(doc);

            String xml = sw.toString();

            if (simpleOutput) {
                dataflow dataflowInstance = XML2Model.loadXML(dataflow.class,
                            xml);
                dataflow simpleDataflow = getSimpleDataflow(dataflowInstance);
                if (textFormat) {
                    xml = getTextOutput(simpleDataflow);
                } else {
                    xml = XML2Model.saveXML(simpleDataflow);
                    xml = xml.replace(" isTarget=\"true\"", "");
                }
            }

            return xml;

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    private String getTextOutput(dataflow dataflow) {
        StringBuffer buffer = new StringBuffer();
        List<relation> relations = dataflow.getRelations();
        if (relations != null) {
            for (int i = 0; i < relations.size(); i++) {
                relation relation = relations.get(i);
                targetColumn target = relation.getTarget();
                List<sourceColumn> sources = relation.getSources();
                if (target != null && sources != null && sources.size() > 0) {
                    buffer.append(target.getColumn())
                                .append(" depends on: ");
                    Set<String> columnSet = new LinkedHashSet<String>();
                    for (int j = 0; j < sources.size(); j++) {
                        sourceColumn sourceColumn = sources.get(j);
                        String columnName = sourceColumn.getColumn();
                        if (sourceColumn.getParent_name() != null
                                    && sourceColumn.getParent_name().length() > 0) {
                            columnName = sourceColumn.getParent_name()
                                        + "."
                                        + columnName;
                        }
                        columnSet.add(columnName);
                    }
                    String[] columns = columnSet.toArray(new String[0]);
                    for (int j = 0; j < columns.length; j++) {
                        buffer.append(columns[j]);
                        if (j == columns.length - 1) {
                            buffer.append("\n");
                        } else {
                            buffer.append(", ");
                        }
                    }
                }
            }
        }
        return buffer.toString();
    }

    private dataflow getSimpleDataflow(dataflow instance) throws Exception {

        dataflow simple = new dataflow();
        List<relation> simpleRelations = new ArrayList<relation>();
        List<relation> relations = instance.getRelations();
        if (relations != null) {
            for (int i = 0; i < relations.size(); i++) {
                relation relationElem = relations.get(i);
                targetColumn target = relationElem.getTarget();
                String targetParent = target.getParent_id();
                if (isTarget(instance, targetParent)) {
                    List<sourceColumn> relationSources = new ArrayList<sourceColumn>();
                    findSourceRaltions(instance,
                                relationElem.getSources(),
                                relationSources);
                    if (relationSources.size() > 0) {
                        relation simpleRelation = (relation) relationElem.clone();
                        simpleRelation.setSources(relationSources);
                        simpleRelations.add(simpleRelation);
                    }
                }
            }
        }
        simple.setTables(instance.getTables());
        simple.setViews(instance.getViews());
        if (instance.getResultsets() != null) {
            List<table> resultSets = new ArrayList<table>();
            for (int i = 0; i < instance.getResultsets().size(); i++) {
                table resultSet = instance.getResultsets().get(i);
                if (resultSet.isTarget()) {
                    resultSets.add(resultSet);
                }
            }
            simple.setResultsets(resultSets);
        }
        simple.setRelations(simpleRelations);
        return simple;
    }

    private void findSourceRaltions(dataflow instance,
                List<sourceColumn> sources, List<sourceColumn> relationSources) {
        if (sources != null) {
            for (int i = 0; i < sources.size(); i++) {
                sourceColumn source = sources.get(i);
                String sourceColumnId = source.getId();
                String sourceParentId = source.getParent_id();
                if (isTarget(instance, sourceParentId)) {
                    relationSources.add(source);
                } else {
                    for (int j = 0; j < instance.getRelations().size(); j++) {
                        relation relation = instance.getRelations().get(j);
                        targetColumn target = relation.getTarget();
                        String targetColumnId = target.getId();
                        String targetParentId = target.getParent_id();

                        if (sourceParentId.equals(targetParentId)
                                    && sourceColumnId.equals(targetColumnId)) {
                            findSourceRaltions(instance,
                                        relation.getSources(),
                                        relationSources);
                            break;
                        }
                    }
                }
            }
        }
    }

    private Map<String, Boolean> targetTables = new HashMap<String, Boolean>();

    private boolean isTarget(dataflow instance, String targetParent) {
        if (targetTables.containsKey(targetParent)) {
            return targetTables.get(targetParent);
        }
        if (isTable(instance, targetParent)) {
            targetTables.put(targetParent, true);
            return true;
        } else if (isView(instance, targetParent)) {
            targetTables.put(targetParent, true);
            return true;
        } else if (isTargetResultSet(instance, targetParent)) {
            targetTables.put(targetParent, true);
            return true;
        }
        targetTables.put(targetParent, false);
        return false;
    }

    private boolean isTargetResultSet(dataflow instance, String targetParent) {
        if (instance.getResultsets() != null) {
            for (int i = 0; i < instance.getResultsets().size(); i++) {
                table resultSet = instance.getResultsets().get(i);
                if (resultSet.getId().equals(targetParent)) {
                    return resultSet.isTarget();
                }
            }
        }
        return false;
    }

    private boolean isView(dataflow instance, String targetParent) {
        if (instance.getViews() != null) {
            for (int i = 0; i < instance.getViews().size(); i++) {
                table resultSet = instance.getViews().get(i);
                if (resultSet.getId().equals(targetParent)) {
                    return true;
                }
            }
        }
        return false;
    }

    private Map<String, Boolean> isTables = new HashMap<String, Boolean>();

    private boolean isTable(dataflow instance, String targetParent) {
        if (isTables.containsKey(targetParent)) {
            return isTables.get(targetParent);
        }
        if (instance.getTables() != null) {
            for (int i = 0; i < instance.getTables().size(); i++) {
                table resultSet = instance.getTables().get(i);
                isTables.put(resultSet.getId(), true);
                if (resultSet.getId().equals(targetParent)) {
                    return true;
                }
            }
        }
        isTables.put(targetParent, false);
        return false;
    }

    private void init() {
        dataflowString = null;
        dataflowResult = null;
        appendResultSets.clear();
        Table.TABLE_ID = 0;
        TableColumn.TABLE_COLUMN_ID = 0;
        AbstractRelation.RELATION_ID = 0;
        ResultSet.DISPLAY_ID.clear();
        ResultSet.DISPLAY_NAME.clear();
    }

    private void analyzeAndOutputResult(TGSqlParser sqlparser, Document doc,
                Element dlineageResult) {
        try {
            accessedStatements.clear();
            stmtStack.clear();
            ModelBindingManager.reset();

            try {
                int result = sqlparser.parse();
                if (result != 0) {
                    System.err.println(sqlparser.getErrormessage());
                }

                if (handleListener != null) {
                    handleListener.endParse();
                }
            } catch (Exception e) {
                if (handleListener != null) {
                    handleListener.endParse();
                }

                e.printStackTrace();
                return;
            }

            if (handleListener != null) {
                handleListener.startAnalyzeDataFlow(sqlparser.sqlstatements.size());
            }

            for (int i = 0; i < sqlparser.sqlstatements.size(); i++) {
                if (handleListener != null && handleListener.isCanceled()) {
                    break;
                }

                if (handleListener != null) {
                    handleListener.startAnalyzeStatment(i);
                }

                TCustomSqlStatement stmt = sqlparser.getSqlstatements()
                            .get(i);
                if (stmt.getErrorCount() == 0) {
                    analyzeCustomSqlStmt(stmt);
                }

                if (handleListener != null) {
                    handleListener.endAnalyzeStatment(i);
                }
            }

            appendTables(doc, dlineageResult);
            appendViews(doc, dlineageResult);
            appendResultSets(doc, dlineageResult);
            appendRelations(doc, dlineageResult);

            if (handleListener != null) {
                handleListener.endAnalyzeDataFlow();
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }

    }

    private void analyzeCustomSqlStmt(TCustomSqlStatement stmt) {
        if (stmt instanceof TCreateTableSqlStatement) {
            stmtStack.push(stmt);
            analyzeCreateTableStmt((TCreateTableSqlStatement) stmt);
            stmtStack.pop();
        } else if (stmt instanceof TSelectSqlStatement) {
            analyzeSelectStmt((TSelectSqlStatement) stmt);
        } else if (stmt instanceof TCreateViewSqlStatement) {
            stmtStack.push(stmt);
            analyzeCreateViewStmt((TCreateViewSqlStatement) stmt);
            stmtStack.pop();
        } else if (stmt instanceof TInsertSqlStatement) {
            stmtStack.push(stmt);
            analyzeInsertStmt((TInsertSqlStatement) stmt);
            stmtStack.pop();
        } else if (stmt instanceof TUpdateSqlStatement) {
            stmtStack.push(stmt);
            analyzeUpdateStmt((TUpdateSqlStatement) stmt);
            stmtStack.pop();
        } else if (stmt instanceof TMergeSqlStatement) {
            stmtStack.push(stmt);
            analyzeMergeStmt((TMergeSqlStatement) stmt);
            stmtStack.pop();
        } else if (stmt.getStatements() != null
                    && stmt.getStatements().size() > 0) {
            for (int i = 0; i < stmt.getStatements().size(); i++) {
                analyzeCustomSqlStmt(stmt.getStatements().get(i));
            }
        }
    }

    private void analyzeCreateTableStmt(TCreateTableSqlStatement stmt) {
        TTable table = stmt.getTargetTable();
        if (table != null) {
            Table tableModel = ModelFactory.createTableFromCreateDML(table);
            if (stmt.getColumnList() != null
                        && stmt.getColumnList().size() > 0) {
                for (int i = 0; i < stmt.getColumnList().size(); i++) {
                    TColumnDefinition column = stmt.getColumnList()
                                .getColumn(i);
                    ModelFactory.createTableColumn(tableModel,
                                column.getColumnName());
                }
            }

            if (stmt.getSubQuery() != null) {
                analyzeSelectStmt(stmt.getSubQuery());
            }

            if (stmt.getSubQuery() != null
                        && stmt.getSubQuery().getResultColumnList() != null) {
                SelectResultSet resultSetModel = (SelectResultSet) ModelBindingManager.getModel(stmt.getSubQuery()
                            .getResultColumnList());
                for (int i = 0; i < resultSetModel.getColumns().size(); i++) {
                    ResultColumn resultColumn = resultSetModel.getColumns()
                                .get(i);
                    if (resultColumn.getColumnObject() instanceof TResultColumn) {
                        TResultColumn columnObject = (TResultColumn) resultColumn.getColumnObject();

                        TAliasClause alias = columnObject.getAliasClause();
                        if (alias != null && alias.getAliasName() != null) {
                            TableColumn tableColumn = ModelFactory.createTableColumn(tableModel,
                                        alias.getAliasName());
                            DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                            relation.setTarget(new TableColumnRelationElement(tableColumn));
                            relation.addSource(new ResultColumnRelationElement(resultColumn));
                        } else if (columnObject.getFieldAttr() != null) {
                            TableColumn tableColumn = ModelFactory.createTableColumn(tableModel,
                                        columnObject.getFieldAttr());

                            ResultColumn column = (ResultColumn) ModelBindingManager.getModel(resultColumn.getColumnObject());
                            if (column != null
                                        && !column.getStarLinkColumns().isEmpty()) {
                                tableColumn.bindStarLinkColumns(column.getStarLinkColumns());
                            }

                            DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                            relation.setTarget(new TableColumnRelationElement(tableColumn));
                            relation.addSource(new ResultColumnRelationElement(resultColumn));
                        } else {
                            System.err.println();
                            System.err.println("Can't handle table column, the create table statement is");
                            System.err.println(stmt.toString());
                            continue;
                        }
                    } else if (resultColumn.getColumnObject() instanceof TObjectName) {
                        TableColumn tableColumn = ModelFactory.createTableColumn(tableModel,
                                    (TObjectName) resultColumn.getColumnObject());
                        DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                        relation.setTarget(new TableColumnRelationElement(tableColumn));
                        relation.addSource(new ResultColumnRelationElement(resultColumn));
                    }
                }
            } else if (stmt.getSubQuery() != null) {
                SelectSetResultSet resultSetModel = (SelectSetResultSet) ModelBindingManager.getModel(stmt.getSubQuery());
                for (int i = 0; i < resultSetModel.getColumns().size(); i++) {
                    ResultColumn resultColumn = resultSetModel.getColumns()
                                .get(i);
                    if (resultColumn.getColumnObject() instanceof TResultColumn) {
                        TResultColumn columnObject = (TResultColumn) resultColumn.getColumnObject();

                        TAliasClause alias = columnObject.getAliasClause();
                        if (alias != null && alias.getAliasName() != null) {
                            TableColumn viewColumn = ModelFactory.createTableColumn(tableModel,
                                        alias.getAliasName());
                            DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                            relation.setTarget(new TableColumnRelationElement(viewColumn));
                            relation.addSource(new ResultColumnRelationElement(resultColumn));
                        } else if (columnObject.getFieldAttr() != null) {
                            TableColumn tableColumn = ModelFactory.createTableColumn(tableModel,
                                        columnObject.getFieldAttr());

                            ResultColumn column = (ResultColumn) ModelBindingManager.getModel(resultColumn.getColumnObject());
                            if (column != null
                                        && !column.getStarLinkColumns().isEmpty()) {
                                tableColumn.bindStarLinkColumns(column.getStarLinkColumns());
                            }

                            DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                            relation.setTarget(new TableColumnRelationElement(tableColumn));
                            relation.addSource(new ResultColumnRelationElement(resultColumn));
                        } else {
                            System.err.println();
                            System.err.println("Can't handle table column, the create table statement is");
                            System.err.println(stmt.toString());
                            continue;
                        }
                    } else if (resultColumn.getColumnObject() instanceof TObjectName) {
                        TableColumn viewColumn = ModelFactory.createTableColumn(tableModel,
                                    (TObjectName) resultColumn.getColumnObject());
                        DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                        relation.setTarget(new TableColumnRelationElement(viewColumn));
                        relation.addSource(new ResultColumnRelationElement(resultColumn));
                    }
                }
            }
        } else {
            System.err.println();
            System.err.println("Can't get target table. CreateTableSqlStatement is ");
            System.err.println(stmt.toString());
        }
    }

    private void analyzeMergeStmt(TMergeSqlStatement stmt) {
        if (stmt.getUsingTable() != null) {
            TTable table = stmt.getTargetTable();
            Table tableModel = ModelFactory.createTable(table);

            if (stmt.getUsingTable().getSubquery() != null) {
                ModelFactory.createQueryTable(stmt.getUsingTable());
                analyzeSelectStmt(stmt.getUsingTable().getSubquery());
            } else {
                ModelFactory.createTable(stmt.getUsingTable());
            }
            if (stmt.getWhenClauses() != null
                        && stmt.getWhenClauses().size() > 0) {
                for (int i = 0; i < stmt.getWhenClauses().size(); i++) {
                    TMergeWhenClause clause = stmt.getWhenClauses()
                                .getElement(i);
                    if (clause.getUpdateClause() != null) {
                        TResultColumnList columns = clause.getUpdateClause()
                                    .getUpdateColumnList();
                        if (columns == null || columns.size() == 0) {
                            continue;
                        }

                        ResultSet resultSet = ModelFactory.createResultSet(clause.getUpdateClause(),
                                    true);

                        for (int j = 0; j < columns.size(); j++) {
                            TResultColumn resultColumn = columns.getResultColumn(j);
                            if (resultColumn.getExpr()
                                        .getLeftOperand()
                                        .getExpressionType() == EExpressionType.simple_object_name_t) {
                                TObjectName columnObject = resultColumn.getExpr()
                                            .getLeftOperand()
                                            .getObjectOperand();

                                ResultColumn updateColumn = ModelFactory.createMergeResultColumn(resultSet,
                                            columnObject);

                                TExpression valueExpression = resultColumn.getExpr()
                                            .getRightOperand();
                                if (valueExpression == null) {
                                    continue;
                                }

                                columnsInExpr visitor = new columnsInExpr();
                                valueExpression.inOrderTraverse(visitor);
                                List<TObjectName> objectNames = visitor.getObjectNames();
                                analyzeDataFlowRelation(updateColumn,
                                            objectNames);

                                TableColumn tableColumn = ModelFactory.createTableColumn(tableModel,
                                            columnObject);

                                DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                                relation.setTarget(new TableColumnRelationElement(tableColumn));
                                relation.addSource(new ResultColumnRelationElement(updateColumn));
                            }
                        }
                    }
                    if (clause.getInsertClause() != null) {
                        TObjectNameList columns = clause.getInsertClause()
                                    .getColumnList();
                        TResultColumnList values = clause.getInsertClause()
                                    .getValuelist();
                        if (columns == null
                                    || columns.size() == 0
                                    || values == null
                                    || values.size() == 0) {
                            continue;
                        }

                        ResultSet resultSet = ModelFactory.createResultSet(clause.getInsertClause(),
                                    true);

                        for (int j = 0; j < columns.size()
                                    && j < values.size(); j++) {
                            TObjectName columnObject = columns.getObjectName(j);

                            ResultColumn insertColumn = ModelFactory.createMergeResultColumn(resultSet,
                                        columnObject);

                            TExpression valueExpression = values.getResultColumn(j)
                                        .getExpr();
                            if (valueExpression == null) {
                                continue;
                            }

                            columnsInExpr visitor = new columnsInExpr();
                            valueExpression.inOrderTraverse(visitor);
                            List<TObjectName> objectNames = visitor.getObjectNames();
                            analyzeDataFlowRelation(insertColumn, objectNames);

                            TableColumn tableColumn = ModelFactory.createTableColumn(tableModel,
                                        columnObject);

                            DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                            relation.setTarget(new TableColumnRelationElement(tableColumn));
                            relation.addSource(new ResultColumnRelationElement(insertColumn));
                        }
                    }
                }

            }

            if (stmt.getCondition() != null) {
                analyzeFilterCondtion(stmt.getCondition());
            }
        }
    }

    private void analyzeInsertStmt(TInsertSqlStatement stmt) {
        if (stmt.getSubQuery() != null) {
            TTable table = stmt.getTargetTable();
            Table tableModel = ModelFactory.createTable(table);

            analyzeSelectStmt(stmt.getSubQuery());

            if (stmt.getColumnList() != null
                        && stmt.getColumnList().size() > 0) {
                TObjectNameList items = stmt.getColumnList();

                ResultSet resultSetModel = null;

                if (stmt.getSubQuery().getResultColumnList() == null) {
                    resultSetModel = (ResultSet) ModelBindingManager.getModel(stmt.getSubQuery());
                } else {
                    resultSetModel = (ResultSet) ModelBindingManager.getModel(stmt.getSubQuery()
                                .getResultColumnList());
                }
                if (resultSetModel == null) {
                    System.err.println("Can't get resultset model");
                }

                for (int i = 0; i < items.size()
                            && i < resultSetModel.getColumns().size(); i++) {
                    TObjectName column = items.getObjectName(i);
                    ResultColumn resultColumn = resultSetModel.getColumns()
                                .get(i);
                    if (column != null) {
                        TableColumn tableColumn = ModelFactory.createTableColumn(tableModel,
                                    column);
                        DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                        relation.setTarget(new TableColumnRelationElement(tableColumn));
                        relation.addSource(new ResultColumnRelationElement(resultColumn));
                    }
                }
            } else if (stmt.getSubQuery().getResultColumnList() != null) {
                SelectResultSet resultSetModel = (SelectResultSet) ModelBindingManager.getModel(stmt.getSubQuery()
                            .getResultColumnList());
                for (int i = 0; i < resultSetModel.getColumns().size(); i++) {
                    ResultColumn resultColumn = resultSetModel.getColumns()
                                .get(i);
                    TAliasClause alias = ((TResultColumn) resultColumn.getColumnObject()).getAliasClause();
                    if (alias != null && alias.getAliasName() != null) {
                        TableColumn tableColumn = ModelFactory.createInsertTableColumn(tableModel,
                                    alias.getAliasName());
                        DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                        relation.setTarget(new TableColumnRelationElement(tableColumn));
                        relation.addSource(new ResultColumnRelationElement(resultColumn));
                    } else if (((TResultColumn) resultColumn.getColumnObject()).getFieldAttr() != null) {
                        TableColumn tableColumn = ModelFactory.createInsertTableColumn(tableModel,
                                    ((TResultColumn) resultColumn.getColumnObject()).getFieldAttr());

                        if ("*".equals(getColumnName(((TResultColumn) resultColumn.getColumnObject()).getFieldAttr()))) {
                            TObjectName columnObject = ((TResultColumn) resultColumn.getColumnObject()).getFieldAttr();
                            TTable sourceTable = columnObject.getSourceTable();
                            if (columnObject.getTableToken() != null
                                        && sourceTable != null) {
                                TObjectName[] columns = ModelBindingManager.getTableColumns(sourceTable);
                                for (int j = 0; j < columns.length; j++) {
                                    TObjectName columnName = columns[j];
                                    if ("*".equals(getColumnName(columnName))) {
                                        continue;
                                    }
                                    resultColumn.bindStarLinkColumn(columnName);
                                }
                            } else {
                                TTableList tables = stmt.getTables();
                                for (int k = 0; k < tables.size(); k++) {
                                    TTable tableElement = tables.getTable(k);
                                    TObjectName[] columns = ModelBindingManager.getTableColumns(tableElement);
                                    for (int j = 0; j < columns.length; j++) {
                                        TObjectName columnName = columns[j];
                                        if ("*".equals(getColumnName(columnName))) {
                                            continue;
                                        }
                                        resultColumn.bindStarLinkColumn(columnName);
                                    }
                                }
                            }
                        }

                        if (resultColumn != null
                                    && !resultColumn.getStarLinkColumns()
                                                .isEmpty()) {
                            tableColumn.bindStarLinkColumns(resultColumn.getStarLinkColumns());
                        }

                        DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                        relation.setTarget(new TableColumnRelationElement(tableColumn));
                        relation.addSource(new ResultColumnRelationElement(resultColumn));
                    } else if (((TResultColumn) resultColumn.getColumnObject()).getExpr()
                                .getExpressionType() == EExpressionType.simple_constant_t) {
                        TableColumn tableColumn = ModelFactory.createInsertTableColumn(tableModel,
                                    ((TResultColumn) resultColumn.getColumnObject()).getExpr()
                                                .getConstantOperand(),
                                    i);
                        DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                        relation.setTarget(new TableColumnRelationElement(tableColumn));
                        relation.addSource(new ResultColumnRelationElement(resultColumn));
                    }
                }
            } else if (stmt.getSubQuery() != null) {
                SelectSetResultSet resultSetModel = (SelectSetResultSet) ModelBindingManager.getModel(stmt.getSubQuery());
                if (resultSetModel != null) {
                    for (int i = 0; i < resultSetModel.getColumns().size(); i++) {
                        ResultColumn resultColumn = resultSetModel.getColumns()
                                    .get(i);
                        TAliasClause alias = ((TResultColumn) resultColumn.getColumnObject()).getAliasClause();
                        if (alias != null && alias.getAliasName() != null) {
                            TableColumn tableColumn = ModelFactory.createInsertTableColumn(tableModel,
                                        alias.getAliasName());
                            DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                            relation.setTarget(new TableColumnRelationElement(tableColumn));
                            relation.addSource(new ResultColumnRelationElement(resultColumn));
                        } else if (((TResultColumn) resultColumn.getColumnObject()).getFieldAttr() != null) {
                            TableColumn tableColumn = ModelFactory.createInsertTableColumn(tableModel,
                                        ((TResultColumn) resultColumn.getColumnObject()).getFieldAttr());
                            DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                            relation.setTarget(new TableColumnRelationElement(tableColumn));
                            relation.addSource(new ResultColumnRelationElement(resultColumn));
                        } else if (((TResultColumn) resultColumn.getColumnObject()).getExpr()
                                    .getExpressionType() == EExpressionType.simple_constant_t) {
                            TableColumn tableColumn = ModelFactory.createInsertTableColumn(tableModel,
                                        ((TResultColumn) resultColumn.getColumnObject()).getExpr()
                                                    .getConstantOperand(),
                                        i);
                            DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                            relation.setTarget(new TableColumnRelationElement(tableColumn));
                            relation.addSource(new ResultColumnRelationElement(resultColumn));
                        }
                    }
                }
            }
        }
    }

    private void analyzeUpdateStmt(TUpdateSqlStatement stmt) {
        if (stmt.getResultColumnList() == null) {
            return;
        }

        TTable table = stmt.getTargetTable();
        Table tableModel = ModelFactory.createTable(table);

        for (int i = 0; i < stmt.tables.size(); i++) {
            TTable tableElement = stmt.tables.getTable(i);
            if (tableElement.getSubquery() != null) {
                QueryTable queryTable = ModelFactory.createQueryTable(tableElement);
                TSelectSqlStatement subquery = tableElement.getSubquery();
                analyzeSelectStmt(subquery);

                if (subquery.getSetOperatorType() != ESetOperatorType.none) {
                    SelectSetResultSet selectSetResultSetModel = (SelectSetResultSet) ModelBindingManager.getModel(subquery);
                    for (int j = 0; j < selectSetResultSetModel.getColumns()
                                .size(); j++) {
                        ResultColumn sourceColumn = selectSetResultSetModel.getColumns()
                                    .get(j);
                        ResultColumn targetColumn = ModelFactory.createSelectSetResultColumn(queryTable,
                                    sourceColumn);
                        DataFlowRelation selectSetRalation = ModelFactory.createDataFlowRelation();
                        selectSetRalation.setTarget(new ResultColumnRelationElement(targetColumn));
                        selectSetRalation.addSource(new ResultColumnRelationElement(sourceColumn));
                    }
                }
            } else {
                ModelFactory.createTable(stmt.tables.getTable(i));
            }
        }

        for (int i = 0; i < stmt.getResultColumnList().size(); i++) {
            TResultColumn field = stmt.getResultColumnList()
                        .getResultColumn(i);

            TExpression expression = field.getExpr().getLeftOperand();
            if (expression == null) {
                System.err.println();
                System.err.println("Can't handle this case. Expression is ");
                System.err.println(field.getExpr().toString());
                continue;
            }
            if (expression.getExpressionType() == EExpressionType.list_t) {
                TExpression setExpression = field.getExpr().getRightOperand();
                if (setExpression != null
                            && setExpression.getSubQuery() != null) {
                    TSelectSqlStatement query = setExpression.getSubQuery();
                    analyzeSelectStmt(query);

                    SelectResultSet resultSetModel = (SelectResultSet) ModelBindingManager.getModel(query.getResultColumnList());

                    TExpressionList columnList = expression.getExprList();
                    for (int j = 0; j < columnList.size(); j++) {
                        TObjectName column = columnList.getExpression(j)
                                    .getObjectOperand();
                        ResultColumn resultColumn = resultSetModel.getColumns()
                                    .get(j);
                        TableColumn tableColumn = ModelFactory.createTableColumn(tableModel,
                                    column);
                        DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                        relation.setTarget(new TableColumnRelationElement(tableColumn));
                        relation.addSource(new ResultColumnRelationElement(resultColumn));

                    }
                }
            } else if (expression.getExpressionType() == EExpressionType.simple_object_name_t) {
                TExpression setExpression = field.getExpr().getRightOperand();
                if (setExpression != null
                            && setExpression.getSubQuery() != null) {
                    TSelectSqlStatement query = setExpression.getSubQuery();
                    analyzeSelectStmt(query);

                    SelectResultSet resultSetModel = (SelectResultSet) ModelBindingManager.getModel(query.getResultColumnList());

                    TObjectName column = expression.getObjectOperand();
                    ResultColumn resultColumn = resultSetModel.getColumns()
                                .get(0);
                    TableColumn tableColumn = ModelFactory.createTableColumn(tableModel,
                                column);
                    DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                    relation.setTarget(new TableColumnRelationElement(tableColumn));
                    relation.addSource(new ResultColumnRelationElement(resultColumn));
                } else if (setExpression != null) {
                    ResultSet resultSet = ModelFactory.createResultSet(stmt,
                                true);

                    TObjectName columnObject = expression.getObjectOperand();

                    ResultColumn updateColumn = ModelFactory.createUpdateResultColumn(resultSet,
                                columnObject);

                    columnsInExpr visitor = new columnsInExpr();
                    field.getExpr()
                                .getRightOperand()
                                .inOrderTraverse(visitor);

                    List<TObjectName> objectNames = visitor.getObjectNames();
                    analyzeDataFlowRelation(updateColumn, objectNames);

                    TableColumn tableColumn = ModelFactory.createTableColumn(tableModel,
                                columnObject);

                    DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                    relation.setTarget(new TableColumnRelationElement(tableColumn));
                    relation.addSource(new ResultColumnRelationElement(updateColumn));
                }
            }
        }

        if (stmt.getJoins() != null && stmt.getJoins().size() > 0) {
            for (int i = 0; i < stmt.getJoins().size(); i++) {
                TJoin join = stmt.getJoins().getJoin(i);
                if (join.getJoinItems() != null) {
                    for (int j = 0; j < join.getJoinItems().size(); j++) {
                        TJoinItem joinItem = join.getJoinItems()
                                    .getJoinItem(j);
                        TExpression expr = joinItem.getOnCondition();
                        analyzeFilterCondtion(expr);
                    }
                }
            }
        }

        if (stmt.getWhereClause() != null
                    && stmt.getWhereClause().getCondition() != null) {
            analyzeFilterCondtion(stmt.getWhereClause().getCondition());
        }
    }

    private void analyzeCreateViewStmt(TCreateViewSqlStatement stmt) {
        if (stmt.getSubquery() != null) {
            analyzeSelectStmt(stmt.getSubquery());
        }

        if (stmt.getViewAliasClause() != null
                    && stmt.getViewAliasClause().getViewAliasItemList() != null) {
            TViewAliasItemList viewItems = stmt.getViewAliasClause()
                        .getViewAliasItemList();
            View viewModel = ModelFactory.createView(stmt);
            ResultSet resultSetModel = (ResultSet) ModelBindingManager.getModel(stmt.getSubquery());
            for (int i = 0; i < viewItems.size(); i++) {
                TObjectName alias = viewItems.getViewAliasItem(i).getAlias();
                ResultColumn resultColumn;
                if (resultSetModel.getColumns().size() <= i) {
                    resultColumn = resultSetModel.getColumns()
                                .get(resultSetModel.getColumns().size() - 1);
                } else {
                    resultColumn = resultSetModel.getColumns().get(i);
                }
                if (alias != null) {
                    ViewColumn viewColumn = ModelFactory.createViewColumn(viewModel,
                                alias,
                                i);
                    DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                    relation.setTarget(new ViewColumnRelationElement(viewColumn));
                    relation.addSource(new ResultColumnRelationElement(resultColumn));
                } else if (resultColumn.getColumnObject() instanceof TObjectName) {
                    ViewColumn viewColumn = ModelFactory.createViewColumn(viewModel,
                                (TObjectName) resultColumn.getColumnObject(),
                                i);
                    DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                    relation.setTarget(new ViewColumnRelationElement(viewColumn));
                    relation.addSource(new ResultColumnRelationElement(resultColumn));
                } else if (resultColumn.getColumnObject() instanceof TResultColumn) {
                    ViewColumn viewColumn = ModelFactory.createViewColumn(viewModel,
                                ((TResultColumn) resultColumn.getColumnObject()).getFieldAttr(),
                                i);
                    ResultColumn column = (ResultColumn) ModelBindingManager.getModel(resultColumn.getColumnObject());
                    if (column != null
                                && !column.getStarLinkColumns().isEmpty()) {
                        viewColumn.bindStarLinkColumns(column.getStarLinkColumns());
                    }
                    DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                    relation.setTarget(new ViewColumnRelationElement(viewColumn));
                    relation.addSource(new ResultColumnRelationElement(resultColumn));
                }
            }
        } else {
            View viewModel = ModelFactory.createView(stmt);
            if (stmt.getSubquery() != null
                        && stmt.getSubquery().getResultColumnList() != null) {
                SelectResultSet resultSetModel = (SelectResultSet) ModelBindingManager.getModel(stmt.getSubquery()
                            .getResultColumnList());
                for (int i = 0; i < resultSetModel.getColumns().size(); i++) {
                    ResultColumn resultColumn = resultSetModel.getColumns()
                                .get(i);
                    if (resultColumn.getColumnObject() instanceof TResultColumn) {
                        TResultColumn columnObject = ((TResultColumn) resultColumn.getColumnObject());

                        TAliasClause alias = ((TResultColumn) resultColumn.getColumnObject()).getAliasClause();
                        if (alias != null && alias.getAliasName() != null) {
                            ViewColumn viewColumn = ModelFactory.createViewColumn(viewModel,
                                        alias.getAliasName(),
                                        i);
                            DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                            relation.setTarget(new ViewColumnRelationElement(viewColumn));
                            relation.addSource(new ResultColumnRelationElement(resultColumn));
                        } else if (columnObject.getFieldAttr() != null) {
                            ViewColumn viewColumn = ModelFactory.createViewColumn(viewModel,
                                        columnObject.getFieldAttr(),
                                        i);
                            ResultColumn column = (ResultColumn) ModelBindingManager.getModel(resultColumn.getColumnObject());
                            if (column != null
                                        && !column.getStarLinkColumns().isEmpty()) {
                                viewColumn.bindStarLinkColumns(column.getStarLinkColumns());
                            }
                            DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                            relation.setTarget(new ViewColumnRelationElement(viewColumn));
                            relation.addSource(new ResultColumnRelationElement(resultColumn));
                        } else if (resultColumn.getAlias() != null
                                    && columnObject.getExpr().getExpressionType() == EExpressionType.sqlserver_proprietary_column_alias_t) {
                            ViewColumn viewColumn = ModelFactory.createViewColumn(viewModel,
                                        columnObject.getExpr()
                                                    .getLeftOperand()
                                                    .getObjectOperand(),
                                        i);
                            DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                            relation.setTarget(new ViewColumnRelationElement(viewColumn));
                            relation.addSource(new ResultColumnRelationElement(resultColumn));
                        } else {
                            System.err.println();
                            System.err.println("Can't handle view column, the view statement is");
                            System.err.println(stmt.toString());
                            continue;
                        }
                    } else if (resultColumn.getColumnObject() instanceof TObjectName) {
                        ViewColumn viewColumn = ModelFactory.createViewColumn(viewModel,
                                    (TObjectName) resultColumn.getColumnObject(),
                                    i);
                        DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                        relation.setTarget(new ViewColumnRelationElement(viewColumn));
                        relation.addSource(new ResultColumnRelationElement(resultColumn));
                    }
                }
            } else if (stmt.getSubquery() != null) {
                SelectSetResultSet resultSetModel = (SelectSetResultSet) ModelBindingManager.getModel(stmt.getSubquery());
                for (int i = 0; i < resultSetModel.getColumns().size(); i++) {
                    ResultColumn resultColumn = resultSetModel.getColumns()
                                .get(i);

                    if (resultColumn.getColumnObject() instanceof TResultColumn) {
                        TResultColumn columnObject = ((TResultColumn) resultColumn.getColumnObject());

                        TAliasClause alias = columnObject.getAliasClause();
                        if (alias != null && alias.getAliasName() != null) {
                            ViewColumn viewColumn = ModelFactory.createViewColumn(viewModel,
                                        alias.getAliasName(),
                                        i);
                            DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                            relation.setTarget(new ViewColumnRelationElement(viewColumn));
                            relation.addSource(new ResultColumnRelationElement(resultColumn));
                        } else if (columnObject.getFieldAttr() != null) {
                            ViewColumn viewColumn = ModelFactory.createViewColumn(viewModel,
                                        columnObject.getFieldAttr(),
                                        i);
                            ResultColumn column = (ResultColumn) ModelBindingManager.getModel(resultColumn.getColumnObject());
                            if (column != null
                                        && !column.getStarLinkColumns().isEmpty()) {
                                viewColumn.bindStarLinkColumns(column.getStarLinkColumns());
                            }
                            DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                            relation.setTarget(new ViewColumnRelationElement(viewColumn));
                            relation.addSource(new ResultColumnRelationElement(resultColumn));
                        } else if (resultColumn.getAlias() != null
                                    && columnObject.getExpr().getExpressionType() == EExpressionType.sqlserver_proprietary_column_alias_t) {
                            ViewColumn viewColumn = ModelFactory.createViewColumn(viewModel,
                                        columnObject.getExpr()
                                                    .getLeftOperand()
                                                    .getObjectOperand(),
                                        i);
                            DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                            relation.setTarget(new ViewColumnRelationElement(viewColumn));
                            relation.addSource(new ResultColumnRelationElement(resultColumn));
                        } else {
                            System.err.println();
                            System.err.println("Can't handle view column, the view statement is");
                            System.err.println(stmt.toString());
                            continue;
                        }
                    } else if (resultColumn.getColumnObject() instanceof TObjectName) {
                        ViewColumn viewColumn = ModelFactory.createViewColumn(viewModel,
                                    (TObjectName) resultColumn.getColumnObject(),
                                    i);
                        DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                        relation.setTarget(new ViewColumnRelationElement(viewColumn));
                        relation.addSource(new ResultColumnRelationElement(resultColumn));
                    }
                }
            }
        }
    }

    private void appendRelations(Document doc, Element dlineageResult) {
        Relation[] relations = ModelBindingManager.getRelations();
        if (simpleOutput) {
            appendRelation(doc,
                        dlineageResult,
                        relations,
                        DataFlowRelation.class);
        } else {
            appendRelation(doc,
                        dlineageResult,
                        relations,
                        DataFlowRelation.class);
            appendRecordSetRelation(doc, dlineageResult, relations);
            appendRelation(doc,
                        dlineageResult,
                        relations,
                        RecordSetRelation.class);
            appendRelation(doc,
                        dlineageResult,
                        relations,
                        ImpactRelation.class);
        }
    }

    private void appendRelation(Document doc, Element dlineageResult,
                Relation[] relations, Class<? extends Relation> clazz) {
        for (int i = 0; i < relations.length; i++) {
            AbstractRelation relation = (AbstractRelation) relations[i];

            Object targetElement = relation.getTarget().getElement();
            if (targetElement instanceof ResultColumn) {
                ResultColumn targetColumn = (ResultColumn) targetElement;
                if (!targetColumn.getStarLinkColumns().isEmpty()) {
                    for (int j = 0; j < targetColumn.getStarLinkColumns()
                                .size(); j++) {
                        appendStarRelation(doc, dlineageResult, relation, j);
                    }
                    continue;
                }
            } else if (targetElement instanceof ViewColumn) {
                ViewColumn targetColumn = (ViewColumn) targetElement;
                if (!targetColumn.getStarLinkColumns().isEmpty()) {
                    for (int j = 0; j < targetColumn.getStarLinkColumns()
                                .size(); j++) {
                        appendStarRelation(doc, dlineageResult, relation, j);
                    }
                    continue;
                }
            } else if (targetElement instanceof TableColumn) {
                TableColumn targetColumn = (TableColumn) targetElement;
                if (!targetColumn.getStarLinkColumns().isEmpty()) {
                    for (int j = 0; j < targetColumn.getStarLinkColumns()
                                .size(); j++) {
                        appendStarRelation(doc, dlineageResult, relation, j);
                    }
                    continue;
                }
            }

            Element relationElement = doc.createElement("relation");
            relationElement.setAttribute("type", relation.getRelationType()
                        .name());
            relationElement.setAttribute("id",
                        String.valueOf(relation.getId()));

            String targetName = null;

            if (relation.getClass() == clazz) {
                if (targetElement instanceof ResultColumn) {
                    ResultColumn targetColumn = (ResultColumn) targetElement;
                    Element target = doc.createElement("target");
                    target.setAttribute("id",
                                String.valueOf(targetColumn.getId()));
                    target.setAttribute("column", targetColumn.getName());
                    target.setAttribute("parent_id",
                                String.valueOf(targetColumn.getResultSet()
                                            .getId()));
                    target.setAttribute("parent_name",
                                getResultSetName(targetColumn.getResultSet()));
                    if (targetColumn.getStartPosition() != null
                                && targetColumn.getEndPosition() != null) {
                        target.setAttribute("coordinate",
                                    targetColumn.getStartPosition()
                                    + ","
                                    + targetColumn.getEndPosition());
                    }
                    if (relation instanceof RecordSetRelation) {
                        target.setAttribute("function",
                                    ((RecordSetRelation) relation).getAggregateFunction());
                    }
                    targetName = targetColumn.getName();
                    relationElement.appendChild(target);
                } else if (targetElement instanceof TableColumn) {
                    TableColumn targetColumn = (TableColumn) targetElement;
                    Element target = doc.createElement("target");
                    target.setAttribute("id",
                                String.valueOf(targetColumn.getId()));
                    target.setAttribute("column", targetColumn.getName());
                    target.setAttribute("parent_id",
                                String.valueOf(targetColumn.getTable().getId()));
                    target.setAttribute("parent_name",
                                getTableName(targetColumn.getTable()));
                    if (targetColumn.getStartPosition() != null
                                && targetColumn.getEndPosition() != null) {
                        target.setAttribute("coordinate",
                                    targetColumn.getStartPosition()
                                    + ","
                                    + targetColumn.getEndPosition());
                    }
                    if (relation instanceof RecordSetRelation) {
                        target.setAttribute("function",
                                    ((RecordSetRelation) relation).getAggregateFunction());
                    }
                    targetName = targetColumn.getName();
                    relationElement.appendChild(target);
                } else if (targetElement instanceof ViewColumn) {
                    ViewColumn targetColumn = (ViewColumn) targetElement;
                    Element target = doc.createElement("target");
                    target.setAttribute("id",
                                String.valueOf(targetColumn.getId()));
                    target.setAttribute("column", targetColumn.getName());
                    target.setAttribute("parent_id",
                                String.valueOf(targetColumn.getView().getId()));
                    target.setAttribute("parent_name", targetColumn.getView()
                                .getName());
                    if (targetColumn.getStartPosition() != null
                                && targetColumn.getEndPosition() != null) {
                        target.setAttribute("coordinate",
                                    targetColumn.getStartPosition()
                                    + ","
                                    + targetColumn.getEndPosition());
                    }
                    if (relation instanceof RecordSetRelation) {
                        target.setAttribute("function",
                                    ((RecordSetRelation) relation).getAggregateFunction());
                    }
                    targetName = targetColumn.getName();
                    relationElement.appendChild(target);
                } else {
                    continue;
                }

                RelationElement<?>[] sourceElements = relation.getSources();
                if (sourceElements.length == 0) {
                    continue;
                }

                boolean append = false;
                for (int j = 0; j < sourceElements.length; j++) {
                    Object sourceElement = sourceElements[j].getElement();
                    if (sourceElement instanceof ResultColumn) {
                        ResultColumn sourceColumn = (ResultColumn) sourceElement;
                        if (!sourceColumn.getStarLinkColumns().isEmpty()) {
                            Element source = doc.createElement("source");

                            if (targetElement instanceof ViewColumn) {
                                source.setAttribute("id",
                                            String.valueOf(sourceColumn.getId())
                                            + "_"
                                            + ((ViewColumn) targetElement).getColumnIndex());
                                source.setAttribute("column",
                                            getColumnName(sourceColumn.getStarLinkColumns()
                                                        .get(((ViewColumn) targetElement).getColumnIndex())));
                                source.setAttribute("parent_id",
                                            String.valueOf(sourceColumn.getResultSet()
                                                        .getId()));
                                source.setAttribute("parent_name",
                                            getResultSetName(sourceColumn.getResultSet()));
                                if (sourceColumn.getStartPosition() != null
                                            && sourceColumn.getEndPosition() != null) {
                                    source.setAttribute("coordinate",
                                                sourceColumn.getStartPosition()
                                                + ","
                                                + sourceColumn.getEndPosition());
                                }
                                append = true;
                                relationElement.appendChild(source);
                            } else {
                                int index = getColumnIndex(sourceColumn.getStarLinkColumns(),
                                            targetName);
                                if (index != -1) {
                                    source.setAttribute("id",
                                                String.valueOf(sourceColumn.getId())
                                                + "_"
                                                + index);
                                    source.setAttribute("column",
                                                getColumnName(sourceColumn.getStarLinkColumns()
                                                            .get(index)));
                                } else {
                                    source.setAttribute("id",
                                                String.valueOf(sourceColumn.getId()));
                                    source.setAttribute("column",
                                                sourceColumn.getName());
                                }
                                source.setAttribute("parent_id",
                                            String.valueOf(sourceColumn.getResultSet()
                                                        .getId()));
                                source.setAttribute("parent_name",
                                            getResultSetName(sourceColumn.getResultSet()));
                                if (sourceColumn.getStartPosition() != null
                                            && sourceColumn.getEndPosition() != null) {
                                    source.setAttribute("coordinate",
                                                sourceColumn.getStartPosition()
                                                + ","
                                                + sourceColumn.getEndPosition());
                                }
                                append = true;
                                relationElement.appendChild(source);
                            }
                        } else {
                            Element source = doc.createElement("source");
                            source.setAttribute("id",
                                        String.valueOf(sourceColumn.getId()));
                            source.setAttribute("column",
                                        sourceColumn.getName());
                            source.setAttribute("parent_id",
                                        String.valueOf(sourceColumn.getResultSet()
                                                    .getId()));
                            source.setAttribute("parent_name",
                                        getResultSetName(sourceColumn.getResultSet()));
                            if (sourceColumn.getStartPosition() != null
                                        && sourceColumn.getEndPosition() != null) {
                                source.setAttribute("coordinate",
                                            sourceColumn.getStartPosition()
                                            + ","
                                            + sourceColumn.getEndPosition());
                            }
                            append = true;
                            relationElement.appendChild(source);
                        }
                    } else if (sourceElement instanceof TableColumn) {
                        TableColumn sourceColumn = (TableColumn) sourceElement;
                        Element source = doc.createElement("source");
                        source.setAttribute("id",
                                    String.valueOf(sourceColumn.getId()));
                        source.setAttribute("column", sourceColumn.getName());
                        source.setAttribute("parent_id",
                                    String.valueOf(sourceColumn.getTable()
                                                .getId()));
                        source.setAttribute("parent_name",
                                    getTableName(sourceColumn.getTable()));
                        if (sourceColumn.getStartPosition() != null
                                    && sourceColumn.getEndPosition() != null) {
                            source.setAttribute("coordinate",
                                        sourceColumn.getStartPosition()
                                        + ","
                                        + sourceColumn.getEndPosition());
                        }
                        append = true;
                        relationElement.appendChild(source);
                    }
                }
                if (append) {
                    dlineageResult.appendChild(relationElement);
                }
            }
        }
    }

    private int getColumnIndex(List<TObjectName> starLinkColumns,
                String targetName) {
        for (int i = 0; i < starLinkColumns.size(); i++) {
            if (getColumnName(starLinkColumns.get(i)).equals(targetName)) {
                return i;
            }
        }
        return -1;
    }

    private void appendStarRelation(Document doc, Element dlineageResult,
                AbstractRelation relation, int index) {
        Object targetElement = relation.getTarget().getElement();

        Element relationElement = doc.createElement("relation");
        relationElement.setAttribute("type", relation.getRelationType()
                    .name());
        relationElement.setAttribute("id", String.valueOf(relation.getId())
                    + "_"
                    + index);

        String targetName = "";

        if (targetElement instanceof ResultColumn) {
            ResultColumn targetColumn = (ResultColumn) targetElement;

            TObjectName linkTargetColumn = targetColumn.getStarLinkColumns()
                        .get(index);
            targetName = getColumnName(linkTargetColumn);

            Element target = doc.createElement("target");
            target.setAttribute("id", String.valueOf(targetColumn.getId())
                        + "_"
                        + index);
            target.setAttribute("column", targetName);

            target.setAttribute("parent_id",
                        String.valueOf(targetColumn.getResultSet().getId()));
            target.setAttribute("parent_name",
                        getResultSetName(targetColumn.getResultSet()));
            if (targetColumn.getStartPosition() != null
                        && targetColumn.getEndPosition() != null) {
                target.setAttribute("coordinate",
                            targetColumn.getStartPosition()
                            + ","
                            + targetColumn.getEndPosition());
            }
            relationElement.appendChild(target);
        } else if (targetElement instanceof ViewColumn) {
            ViewColumn targetColumn = (ViewColumn) targetElement;

            TObjectName linkTargetColumn = targetColumn.getStarLinkColumns()
                        .get(index);
            targetName = getColumnName(linkTargetColumn);

            Element target = doc.createElement("target");
            target.setAttribute("id", String.valueOf(targetColumn.getId())
                        + "_"
                        + index);
            target.setAttribute("column", targetName);
            target.setAttribute("parent_id",
                        String.valueOf(targetColumn.getView().getId()));
            target.setAttribute("parent_name", targetColumn.getView()
                        .getName());
            if (targetColumn.getStartPosition() != null
                        && targetColumn.getEndPosition() != null) {
                target.setAttribute("coordinate",
                            targetColumn.getStartPosition()
                            + ","
                            + targetColumn.getEndPosition());
            }
            relationElement.appendChild(target);
        } else if (targetElement instanceof TableColumn) {
            TableColumn targetColumn = (TableColumn) targetElement;

            TObjectName linkTargetColumn = targetColumn.getStarLinkColumns()
                        .get(index);
            targetName = getColumnName(linkTargetColumn);

            Element target = doc.createElement("target");
            target.setAttribute("id", String.valueOf(targetColumn.getId())
                        + "_"
                        + index);
            target.setAttribute("column", targetName);
            target.setAttribute("parent_id",
                        String.valueOf(targetColumn.getTable().getId()));
            target.setAttribute("parent_name", targetColumn.getTable()
                        .getName());
            if (targetColumn.getStartPosition() != null
                        && targetColumn.getEndPosition() != null) {
                target.setAttribute("coordinate",
                            targetColumn.getStartPosition()
                            + ","
                            + targetColumn.getEndPosition());
            }
            relationElement.appendChild(target);
        } else {
            return;
        }

        RelationElement<?>[] sourceElements = relation.getSources();
        if (sourceElements.length == 0) {
            return;
        }

        for (int j = 0; j < sourceElements.length; j++) {
            Object sourceElement = sourceElements[j].getElement();
            if (sourceElement instanceof ResultColumn) {
                ResultColumn sourceColumn = (ResultColumn) sourceElement;
                if (!sourceColumn.getStarLinkColumns().isEmpty()) {
                    for (int k = 0; k < sourceColumn.getStarLinkColumns()
                                .size(); k++) {
                        TObjectName sourceName = sourceColumn.getStarLinkColumns()
                                    .get(k);
                        Element source = doc.createElement("source");
                        source.setAttribute("id",
                                    String.valueOf(sourceColumn.getId())
                                    + "_"
                                    + k);
                        source.setAttribute("column",
                                    getColumnName(sourceName));
                        source.setAttribute("parent_id",
                                    String.valueOf(sourceColumn.getResultSet()
                                                .getId()));
                        source.setAttribute("parent_name",
                                    getResultSetName(sourceColumn.getResultSet()));
                        if (sourceColumn.getStartPosition() != null
                                    && sourceColumn.getEndPosition() != null) {
                            source.setAttribute("coordinate",
                                        sourceColumn.getStartPosition()
                                        + ","
                                        + sourceColumn.getEndPosition());
                        }
                        if (relation.getRelationType() == RelationType.dataflow) {
                            if (!targetName.equals(getColumnName(sourceName))) {
                                continue;
                            }
                        }
                        relationElement.appendChild(source);
                    }
                } else {
                    Element source = doc.createElement("source");
                    source.setAttribute("id",
                                String.valueOf(sourceColumn.getId()));
                    source.setAttribute("column", sourceColumn.getName());
                    source.setAttribute("parent_id",
                                String.valueOf(sourceColumn.getResultSet()
                                            .getId()));
                    source.setAttribute("parent_name",
                                getResultSetName(sourceColumn.getResultSet()));
                    if (sourceColumn.getStartPosition() != null
                                && sourceColumn.getEndPosition() != null) {
                        source.setAttribute("coordinate",
                                    sourceColumn.getStartPosition()
                                    + ","
                                    + sourceColumn.getEndPosition());
                    }
                    if (relation.getRelationType() == RelationType.dataflow) {
                        if (!targetName.equals(sourceColumn.getName())) {
                            continue;
                        }
                    }
                    relationElement.appendChild(source);
                }
            } else if (sourceElement instanceof TableColumn) {
                TableColumn sourceColumn = (TableColumn) sourceElement;
                Element source = doc.createElement("source");
                source.setAttribute("id",
                            String.valueOf(sourceColumn.getId()));
                source.setAttribute("column", sourceColumn.getName());
                source.setAttribute("parent_id",
                            String.valueOf(sourceColumn.getTable().getId()));
                source.setAttribute("parent_name",
                            getTableName(sourceColumn.getTable()));
                if (sourceColumn.getStartPosition() != null
                            && sourceColumn.getEndPosition() != null) {
                    source.setAttribute("coordinate",
                                sourceColumn.getStartPosition()
                                + ","
                                + sourceColumn.getEndPosition());
                }
                if (relation.getRelationType() == RelationType.dataflow) {
                    if (!targetName.equals(sourceColumn.getName())) {
                        continue;
                    }
                }
                relationElement.appendChild(source);
            }
        }

        dlineageResult.appendChild(relationElement);
    }

    private String getColumnName(TObjectName column) {
        String name = column.getColumnNameOnly();
        if (name == null || "".equals(name.trim())) {
            return column.toString();
        } else {
            return name.trim();
        }
    }

    private void appendRecordSetRelation(Document doc, Element dlineageResult,
                Relation[] relations) {
        for (int i = 0; i < relations.length; i++) {
            AbstractRelation relation = (AbstractRelation) relations[i];
            Element relationElement = doc.createElement("relation");
            relationElement.setAttribute("type", relation.getRelationType()
                        .name());
            relationElement.setAttribute("id",
                        String.valueOf(relation.getId()));

            if (relation instanceof RecordSetRelation) {
                RecordSetRelation recordCountRelation = (RecordSetRelation) relation;

                Object targetElement = recordCountRelation.getTarget()
                            .getElement();
                if (targetElement instanceof ResultColumn) {
                    ResultColumn targetColumn = (ResultColumn) targetElement;
                    Element target = doc.createElement("target");
                    target.setAttribute("id",
                                String.valueOf(targetColumn.getId()));
                    target.setAttribute("column", targetColumn.getName());
                    target.setAttribute("function",
                                recordCountRelation.getAggregateFunction());
                    target.setAttribute("parent_id",
                                String.valueOf(targetColumn.getResultSet()
                                            .getId()));
                    target.setAttribute("parent_name",
                                getResultSetName(targetColumn.getResultSet()));
                    if (targetColumn.getStartPosition() != null
                                && targetColumn.getEndPosition() != null) {
                        target.setAttribute("coordinate",
                                    targetColumn.getStartPosition()
                                    + ","
                                    + targetColumn.getEndPosition());
                    }
                    relationElement.appendChild(target);
                } else if (targetElement instanceof TableColumn) {
                    TableColumn targetColumn = (TableColumn) targetElement;
                    Element target = doc.createElement("target");
                    target.setAttribute("id",
                                String.valueOf(targetColumn.getId()));
                    target.setAttribute("column", targetColumn.getName());
                    target.setAttribute("function",
                                recordCountRelation.getAggregateFunction());
                    target.setAttribute("parent_id",
                                String.valueOf(targetColumn.getTable().getId()));
                    target.setAttribute("parent_name",
                                getTableName(targetColumn.getTable()));
                    if (targetColumn.getStartPosition() != null
                                && targetColumn.getEndPosition() != null) {
                        target.setAttribute("coordinate",
                                    targetColumn.getStartPosition()
                                    + ","
                                    + targetColumn.getEndPosition());
                    }
                    relationElement.appendChild(target);
                } else {
                    continue;
                }

                RelationElement<?>[] sourceElements = recordCountRelation.getSources();
                if (sourceElements.length == 0) {
                    continue;
                }

                boolean append = false;
                for (int j = 0; j < sourceElements.length; j++) {
                    Object sourceElement = sourceElements[j].getElement();
                    if (sourceElement instanceof Table) {
                        Table table = (Table) sourceElement;
                        Element source = doc.createElement("source");
                        source.setAttribute("source_id",
                                    String.valueOf(table.getId()));
                        source.setAttribute("source_name",
                                    getTableName(table));
                        if (table.getStartPosition() != null
                                    && table.getEndPosition() != null) {
                            source.setAttribute("coordinate",
                                        table.getStartPosition()
                                        + ","
                                        + table.getEndPosition());
                        }
                        append = true;
                        relationElement.appendChild(source);
                    } else if (sourceElement instanceof QueryTable) {
                        QueryTable table = (QueryTable) sourceElement;
                        Element source = doc.createElement("source");
                        source.setAttribute("source_id",
                                    String.valueOf(table.getId()));
                        source.setAttribute("source_name",
                                    getResultSetName(table));
                        if (table.getStartPosition() != null
                                    && table.getEndPosition() != null) {
                            source.setAttribute("coordinate",
                                        table.getStartPosition()
                                        + ","
                                        + table.getEndPosition());
                        }
                        append = true;
                        relationElement.appendChild(source);
                    }
                }

                if (append) {
                    dlineageResult.appendChild(relationElement);
                }
            }
        }
    }

    private void appendResultSets(Document doc, Element dlineageResult) {
        List<TResultColumnList> selectResultSets = ModelBindingManager.getSelectResultSets();
        List<TTable> tableWithSelectSetResultSets = ModelBindingManager.getTableWithSelectSetResultSets();
        List<TSelectSqlStatement> selectSetResultSets = ModelBindingManager.getSelectSetResultSets();
        List<TCTE> ctes = ModelBindingManager.getCTEs();
        List<TParseTreeNode> mergeResultSets = ModelBindingManager.getMergeResultSets();
        List<TParseTreeNode> updateResultSets = ModelBindingManager.getUpdateResultSets();

        List<TParseTreeNode> resultSets = new ArrayList<TParseTreeNode>();
        resultSets.addAll(selectResultSets);
        resultSets.addAll(tableWithSelectSetResultSets);
        resultSets.addAll(selectSetResultSets);
        resultSets.addAll(ctes);
        resultSets.addAll(mergeResultSets);
        resultSets.addAll(updateResultSets);

        for (int i = 0; i < resultSets.size(); i++) {
            ResultSet resultSetModel = (ResultSet) ModelBindingManager.getModel(resultSets.get(i));
            appendResultSet(doc, dlineageResult, resultSetModel);
        }
    }

    private void appendResultSet(Document doc, Element dlineageResult,
                ResultSet resultSetModel) {
        if (resultSetModel == null) {
            System.err.println("ResultSet Model should not be null.");
        }

        if (!appendResultSets.contains(resultSetModel)) {
            appendResultSets.add(resultSetModel);
        } else {
            return;
        }

        Element resultSetElement = doc.createElement("resultset");
        resultSetElement.setAttribute("id",
                    String.valueOf(resultSetModel.getId()));
        resultSetElement.setAttribute("name",
                    getResultSetName(resultSetModel));
        resultSetElement.setAttribute("type",
                    getResultSetType(resultSetModel));
        if (simpleOutput && resultSetModel.isTarget()) {
            resultSetElement.setAttribute("isTarget",
                        String.valueOf(resultSetModel.isTarget()));
        }
        if (resultSetModel.getStartPosition() != null
                    && resultSetModel.getEndPosition() != null) {
            resultSetElement.setAttribute("coordinate",
                        resultSetModel.getStartPosition()
                        + ","
                        + resultSetModel.getEndPosition());
        }
        dlineageResult.appendChild(resultSetElement);

        List<ResultColumn> columns = resultSetModel.getColumns();
        for (int j = 0; j < columns.size(); j++) {
            ResultColumn columnModel = columns.get(j);
            if (!columnModel.getStarLinkColumns().isEmpty()) {
                for (int k = 0; k < columnModel.getStarLinkColumns().size(); k++) {
                    Element columnElement = doc.createElement("column");
                    columnElement.setAttribute("id",
                                String.valueOf(columnModel.getId()) + "_" + k);
                    columnElement.setAttribute("name",
                                getColumnName(columnModel.getStarLinkColumns()
                                            .get(k)));
                    if (columnModel.getStartPosition() != null
                                && columnModel.getEndPosition() != null) {
                        columnElement.setAttribute("coordinate",
                                    columnModel.getStartPosition()
                                    + ","
                                    + columnModel.getEndPosition());
                    }
                    resultSetElement.appendChild(columnElement);
                }
            } else {
                Element columnElement = doc.createElement("column");
                columnElement.setAttribute("id",
                            String.valueOf(columnModel.getId()));
                columnElement.setAttribute("name", columnModel.getName());
                if (columnModel.getStartPosition() != null
                            && columnModel.getEndPosition() != null) {
                    columnElement.setAttribute("coordinate",
                                columnModel.getStartPosition()
                                + ","
                                + columnModel.getEndPosition());
                }
                resultSetElement.appendChild(columnElement);
            }
        }
    }

    private String getResultSetType(ResultSet resultSetModel) {
        if (resultSetModel instanceof QueryTable) {
            QueryTable table = (QueryTable) resultSetModel;
            if (table.getTableObject().getCTE() != null) {
                return "with_cte";
            }
        }

        if (resultSetModel instanceof SelectSetResultSet) {
            ESetOperatorType type = ((SelectSetResultSet) resultSetModel).getSetOperatorType();
            return "select_" + type.name();
        }

        if (resultSetModel instanceof SelectResultSet) {
            if (((SelectResultSet) resultSetModel).getSelectStmt()
                        .getParentStmt() instanceof TInsertSqlStatement) {
                return "insert-select";
            }
            if (((SelectResultSet) resultSetModel).getSelectStmt()
                        .getParentStmt() instanceof TUpdateSqlStatement) {
                return "update-set";
            }
        }

        if (resultSetModel.getGspObject() instanceof TMergeUpdateClause) {
            return "merge-update";
        }

        if (resultSetModel.getGspObject() instanceof TMergeInsertClause) {
            return "merge-insert";
        }

        if (resultSetModel.getGspObject() instanceof TUpdateSqlStatement) {
            return "update-set";
        }

        return "select_list";
    }

    private String getTableName(Table tableModel) {
        String tableName;
        if (tableModel.getFullName() != null
                    && tableModel.getFullName().trim().length() > 0) {
            return tableModel.getFullName();
        }
        if (tableModel.getAlias() != null
                    && tableModel.getAlias().trim().length() > 0) {
            tableName = "RESULT_OF_" + tableModel.getAlias().trim();

        } else {
            tableName = getResultSetDisplayId("RESULT_OF_SELECT-QUERY");
        }
        return tableName;
    }

    private String getResultSetName(ResultSet resultSetModel) {

        if (ResultSet.DISPLAY_NAME.containsKey(resultSetModel.getId())) {
            return ResultSet.DISPLAY_NAME.get(resultSetModel.getId());
        }

        if (resultSetModel instanceof QueryTable) {
            QueryTable table = (QueryTable) resultSetModel;
            if (table.getAlias() != null
                        && table.getAlias().trim().length() > 0) {
                String name = "RESULT_OF_" + table.getAlias().trim();
                ResultSet.DISPLAY_NAME.put(resultSetModel.getId(), name);
                return name;
            } else if (table.getTableObject().getCTE() != null) {
                String name = "RESULT_OF_WITH-"
                            + table.getTableObject()
                                        .getCTE()
                                        .getTableName()
                                        .toString();
                ResultSet.DISPLAY_NAME.put(table.getId(), name);
                return name;
            }
        }

        if (resultSetModel instanceof SelectResultSet) {
            if (((SelectResultSet) resultSetModel).getSelectStmt()
                        .getParentStmt() instanceof TInsertSqlStatement) {
                String name = getResultSetDisplayId("INSERT-SELECT");
                ResultSet.DISPLAY_NAME.put(resultSetModel.getId(), name);
                return name;
            }

            if (((SelectResultSet) resultSetModel).getSelectStmt()
                        .getParentStmt() instanceof TUpdateSqlStatement) {
                String name = getResultSetDisplayId("UPDATE-SET");
                ResultSet.DISPLAY_NAME.put(resultSetModel.getId(), name);
                return name;
            }
        }

        if (resultSetModel instanceof SelectSetResultSet) {
            ESetOperatorType type = ((SelectSetResultSet) resultSetModel).getSetOperatorType();
            String name = getResultSetDisplayId("RESULT_OF_"
                        + type.name().toUpperCase());
            ResultSet.DISPLAY_NAME.put(resultSetModel.getId(), name);
            return name;
        }

        if (resultSetModel.getGspObject() instanceof TMergeUpdateClause) {
            String name = getResultSetDisplayId("MERGE-UPDATE");
            ResultSet.DISPLAY_NAME.put(resultSetModel.getId(), name);
            return name;
        }

        if (resultSetModel.getGspObject() instanceof TMergeInsertClause) {
            String name = getResultSetDisplayId("MERGE-INSERT");
            ResultSet.DISPLAY_NAME.put(resultSetModel.getId(), name);
            return name;
        }

        if (resultSetModel.getGspObject() instanceof TUpdateSqlStatement) {
            String name = getResultSetDisplayId("UPDATE-SET");
            ResultSet.DISPLAY_NAME.put(resultSetModel.getId(), name);
            return name;
        }

        String name = getResultSetDisplayId("RESULT_OF_SELECT-QUERY");
        ResultSet.DISPLAY_NAME.put(resultSetModel.getId(), name);
        return name;
    }

    private String getResultSetDisplayId(String type) {
        if (!ResultSet.DISPLAY_ID.containsKey(type)) {
            ResultSet.DISPLAY_ID.put(type, 1);
            return type;
        } else {
            int id = ResultSet.DISPLAY_ID.get(type);
            ResultSet.DISPLAY_ID.put(type, id + 1);
            return type + "-" + id;
        }
    }

    private void appendViews(Document doc, Element dlineageResult) {
        List<TCreateViewSqlStatement> views = ModelBindingManager.getViews();
        for (int i = 0; i < views.size(); i++) {
            View viewModel = (View) ModelBindingManager.getViewModel(views.get(i));
            Element viewElement = doc.createElement("view");
            viewElement.setAttribute("id", String.valueOf(viewModel.getId()));
            viewElement.setAttribute("name", viewModel.getName());
            viewElement.setAttribute("type", "view");

            if (viewModel.getStartPosition() != null
                        && viewModel.getEndPosition() != null) {
                viewElement.setAttribute("coordinate",
                            viewModel.getStartPosition()
                            + ","
                            + viewModel.getEndPosition());
            }
            dlineageResult.appendChild(viewElement);

            List<ViewColumn> columns = viewModel.getColumns();
            for (int j = 0; j < columns.size(); j++) {
                ViewColumn columnModel = columns.get(j);
                if (!columnModel.getStarLinkColumns().isEmpty()) {
                    for (int k = 0; k < columnModel.getStarLinkColumns()
                                .size(); k++) {
                        Element columnElement = doc.createElement("column");
                        columnElement.setAttribute("id",
                                    String.valueOf(columnModel.getId())
                                    + "_"
                                    + k);
                        columnElement.setAttribute("name",
                                    getColumnName(columnModel.getStarLinkColumns()
                                                .get(k)));
                        if (columnModel.getStartPosition() != null
                                    && columnModel.getEndPosition() != null) {
                            columnElement.setAttribute("coordinate",
                                        columnModel.getStartPosition()
                                        + ","
                                        + columnModel.getEndPosition());
                        }
                        viewElement.appendChild(columnElement);
                    }
                } else {
                    Element columnElement = doc.createElement("column");
                    columnElement.setAttribute("id",
                                String.valueOf(columnModel.getId()));
                    columnElement.setAttribute("name", columnModel.getName());
                    if (columnModel.getStartPosition() != null
                                && columnModel.getEndPosition() != null) {
                        columnElement.setAttribute("coordinate",
                                    columnModel.getStartPosition()
                                    + ","
                                    + columnModel.getEndPosition());
                    }
                    viewElement.appendChild(columnElement);
                }
            }
        }
    }

    private void appendTables(Document doc, Element dlineageResult) {
        List<TTable> tables = ModelBindingManager.getBaseTables();
        for (int i = 0; i < tables.size(); i++) {
            Object model = ModelBindingManager.getModel(tables.get(i));
            if (model instanceof Table) {
                Table tableModel = (Table) model;
                Element tableElement = doc.createElement("table");
                tableElement.setAttribute("id",
                            String.valueOf(tableModel.getId()));
                tableElement.setAttribute("name", tableModel.getFullName());
                tableElement.setAttribute("type", "table");
                if (tableModel.getAlias() != null
                            && tableModel.getAlias().trim().length() > 0) {
                    tableElement.setAttribute("alias", tableModel.getAlias());
                }
                if (tableModel.getStartPosition() != null
                            && tableModel.getEndPosition() != null) {
                    tableElement.setAttribute("coordinate",
                                tableModel.getStartPosition()
                                + ","
                                + tableModel.getEndPosition());
                }
                dlineageResult.appendChild(tableElement);

                List<TableColumn> columns = tableModel.getColumns();
                for (int j = 0; j < columns.size(); j++) {
                    TableColumn columnModel = columns.get(j);
                    if (!columnModel.getStarLinkColumns().isEmpty()) {
                        for (int k = 0; k < columnModel.getStarLinkColumns()
                                    .size(); k++) {
                            Element columnElement = doc.createElement("column");
                            columnElement.setAttribute("id",
                                        String.valueOf(columnModel.getId())
                                        + "_"
                                        + k);
                            columnElement.setAttribute("name",
                                        getColumnName(columnModel.getStarLinkColumns()
                                                    .get(k)));
                            if (columnModel.getStartPosition() != null
                                        && columnModel.getEndPosition() != null) {
                                columnElement.setAttribute("coordinate",
                                            columnModel.getStartPosition()
                                            + ","
                                            + columnModel.getEndPosition());
                            }
                            tableElement.appendChild(columnElement);
                        }
                    } else {
                        Element columnElement = doc.createElement("column");
                        columnElement.setAttribute("id",
                                    String.valueOf(columnModel.getId()));
                        columnElement.setAttribute("name",
                                    columnModel.getName());
                        if (columnModel.getStartPosition() != null
                                    && columnModel.getEndPosition() != null) {
                            columnElement.setAttribute("coordinate",
                                        columnModel.getStartPosition()
                                        + ","
                                        + columnModel.getEndPosition());
                        }
                        tableElement.appendChild(columnElement);
                    }
                }
            } else if (model instanceof QueryTable) {
                appendResultSet(doc, dlineageResult, (QueryTable) model);
            }
        }
    }

    private void analyzeSelectStmt(TSelectSqlStatement stmt) {
        if (!accessedStatements.contains(stmt)) {
            accessedStatements.add(stmt);
        } else {
            return;
        }

        if (stmt.getSetOperatorType() != ESetOperatorType.none) {
            analyzeSelectStmt(stmt.getLeftStmt());
            analyzeSelectStmt(stmt.getRightStmt());

            stmtStack.push(stmt);
            SelectSetResultSet resultSet = ModelFactory.createSelectSetResultSet(stmt);

            if (resultSet.getColumns() == null
                        || resultSet.getColumns().isEmpty()) {
                if (stmt.getLeftStmt().getResultColumnList() != null) {
                    createSelectSetResultColumns(resultSet, stmt.getLeftStmt());
                } else if (stmt.getRightStmt().getResultColumnList() != null) {
                    createSelectSetResultColumns(resultSet,
                                stmt.getRightStmt());
                }
            }

            List<ResultColumn> columns = resultSet.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                DataFlowRelation relation = ModelFactory.createDataFlowRelation();
                relation.setTarget(new ResultColumnRelationElement(columns.get(i)));

                if (stmt.getLeftStmt().getResultColumnList() != null) {
                    ResultSet sourceResultSet = (ResultSet) ModelBindingManager.getModel(stmt.getLeftStmt()
                                .getResultColumnList());
                    if (sourceResultSet.getColumns().size() > i) {
                        relation.addSource(new ResultColumnRelationElement(sourceResultSet.getColumns()
                                    .get(i)));
                    }
                } else {
                    ResultSet sourceResultSet = (ResultSet) ModelBindingManager.getModel(stmt.getLeftStmt());
                    if (sourceResultSet != null
                                && sourceResultSet.getColumns().size() > i) {
                        relation.addSource(new ResultColumnRelationElement(sourceResultSet.getColumns()
                                    .get(i)));
                    }
                }

                if (stmt.getRightStmt().getResultColumnList() != null) {
                    ResultSet sourceResultSet = (ResultSet) ModelBindingManager.getModel(stmt.getRightStmt()
                                .getResultColumnList());
                    if (sourceResultSet.getColumns().size() > i) {
                        relation.addSource(new ResultColumnRelationElement(sourceResultSet.getColumns()
                                    .get(i)));
                    }
                } else {
                    ResultSet sourceResultSet = (ResultSet) ModelBindingManager.getModel(stmt.getRightStmt());
                    if (sourceResultSet != null
                                && sourceResultSet.getColumns().size() > i) {
                        relation.addSource(new ResultColumnRelationElement(sourceResultSet.getColumns()
                                    .get(i)));
                    }
                }
            }

            stmtStack.pop();
        } else {
            stmtStack.push(stmt);

            TTableList fromTables = stmt.tables;
            for (int i = 0; i < fromTables.size(); i++) {
                TTable table = fromTables.getTable(i);

                if (table.getSubquery() != null) {
                    QueryTable queryTable = ModelFactory.createQueryTable(table);
                    TSelectSqlStatement subquery = table.getSubquery();
                    analyzeSelectStmt(subquery);

                    if (subquery.getSetOperatorType() != ESetOperatorType.none) {
                        SelectSetResultSet selectSetResultSetModel = (SelectSetResultSet) ModelBindingManager.getModel(subquery);
                        for (int j = 0; j < selectSetResultSetModel.getColumns()
                                    .size(); j++) {
                            ResultColumn sourceColumn = selectSetResultSetModel.getColumns()
                                        .get(j);
                            ResultColumn targetColumn = ModelFactory.createSelectSetResultColumn(queryTable,
                                        sourceColumn);
                            DataFlowRelation selectSetRalation = ModelFactory.createDataFlowRelation();
                            selectSetRalation.setTarget(new ResultColumnRelationElement(targetColumn));
                            selectSetRalation.addSource(new ResultColumnRelationElement(sourceColumn));
                        }
                    }
                } else if (table.getCTE() != null
                            && ModelBindingManager.getModel(table.getCTE()) == null) {
                    QueryTable queryTable = ModelFactory.createQueryTable(table);
                    TSelectSqlStatement subquery = table.getCTE()
                                .getSubquery();
                    if (subquery != null) {
                        analyzeSelectStmt(subquery);

                        if (subquery.getSetOperatorType() != ESetOperatorType.none) {
                            SelectSetResultSet selectSetResultSetModel = (SelectSetResultSet) ModelBindingManager.getModel(subquery);
                            for (int j = 0; j < selectSetResultSetModel.getColumns()
                                        .size(); j++) {
                                ResultColumn sourceColumn = selectSetResultSetModel.getColumns()
                                            .get(j);
                                ResultColumn targetColumn = ModelFactory.createSelectSetResultColumn(queryTable,
                                            sourceColumn);
                                DataFlowRelation selectSetRalation = ModelFactory.createDataFlowRelation();
                                selectSetRalation.setTarget(new ResultColumnRelationElement(targetColumn));
                                selectSetRalation.addSource(new ResultColumnRelationElement(sourceColumn));
                            }
                        } else {
                            ResultSet resultSetModel = (ResultSet) ModelBindingManager.getModel(subquery);
                            for (int j = 0; j < resultSetModel.getColumns()
                                        .size(); j++) {
                                ResultColumn sourceColumn = resultSetModel.getColumns()
                                            .get(j);
                                ResultColumn targetColumn = ModelFactory.createSelectSetResultColumn(queryTable,
                                            sourceColumn);
                                DataFlowRelation selectSetRalation = ModelFactory.createDataFlowRelation();
                                selectSetRalation.setTarget(new ResultColumnRelationElement(targetColumn));
                                selectSetRalation.addSource(new ResultColumnRelationElement(sourceColumn));
                            }
                        }
                    } else if (table.getCTE().getUpdateStmt() != null) {
                        analyzeCustomSqlStmt(table.getCTE().getUpdateStmt());
                    } else if (table.getCTE().getInsertStmt() != null) {
                        analyzeCustomSqlStmt(table.getCTE().getInsertStmt());
                    } else if (table.getCTE().getDeleteStmt() != null) {
                        analyzeCustomSqlStmt(table.getCTE().getDeleteStmt());
                    }
                } else if (table.getObjectNameReferences() != null
                            && table.getObjectNameReferences().size() > 0) {
                    Table tableModel = ModelFactory.createTable(table);
                    for (int j = 0; j < table.getObjectNameReferences()
                                .size(); j++) {
                        TObjectName object = table.getObjectNameReferences()
                                    .getObjectName(j);
                        if (!isFunctionName(object)) {
                            ModelFactory.createTableColumn(tableModel, object);
                        }
                    }
                }
            }

            if (stmt.getResultColumnList() != null) {
                Object queryModel = ModelBindingManager.getModel(stmt.getResultColumnList());

                if (queryModel == null) {
                    TSelectSqlStatement parentStmt = getParentSetSelectStmt(stmt);
                    if (stmt.getParentStmt() == null || parentStmt == null) {
                        SelectResultSet resultSetModel = ModelFactory.createResultSet(stmt,
                                    stmt.getParentStmt() == null);
                        for (int i = 0; i < stmt.getResultColumnList().size(); i++) {
                            TResultColumn column = stmt.getResultColumnList()
                                        .getResultColumn(i);

                            ResultColumn resultColumn = ModelFactory.createResultColumn(resultSetModel,
                                        column);

                            if ("*".equals(column.getColumnNameOnly())) {
                                TObjectName columnObject = column.getFieldAttr();
                                TTable sourceTable = columnObject.getSourceTable();
                                if (columnObject.getTableToken() != null
                                            && sourceTable != null) {
                                    TObjectName[] columns = ModelBindingManager.getTableColumns(sourceTable);
                                    for (int j = 0; j < columns.length; j++) {
                                        TObjectName columnName = columns[j];
                                        if ("*".equals(getColumnName(columnName))) {
                                            continue;
                                        }
                                        resultColumn.bindStarLinkColumn(columnName);
                                    }
                                } else {
                                    TTableList tables = stmt.getTables();
                                    for (int k = 0; k < tables.size(); k++) {
                                        TTable table = tables.getTable(k);
                                        TObjectName[] columns = ModelBindingManager.getTableColumns(table);
                                        for (int j = 0; j < columns.length; j++) {
                                            TObjectName columnName = columns[j];
                                            if ("*".equals(getColumnName(columnName))) {
                                                continue;
                                            }
                                            resultColumn.bindStarLinkColumn(columnName);
                                        }
                                    }
                                }
                            }
                            analyzeResultColumn(column);
                        }
                    }

                    TSelectSqlStatement parent = getParentSetSelectStmt(stmt);
                    if (parent != null
                                && parent.getSetOperatorType() != ESetOperatorType.none) {
                        SelectResultSet resultSetModel = ModelFactory.createResultSet(stmt,
                                    false);
                        for (int i = 0; i < stmt.getResultColumnList().size(); i++) {
                            TResultColumn column = stmt.getResultColumnList()
                                        .getResultColumn(i);
                            ResultColumn resultColumn = ModelFactory.createResultColumn(resultSetModel,
                                        column);
                            if ("*".equals(column.getColumnNameOnly())) {
                                TObjectName columnObject = column.getFieldAttr();
                                TTable sourceTable = columnObject.getSourceTable();
                                if (columnObject.getTableToken() != null
                                            && sourceTable != null) {
                                    TObjectName[] columns = ModelBindingManager.getTableColumns(sourceTable);
                                    for (int j = 0; j < columns.length; j++) {
                                        TObjectName columnName = columns[j];
                                        if ("*".equals(getColumnName(columnName))) {
                                            continue;
                                        }
                                        resultColumn.bindStarLinkColumn(columnName);
                                    }
                                } else {
                                    TTableList tables = stmt.getTables();
                                    for (int k = 0; k < tables.size(); k++) {
                                        TTable table = tables.getTable(k);
                                        TObjectName[] columns = ModelBindingManager.getTableColumns(table);
                                        for (int j = 0; j < columns.length; j++) {
                                            TObjectName columnName = columns[j];
                                            if ("*".equals(getColumnName(columnName))) {
                                                continue;
                                            }
                                            resultColumn.bindStarLinkColumn(columnName);
                                        }
                                    }
                                }
                            }
                            analyzeResultColumn(column);
                        }
                    }
                } else {
                    for (int i = 0; i < stmt.getResultColumnList().size(); i++) {
                        TResultColumn column = stmt.getResultColumnList()
                                    .getResultColumn(i);

                        ResultColumn resultColumn;

                        if (queryModel instanceof QueryTable) {
                            resultColumn = ModelFactory.createResultColumn((QueryTable) queryModel,
                                        column);
                        } else if (queryModel instanceof ResultSet) {
                            resultColumn = ModelFactory.createResultColumn((ResultSet) queryModel,
                                        column);
                        } else {
                            continue;
                        }

                        if ("*".equals(column.getColumnNameOnly())) {
                            TObjectName columnObject = column.getFieldAttr();
                            TTable sourceTable = columnObject.getSourceTable();
                            if (columnObject.getTableToken() != null
                                        && sourceTable != null) {
                                TObjectName[] columns = ModelBindingManager.getTableColumns(sourceTable);
                                for (int j = 0; j < columns.length; j++) {
                                    TObjectName columnName = columns[j];
                                    if ("*".equals(getColumnName(columnName))) {
                                        continue;
                                    }
                                    resultColumn.bindStarLinkColumn(columnName);
                                }
                            } else {
                                TTableList tables = stmt.getTables();
                                for (int k = 0; k < tables.size(); k++) {
                                    TTable table = tables.getTable(k);
                                    TObjectName[] columns = ModelBindingManager.getTableColumns(table);
                                    for (int j = 0; j < columns.length; j++) {
                                        TObjectName columnName = columns[j];
                                        if ("*".equals(getColumnName(columnName))) {
                                            continue;
                                        }
                                        resultColumn.bindStarLinkColumn(columnName);
                                    }
                                }
                            }
                        }

                        analyzeResultColumn(column);
                    }
                }
            }

            if (stmt.getJoins() != null && stmt.getJoins().size() > 0) {
                for (int i = 0; i < stmt.getJoins().size(); i++) {
                    TJoin join = stmt.getJoins().getJoin(i);
                    if (join.getJoinItems() != null) {
                        for (int j = 0; j < join.getJoinItems().size(); j++) {
                            TJoinItem joinItem = join.getJoinItems()
                                        .getJoinItem(j);
                            TExpression expr = joinItem.getOnCondition();
                            if (expr != null) {
                                analyzeFilterCondtion(expr);
                            }
                        }
                    }
                }
            }

            if (stmt.getWhereClause() != null) {
                TExpression expr = stmt.getWhereClause().getCondition();
                if (expr != null) {
                    analyzeFilterCondtion(expr);
                }
            }

            if (stmt.getGroupByClause() != null) {
                TGroupByItemList groupByList = stmt.getGroupByClause()
                            .getItems();
                for (int i = 0; i < groupByList.size(); i++) {
                    TGroupByItem groupBy = groupByList.getGroupByItem(i);
                    TExpression expr = groupBy.getExpr();
                    analyzeAggregate(expr);
                }

                if (stmt.getGroupByClause().getHavingClause() != null) {
                    analyzeAggregate(stmt.getGroupByClause()
                                .getHavingClause());
                }
            }

            stmtStack.pop();
        }
    }

    private TSelectSqlStatement getParentSetSelectStmt(TSelectSqlStatement stmt) {
        TCustomSqlStatement parent = stmt.getParentStmt();
        if (parent == null) {
            return null;
        }
        if (parent.getStatements() != null) {
            for (int i = 0; i < parent.getStatements().size(); i++) {
                TCustomSqlStatement temp = parent.getStatements().get(i);
                if (temp instanceof TSelectSqlStatement) {
                    TSelectSqlStatement select = (TSelectSqlStatement) temp;
                    if (select.getLeftStmt() == stmt
                                || select.getRightStmt() == stmt) {
                        return select;
                    }
                }
            }
        }
        if (parent instanceof TSelectSqlStatement) {
            TSelectSqlStatement select = (TSelectSqlStatement) parent;
            if (select.getLeftStmt() == stmt
                        || select.getRightStmt() == stmt) {
                return select;
            }
        }
        return null;
    }

    private void createSelectSetResultColumns(SelectSetResultSet resultSet,
                TSelectSqlStatement stmt) {
        if (stmt.getSetOperatorType() != ESetOperatorType.none) {
            createSelectSetResultColumns(resultSet, stmt.getLeftStmt());
        } else {
            TResultColumnList columnList = stmt.getResultColumnList();
            for (int i = 0; i < columnList.size(); i++) {
                TResultColumn column = columnList.getResultColumn(i);
                ResultColumn resultColumn = ModelFactory.createSelectSetResultColumn(resultSet,
                            column);

                if (resultColumn.getColumnObject() instanceof TResultColumn) {
                    TResultColumn columnObject = (TResultColumn) resultColumn.getColumnObject();
                    if (columnObject.getFieldAttr() != null) {
                        if ("*".equals(getColumnName(columnObject.getFieldAttr()))) {
                            TObjectName fieldAttr = columnObject.getFieldAttr();
                            TTable sourceTable = fieldAttr.getSourceTable();
                            if (fieldAttr.getTableToken() != null
                                        && sourceTable != null) {
                                TObjectName[] columns = ModelBindingManager.getTableColumns(sourceTable);
                                for (int j = 0; j < columns.length; j++) {
                                    TObjectName columnName = columns[j];
                                    if ("*".equals(getColumnName(columnName))) {
                                        continue;
                                    }
                                    resultColumn.bindStarLinkColumn(columnName);
                                }
                            } else {
                                TTableList tables = stmt.getTables();
                                for (int k = 0; k < tables.size(); k++) {
                                    TTable tableElement = tables.getTable(k);
                                    TObjectName[] columns = ModelBindingManager.getTableColumns(tableElement);
                                    for (int j = 0; j < columns.length; j++) {
                                        TObjectName columnName = columns[j];
                                        if ("*".equals(getColumnName(columnName))) {
                                            continue;
                                        }
                                        resultColumn.bindStarLinkColumn(columnName);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void analyzeResultColumn(TResultColumn column) {
        TExpression expression = column.getExpr();
        columnsInExpr visitor = new columnsInExpr();
        expression.inOrderTraverse(visitor);
        List<TObjectName> objectNames = visitor.getObjectNames();

        List<TFunctionCall> functions = visitor.getFunctions();

        analyzeDataFlowRelation(column, objectNames);
        analyzeRecordSetRelation(column, functions);
    }

    private void analyzeRecordSetRelation(TResultColumn column,
                List<TFunctionCall> functions) {
        if (functions == null || functions.size() == 0) {
            return;
        }

        RecordSetRelation relation = ModelFactory.createRecordSetRelation();
        relation.setTarget(new ResultColumnRelationElement((ResultColumn) ModelBindingManager.getModel(column)));

        for (int i = 0; i < functions.size(); i++) {
            TFunctionCall function = functions.get(i);
            if (stmtStack.peek().getTables().size() == 1) {
                Object tableObject = ModelBindingManager.getModel(stmtStack.peek()
                            .getTables()
                            .getTable(0));
                if (tableObject instanceof Table) {
                    Table tableModel = (Table) tableObject;
                    relation.addSource(new TableRelationElement(tableModel));
                    relation.setAggregateFunction(function.getFunctionName()
                                .toString());
                } else if (tableObject instanceof QueryTable) {
                    QueryTable tableModel = (QueryTable) tableObject;
                    relation.addSource(new QueryTableRelationElement(tableModel));
                    relation.setAggregateFunction(function.getFunctionName()
                                .toString());
                }
            }
        }
    }

    private void analyzeDataFlowRelation(TParseTreeNode gspObject,
                List<TObjectName> objectNames) {
        Object columnObject = ModelBindingManager.getModel(gspObject);
        analyzeDataFlowRelation(columnObject, objectNames);
    }

    private void analyzeDataFlowRelation(Object modelObject,
                List<TObjectName> objectNames) {
        if (objectNames == null || objectNames.size() == 0) {
            return;
        }

        DataFlowRelation relation = ModelFactory.createDataFlowRelation();

        if (modelObject instanceof ResultColumn) {
            relation.setTarget(new ResultColumnRelationElement((ResultColumn) modelObject));
        } else if (modelObject instanceof TableColumn) {
            relation.setTarget(new TableColumnRelationElement((TableColumn) modelObject));
        } else if (modelObject instanceof ViewColumn) {
            relation.setTarget(new ViewColumnRelationElement((ViewColumn) modelObject));
        } else {
            throw new UnsupportedOperationException();
        }

        for (int i = 0; i < objectNames.size(); i++) {
            TObjectName columnName = objectNames.get(i);

            List<TTable> tables = new ArrayList<TTable>();
            {
                TCustomSqlStatement stmt = stmtStack.peek();

                TTable table = ModelBindingManager.getTable(stmt, columnName);

                if (table == null) {
                    if (columnName.getTableToken() != null
                                || !"*".equals(getColumnName(columnName))) {
                        table = columnName.getSourceTable();
                    }
                }

                if (table == null) {
                    if (stmt.tables != null) {
                        for (int j = 0; j < stmt.tables.size(); j++) {
                            if (table != null) {
                                break;
                            }

                            TTable tTable = stmt.tables.getTable(j);
                            if (tTable.getObjectNameReferences() != null
                                        && tTable.getObjectNameReferences().size() > 0) {
                                for (int z = 0; z < tTable.getObjectNameReferences()
                                            .size(); z++) {
                                    TObjectName refer = tTable.getObjectNameReferences()
                                                .getObjectName(z);
                                    if ("*".equals(getColumnName(refer))) {
                                        continue;
                                    }
                                    if (refer == columnName) {
                                        table = tTable;
                                        break;
                                    }
                                }
                            } else if (columnName.getTableToken() != null
                                        && (columnName.getTableToken().astext.equals(tTable.getName()) || columnName.getTableToken().astext.equals(tTable.getAliasName()))) {
                                table = tTable;
                                break;
                            }
                        }
                    }
                }

                if (table != null) {
                    tables.add(table);
                } else if (columnName.getTableToken() == null
                            && "*".equals(getColumnName(columnName))) {
                    if (stmt.tables != null) {
                        for (int j = 0; j < stmt.tables.size(); j++) {
                            tables.add(stmt.tables.getTable(j));
                        }
                    }
                }
            }

            for (int k = 0; k < tables.size(); k++) {
                TTable table = tables.get(k);
                if (table != null) {
                    if (ModelBindingManager.getModel(table) instanceof Table) {
                        Table tableModel = (Table) ModelBindingManager.getModel(table);
                        if (tableModel != null) {
                            if (getColumnName(columnName).equals("*")) {
                                TObjectName[] columns = ModelBindingManager.getTableColumns(table);
                                for (int j = 0; j < columns.length; j++) {
                                    TObjectName objectName = columns[j];
                                    if ("*".equals(getColumnName(objectName))) {
                                        continue;
                                    }
                                    TableColumn columnModel = ModelFactory.createTableColumn(tableModel,
                                                objectName);
                                    relation.addSource(new TableColumnRelationElement(columnModel));
                                }
                            } else {
                                TableColumn columnModel = ModelFactory.createTableColumn(tableModel,
                                            columnName);
                                relation.addSource(new TableColumnRelationElement(columnModel));
                            }
                        }
                    } else if (ModelBindingManager.getModel(table) instanceof QueryTable) {
                        QueryTable queryTable = (QueryTable) ModelBindingManager.getModel(table);
                        TSelectSqlStatement subquery = null;
                        if (queryTable.getTableObject().getCTE() != null) {
                            subquery = queryTable.getTableObject()
                                        .getCTE()
                                        .getSubquery();
                        } else {
                            subquery = queryTable.getTableObject()
                                        .getSubquery();
                        }

                        if (subquery != null
                                    && subquery.getSetOperatorType() != ESetOperatorType.none) {
                            SelectSetResultSet selectSetResultSetModel = (SelectSetResultSet) ModelBindingManager.getModel(subquery);
                            if (selectSetResultSetModel != null) {
                                for (int j = 0; j < selectSetResultSetModel.getColumns()
                                            .size(); j++) {
                                    ResultColumn sourceColumn = selectSetResultSetModel.getColumns()
                                                .get(j);
                                    if (sourceColumn.getName()
                                                .equals(getColumnName(columnName))) {
                                        ResultColumn targetColumn = ModelFactory.createSelectSetResultColumn(queryTable,
                                                    sourceColumn);
                                        relation.addSource(new ResultColumnRelationElement(targetColumn));
                                        break;
                                    }
                                }
                            }
                        } else {
                            List<ResultColumn> columns = queryTable.getColumns();
                            if (getColumnName(columnName).equals("*")) {
                                for (int j = 0; j < queryTable.getColumns()
                                            .size(); j++) {
                                    relation.addSource(new ResultColumnRelationElement(queryTable.getColumns()
                                                .get(j)));
                                }
                            } else {
                                if (table.getCTE() != null) {
                                    for (k = 0; k < columns.size(); k++) {
                                        ResultColumn column = columns.get(k);
                                        if (getColumnName(columnName).equals(column.getName())) {
                                            if (!column.equals(modelObject)) {
                                                relation.addSource(new ResultColumnRelationElement(column));
                                            }
                                            break;
                                        }
                                    }
                                } else if (table.getSubquery() != null) {
                                    if (columnName.getSourceColumn() != null) {
                                        Object model = ModelBindingManager.getModel(columnName.getSourceColumn());
                                        if (model instanceof ResultColumn) {
                                            ResultColumn resultColumn = (ResultColumn) model;
                                            relation.addSource(new ResultColumnRelationElement(resultColumn));
                                        }
                                    } else if (columnName.getSourceTable() != null) {
                                        Object tablModel = ModelBindingManager.getModel(columnName.getSourceTable());
                                        if (tablModel instanceof Table) {
                                            Object model = ModelBindingManager.getModel(new Pair<Table, TObjectName>((Table) tablModel,
                                                        columnName));
                                            if (model instanceof TableColumn) {
                                                relation.addSource(new TableColumnRelationElement((TableColumn) model));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void analyzeAggregate(TExpression expr) {
        if (expr == null) {
            return;
        }

        TCustomSqlStatement stmt = stmtStack.peek();

        columnsInExpr visitor = new columnsInExpr();
        expr.inOrderTraverse(visitor);
        List<TObjectName> objectNames = visitor.getObjectNames();

        TResultColumnList columns = stmt.getResultColumnList();
        for (int i = 0; i < columns.size(); i++) {
            TResultColumn column = columns.getResultColumn(i);
            AbstractRelation relation;
            if (isAggregateFunction(column.getExpr().getFunctionCall())) {
                relation = ModelFactory.createRecordSetRelation();
                relation.setTarget(new ResultColumnRelationElement((ResultColumn) ModelBindingManager.getModel(column)));
                ((RecordSetRelation) relation).setAggregateFunction(column.getExpr()
                            .getFunctionCall()
                            .getFunctionName()
                            .toString());
            } else {
                relation = ModelFactory.createImpactRelation();
                relation.setTarget(new ResultColumnRelationElement((ResultColumn) ModelBindingManager.getModel(column)));
            }

            for (int j = 0; j < objectNames.size(); j++) {
                TObjectName columnName = objectNames.get(j);

                TTable table = ModelBindingManager.getTable(stmt, columnName);
                if (table != null) {
                    if (ModelBindingManager.getModel(table) instanceof Table) {
                        Table tableModel = (Table) ModelBindingManager.getModel(table);
                        if (tableModel != null) {
                            TableColumn columnModel = ModelFactory.createTableColumn(tableModel,
                                        columnName);
                            relation.addSource(new TableColumnRelationElement(columnModel));
                        }
                    } else if (ModelBindingManager.getModel(table) instanceof QueryTable) {
                        ResultColumn resultColumn = (ResultColumn) ModelBindingManager.getModel(columnName.getSourceColumn());
                        if (resultColumn != null) {
                            relation.addSource(new ResultColumnRelationElement(resultColumn));
                        }
                    }
                }
            }
        }
    }

    private void analyzeFilterCondtion(TExpression expr) {
        if (expr == null) {
            return;
        }

        TCustomSqlStatement stmt = stmtStack.peek();

        columnsInExpr visitor = new columnsInExpr();
        expr.inOrderTraverse(visitor);
        List<TObjectName> objectNames = visitor.getObjectNames();

        TResultColumnList columns = stmt.getResultColumnList();
        if (columns != null) {
            for (int i = 0; i < columns.size(); i++) {
                TResultColumn column = columns.getResultColumn(i);

                AbstractRelation relation;
                if (isAggregateFunction(column.getExpr().getFunctionCall())) {
                    relation = ModelFactory.createRecordSetRelation();
                    relation.setTarget(new ResultColumnRelationElement((ResultColumn) ModelBindingManager.getModel(column)));
                    ((RecordSetRelation) relation).setAggregateFunction(column.getExpr()
                                .getFunctionCall()
                                .getFunctionName()
                                .toString());
                } else {
                    relation = ModelFactory.createImpactRelation();
                    relation.setTarget(new ResultColumnRelationElement((ResultColumn) ModelBindingManager.getModel(column)));
                }
                for (int j = 0; j < objectNames.size(); j++) {
                    TObjectName columnName = objectNames.get(j);

                    TTable table = ModelBindingManager.getTable(stmt,
                                columnName);
                    if (table != null) {
                        if (ModelBindingManager.getModel(table) instanceof Table) {
                            Table tableModel = (Table) ModelBindingManager.getModel(table);
                            if (tableModel != null) {
                                TableColumn columnModel = ModelFactory.createTableColumn(tableModel,
                                            columnName);
                                relation.addSource(new TableColumnRelationElement(columnModel));
                            }
                        } else if (ModelBindingManager.getModel(table) instanceof QueryTable) {
                            ResultColumn resultColumn = (ResultColumn) ModelBindingManager.getModel(columnName.getSourceColumn());
                            if (resultColumn != null) {
                                relation.addSource(new ResultColumnRelationElement(resultColumn));
                            }
                        }
                    }
                }
            }
        }
    }

    class columnsInExpr implements IExpressionVisitor {

        private List<TConstant> constants = new ArrayList<TConstant>();
        private List<TObjectName> objectNames = new ArrayList<TObjectName>();
        private List<TFunctionCall> functions = new ArrayList<TFunctionCall>();

        public List<TFunctionCall> getFunctions() {
            return functions;
        }

        public List<TConstant> getConstants() {
            return constants;
        }

        public List<TObjectName> getObjectNames() {
            return objectNames;
        }

        @Override
        public boolean exprVisit(TParseTreeNode pNode, boolean isLeafNode) {
            TExpression lcexpr = (TExpression) pNode;
            if (lcexpr.getExpressionType() == EExpressionType.simple_constant_t) {
                if (lcexpr.getConstantOperand() != null) {
                    constants.add(lcexpr.getConstantOperand());
                }
            } else if (lcexpr.getExpressionType() == EExpressionType.simple_object_name_t) {
                if (lcexpr.getObjectOperand() != null
                            && !isFunctionName(lcexpr.getObjectOperand())) {
                    objectNames.add(lcexpr.getObjectOperand());
                }
            } else if (lcexpr.getExpressionType() == EExpressionType.function_t) {
                TFunctionCall func = lcexpr.getFunctionCall();
                if (isAggregateFunction(func)) {
                    functions.add(func);
                }

                if (func.getArgs() != null) {
                    for (int k = 0; k < func.getArgs().size(); k++) {
                        TExpression expr = func.getArgs().getExpression(k);
                        if (expr != null) {
                            expr.inOrderTraverse(this);
                        }
                    }
                }
            } else if (lcexpr.getExpressionType() == EExpressionType.subquery_t) {
                TSelectSqlStatement select = lcexpr.getSubQuery();
                analyzeSelectStmt(select);
                inOrderTraverse(select, this);
            }
            return true;
        }

        private void inOrderTraverse(TSelectSqlStatement select,
                    columnsInExpr columnsInExpr) {
            if (select.getResultColumnList() != null) {
                for (int i = 0; i < select.getResultColumnList().size(); i++) {
                    select.getResultColumnList()
                                .getResultColumn(i)
                                .getExpr()
                                .inOrderTraverse(columnsInExpr);
                }
            } else if (select.getSetOperatorType() != ESetOperatorType.none) {
                inOrderTraverse(select.getLeftStmt(), columnsInExpr);
                inOrderTraverse(select.getRightStmt(), columnsInExpr);
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java DataFlowAnalyzer [/f <path_to_sql_file>] [/d <path_to_directory_includes_sql_files>] [/s [/text]] [/t <database type>] [/o <output file path>]");
            System.out.println("/f: Option, specify the sql file path to analyze dataflow relation.");
            System.out.println("/d: Option, specify the sql directory path to analyze dataflow relation.");
            System.out.println("/s: Option, simple output, ignore the intermediate results.");
            System.out.println("/text: Option, print the plain text format output.");
            System.out.println("/t: Option, set the database type. Support oracle, mysql, mssql, db2, netezza, teradata, informix, sybase, postgresql, hive, greenplum and redshift, the default type is oracle");
            System.out.println("/o: Option, write the output stream to the specified file.");
            System.out.println("/log: Option, generate a dataflow.log file to log information.");
            return;
        }

        File sqlFiles = null;

        List<String> argList = Arrays.asList(args);

        if (argList.indexOf("/f") != -1
                    && argList.size() > argList.indexOf("/f") + 1) {
            sqlFiles = new File(args[argList.indexOf("/f") + 1]);
            if (!sqlFiles.exists() || !sqlFiles.isFile()) {
                System.out.println(sqlFiles + " is not a valid file.");
                return;
            }
        } else if (argList.indexOf("/d") != -1
                    && argList.size() > argList.indexOf("/d") + 1) {
            sqlFiles = new File(args[argList.indexOf("/d") + 1]);
            if (!sqlFiles.exists() || !sqlFiles.isDirectory()) {
                System.out.println(sqlFiles + " is not a valid directory.");
                return;
            }
        } else {
            System.out.println("Please specify a sql file path or directory path to analyze dlineage.");
            return;
        }

        EDbVendor vendor = EDbVendor.dbvoracle;

        int index = argList.indexOf("/t");

        if (index != -1 && args.length > index + 1) {
            if (args[index + 1].equalsIgnoreCase("mssql")) {
                vendor = EDbVendor.dbvmssql;
            } else if (args[index + 1].equalsIgnoreCase("db2")) {
                vendor = EDbVendor.dbvdb2;
            } else if (args[index + 1].equalsIgnoreCase("mysql")) {
                vendor = EDbVendor.dbvmysql;
            } else if (args[index + 1].equalsIgnoreCase("netezza")) {
                vendor = EDbVendor.dbvnetezza;
            } else if (args[index + 1].equalsIgnoreCase("teradata")) {
                vendor = EDbVendor.dbvteradata;
            } else if (args[index + 1].equalsIgnoreCase("oracle")) {
                vendor = EDbVendor.dbvoracle;
            } else if (args[index + 1].equalsIgnoreCase("informix")) {
                vendor = EDbVendor.dbvinformix;
            } else if (args[index + 1].equalsIgnoreCase("sybase")) {
                vendor = EDbVendor.dbvsybase;
            } else if (args[index + 1].equalsIgnoreCase("postgresql")) {
                vendor = EDbVendor.dbvpostgresql;
            } else if (args[index + 1].equalsIgnoreCase("hive")) {
                vendor = EDbVendor.dbvhive;
            } else if (args[index + 1].equalsIgnoreCase("greenplum")) {
                vendor = EDbVendor.dbvgreenplum;
            } else if (args[index + 1].equalsIgnoreCase("redshift")) {
                vendor = EDbVendor.dbvredshift;
            }
        }

        String outputFile = null;

        index = argList.indexOf("/o");

        if (index != -1 && args.length > index + 1) {
            outputFile = args[index + 1];
        }

        FileOutputStream writer = null;
        if (outputFile != null) {
            try {
                writer = new FileOutputStream(outputFile);
                System.setOut(new PrintStream(writer));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        boolean simple = argList.indexOf("/s") != -1;
        boolean textFormat = false;
        if (simple) {
            textFormat = argList.indexOf("/text") != -1;
        }

        DataFlowAnalyzer dlineage = new DataFlowAnalyzer(sqlFiles,
                    vendor,
                    simple);
        if (simple) {
            dlineage.setTextFormat(textFormat);
        }

        StringBuffer errorBuffer = new StringBuffer();
        String result = dlineage.generateDataFlow(errorBuffer);

        if (result != null) {
            System.out.println(result);

            if (writer != null && result.length() < 1024 * 1024) {
                System.err.println(result);
            }
        }

        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        boolean log = argList.indexOf("/log") != -1;
        PrintStream pw = null;
        ByteArrayOutputStream sw = null;
        PrintStream systemSteam = System.err;

        try {
            sw = new ByteArrayOutputStream();
            pw = new PrintStream(sw);
            System.setErr(pw);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (errorBuffer.length() > 0) {
            System.err.println("Error log:\n" + errorBuffer);
        }

        if (sw != null) {
            String errorMessage = sw.toString().trim();
            if (errorMessage.length() > 0) {
                if (log) {
                    try {
                        pw = new PrintStream(new File(".", "dataflow.log"));
                        pw.print(errorMessage);
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                }

                System.setErr(systemSteam);
                System.err.println(errorMessage);
            }
        }
    }

    private void setTextFormat(boolean textFormat) {
        this.textFormat = textFormat;
    }

    public boolean isFunctionName(TObjectName object) {
        if (object == null || object.getGsqlparser() == null) {
            return false;
        }
        EDbVendor vendor = object.getGsqlparser().getDbVendor();
        if (vendor == EDbVendor.dbvteradata) {
            boolean result = TERADATA_BUILTIN_FUNCTIONS.contains(object.toString());
            if (result) {
                return true;
            }
        }

        List<String> versions = functionChecker.getAvailableDbVersions(vendor);
        if (versions != null && versions.size() > 0) {
            for (int i = 0; i < versions.size(); i++) {
                boolean result = functionChecker.isBuiltInFunction(object.toString(),
                            object.getGsqlparser().getDbVendor(),
                            versions.get(i));
                if (!result) {
                    return false;
                }
            }

            boolean result = TERADATA_BUILTIN_FUNCTIONS.contains(object.toString());
            if (result) {
                return true;
            }
        }

        return false;
    }

    public boolean isAggregateFunction(TFunctionCall func) {
        if (func == null) {
            return false;
        }
        return Arrays.asList(new String[]{
            "AVG",
            "COUNT",
            "MAX",
            "MIN",
            "SUM",
            "COLLECT",
            "CORR",
            "COVAR_POP",
            "COVAR_SAMP",
            "CUME_DIST",
            "DENSE_RANK",
            "FIRST",
            "GROUP_ID",
            "GROUPING",
            "GROUPING_ID",
            "LAST",
            "LISTAGG",
            "MEDIAN",
            "PERCENT_RANK",
            "PERCENTILE_CONT",
            "PERCENTILE_DISC",
            "RANK",
            "STATS_BINOMIAL_TEST",
            "STATS_CROSSTAB",
            "STATS_F_TEST",
            "STATS_KS_TEST",
            "STATS_MODE",
            "STATS_MW_TEST",
            "STATS_ONE_WAY_ANOVA",
            "STATS_WSR_TEST",
            "STDDEV",
            "STDDEV_POP",
            "STDDEV_SAMP",
            "SYS_XMLAGG",
            "VAR_ POP",
            "VAR_ SAMP",
            "VARI ANCE",
            "XMLAGG"
        }).contains(func.getFunctionName().toString());
    }

    public static String getanalyzeXmlfromString(String sqlFile, String dbVender) throws Exception {
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
            }  else if (dbVender.equalsIgnoreCase("oracle")) {
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

        //	EDbVendor vendor = EDbVendor.dbvoracle;
        DataFlowAnalyzer dlineage = new DataFlowAnalyzer(sqlFile, dbVendor, false);
        StringBuffer errorBuffer = new StringBuffer();

        String result = dlineage.generateDataFlow(errorBuffer);
        System.out.println(errorBuffer);

//	        System.out.println(result);
        xmlfiles.add(result);
//                FileUtils.write(new File("F:\\jars\\xmlfiles\\files.xml"), result);
        return result;
    }
}
