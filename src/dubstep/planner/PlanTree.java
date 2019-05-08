package dubstep.planner;

import dubstep.executor.*;
import dubstep.storage.DubTable;
import dubstep.storage.TableManager;
import dubstep.utils.Evaluator;
import dubstep.utils.GenerateAggregateNode;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.*;

import static dubstep.Main.mySchema;

public class PlanTree {

    private static final int ON_DISK_JOIN_THRESHOLD = 100;



    public static BaseNode generatePlan(PlainSelect plainSelect) {
        //Handle lowermost node
        FromItem fromItem = plainSelect.getFromItem();
        BaseNode scanRoot;
        if (fromItem instanceof SubSelect) {
            SelectBody selectBody = ((SubSelect) fromItem).getSelectBody();
            if (selectBody instanceof PlainSelect) {
                scanRoot = generatePlan((PlainSelect) selectBody);
            } else {
                throw new UnsupportedOperationException("Subselect is of type other than PlainSelect");
            }
        } else if (fromItem instanceof Table) {
            String tableName = ((Table) fromItem).getWholeTableName();
            DubTable table = mySchema.getTable(tableName);
            if (table == null) {
                throw new IllegalStateException("Table " + tableName + " not found in our schema");
            }
            BaseNode scanNode = new ScanNode(fromItem, null, mySchema);
            List<Join> joins = plainSelect.getJoins();
            scanRoot = generateJoin(scanNode, joins, mySchema);
        } else {
            throw new UnsupportedOperationException("We don't support this FROM clause");
        }

        //assuming there will always be a select node over our scan node
        Expression filter = plainSelect.getWhere();
        BaseNode projInnerNode;
        if (filter != null) {
            BaseNode selectNode = generateSelect(scanRoot, filter);
            projInnerNode = selectNode;
        } else
            projInnerNode = scanRoot;

        //handle projection
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        List<Column> groupColumns = plainSelect.getGroupByColumnReferences();
        GenerateAggregateNode genAgg = new GenerateAggregateNode(selectItems, projInnerNode);
        BaseNode projNode = genAgg.getAggregateNode();

        if (plainSelect.getDistinct() != null) {
            List<SelectExpressionItem> distinctItems = plainSelect.getDistinct().getOnSelectItems();
            if (distinctItems != null) {
                projNode = new DistinctNode(distinctItems, projNode);
            }
        }

        if (plainSelect.getOrderByElements() != null) {
            SortNode sortNode = new SortNode(plainSelect.getOrderByElements(), projNode);
            projNode = sortNode;
        }

        if(plainSelect.getLimit() != null)
        {
            projNode = new LimitNode(plainSelect.getLimit().getRowCount(), projNode);
        }
        return projNode;
    }

    private static BaseNode generateSelect(BaseNode lowerNode, Expression filter) {
        if (filter instanceof AndExpression) {
            BinaryExpression andFilter = (BinaryExpression) filter;
            lowerNode = generateSelect(lowerNode, andFilter.getLeftExpression());
            lowerNode = generateSelect(lowerNode, andFilter.getRightExpression());
        } else
            lowerNode = new SelectNode(filter, lowerNode);
        return lowerNode;
    }

    private static BaseNode generateJoin(BaseNode lowerNode, List<Join> Joins, TableManager mySchema) {
        if(Joins == null)
            return lowerNode;
        for (Join join : Joins) {
            BaseNode rightNode;
            if (join.getRightItem() instanceof Table) {
                rightNode = new ScanNode(join.getRightItem(), null, mySchema);
                lowerNode = new JoinNode(lowerNode, rightNode);
            } else {
                throw new IllegalStateException("Error in join - we expect only tables");
            }
        }
        return lowerNode;
    }

    public static BaseNode generateUnionPlan(Union union) {
        List<PlainSelect> plainSelects = union.getPlainSelects();
        return generateUnionPlanUtil(plainSelects);
    }

    private static BaseNode generateUnionPlanUtil(List<PlainSelect> plainSelects) {
        BaseNode root;
        if (plainSelects.size() == 2) {
            //base case
            root = new UnionNode(generatePlan(plainSelects.get(0)), generatePlan(plainSelects.get(1)));
        } else {
            //recur
            BaseNode leftNode = generatePlan(plainSelects.get(0));
            plainSelects.remove(0);
            BaseNode rightNode = generateUnionPlanUtil(plainSelects);
            root = new UnionNode(leftNode, rightNode);
        }
        return root;
    }

    private static BaseNode getResponsibleChild(BaseNode currentNode, List<Column> columnList) {
        if (currentNode instanceof UnionNode || currentNode instanceof ScanNode)
            return currentNode;
        if (currentNode.innerNode != null &&( currentNode instanceof JoinNode || currentNode instanceof HashJoinNode || currentNode instanceof SortMergeJoinNode) ){
            boolean inner = false, outer = false;

            for (Column column : columnList) {
                if (currentNode.innerNode.projectionInfo.containsKey(column.getWholeColumnName()))
                    inner = true;
                if (currentNode.outerNode.projectionInfo.containsKey(column.getWholeColumnName()))
                    outer = true;
            }

            if (inner == true && outer == true)
                return currentNode;
            else if (inner == true)
                return getResponsibleChild(currentNode.innerNode, columnList);
            else
                return getResponsibleChild(currentNode.outerNode, columnList);
        } else
            return getResponsibleChild(currentNode.innerNode, columnList);
    }

    public static List<Column> getSelectExprColumnList(Expression expression) {
        List<Column> columnList = new ArrayList<>();
        if (expression instanceof BinaryExpression) {
            BinaryExpression bin = (BinaryExpression) expression;
            if (bin.getRightExpression() instanceof Column)
                columnList.add((Column) bin.getRightExpression());
            else if(bin.getRightExpression() instanceof BinaryExpression)
                columnList.addAll(getSelectExprColumnList(bin.getRightExpression()));

            if (bin.getLeftExpression() instanceof Column)
                columnList.add((Column) bin.getLeftExpression());
            else if(bin.getLeftExpression() instanceof  BinaryExpression)
                columnList.addAll(getSelectExprColumnList(bin.getLeftExpression()));

            if(bin.getLeftExpression() instanceof Function)
            {
                Function fun = (Function) bin.getLeftExpression();
                if(fun.getName().equals("DATE"))
                bin.setLeftExpression( new LongValue( (DateValue.parseEscaped(fun.getParameters().getExpressions().get(0).toString())).getValue().getTime() ));

            }

            if(bin.getRightExpression() instanceof Function)
            {
                Function fun = (Function) bin.getRightExpression();
                if(fun.getName().equals("DATE"))
                bin.setRightExpression(new LongValue( (DateValue.parseEscaped(fun.getParameters().getExpressions().get(0).toString())).getValue().getTime() ));

            }
        }
        return columnList;
    }


    public static List<String> getSelectExprColumnStrList(Expression expression) {
        List<String> columnList = new ArrayList<>();
        if(expression instanceof  Column)
        {
            columnList.add(((Column)expression).getWholeColumnName());
        }
        if(expression instanceof Function)
        {
            Function function = (Function)expression;
            ExpressionList exprList= function.getParameters();
            if(exprList!= null) {
                List<Expression> exprs = exprList.getExpressions();

                for (Expression expression1 : exprs) {
                    columnList.addAll(getSelectExprColumnStrList(expression1));
                }
            }
        }
        if(expression instanceof CaseExpression) {
            CaseExpression c = (CaseExpression)expression;
            if (c.getSwitchExpression() == null) {
                Iterator var2 = c.getWhenClauses().iterator();

                while (var2.hasNext()) {
                    Object ow = var2.next();
                    WhenClause w = (WhenClause) ow;
                    columnList.addAll(getSelectExprColumnStrList(w.getWhenExpression()));
                }
            }
        }


        if (expression instanceof BinaryExpression) {
            BinaryExpression bin = (BinaryExpression) expression;
            if (bin.getRightExpression() instanceof Column)
                columnList.add(((Column) bin.getRightExpression()).getWholeColumnName());
            else if(bin.getRightExpression() instanceof BinaryExpression)
                columnList.addAll(getSelectExprColumnStrList(bin.getRightExpression()));

            if (bin.getLeftExpression() instanceof Column)
                columnList.add(((Column) bin.getLeftExpression()).getWholeColumnName());
            else if(bin.getLeftExpression() instanceof  BinaryExpression)
                columnList.addAll(getSelectExprColumnStrList(bin.getLeftExpression()));
        }
        return columnList;
    }

    private static void selectPushDown(SelectNode selectNode) {
        List<Column> columnList = getSelectExprColumnList(selectNode.filter);
        BaseNode newNode = getResponsibleChild(selectNode, columnList);
        if (newNode == selectNode)
            return;
        else {
            BaseNode parent = selectNode.parentNode;
            BaseNode child = selectNode.innerNode;
            parent.innerNode = child;
            child.parentNode = parent;

            BaseNode newParent = newNode.parentNode;
            if(newNode.isInner) {
                newParent.innerNode = selectNode;
                selectNode.isInner = true;
            }
            else {
                newParent.outerNode = selectNode;
                selectNode.isInner = false;
            }

            selectNode.parentNode = newParent;
            selectNode.innerNode = newNode;
            newNode.isInner = true;
            newNode.parentNode = selectNode;
            selectNode.projectionInfo = selectNode.innerNode.projectionInfo;
            selectNode.eval = new Evaluator(selectNode.projectionInfo);
        }
    }

    private static BaseNode getResponsibleJoinChild(BaseNode currentNode, List<Column> columnList) {
        if (currentNode instanceof JoinNode || currentNode instanceof HashJoinNode || currentNode instanceof SortMergeJoinNode) {
            boolean inner = false, outer = false;
            for (Column column : columnList) {
                if (currentNode.innerNode.projectionInfo.containsKey(column.getWholeColumnName()))
                    inner = true;
                if (currentNode.outerNode.projectionInfo.containsKey(column.getWholeColumnName()))
                    outer = true;
            }
            if (inner && outer ) {
                if(currentNode instanceof JoinNode)
                return currentNode;
                else
                    return null;
            } else {
                BaseNode leftChild = getResponsibleJoinChild(currentNode.innerNode, columnList);
                if (leftChild != null) {
                    return leftChild;
                }
                BaseNode rightChild = getResponsibleJoinChild(currentNode.outerNode, columnList);
                if (rightChild != null) {
                    return rightChild;
                }
            }
        } else if (currentNode instanceof SelectNode) {
            return getResponsibleJoinChild(currentNode.innerNode, columnList);
        }
        return null;
    }

    private static void convertJoins(SelectNode selectNode) {
        List<Column> columnList = getSelectExprColumnList(selectNode.filter);
        if (columnList.size() != 2) {
            return;
        }
        BaseNode joinNode = getResponsibleJoinChild(selectNode, columnList);
        if (joinNode != null) {
            BaseNode selectParent = selectNode.parentNode;
            BaseNode selectChild = selectNode.innerNode;
            selectParent.innerNode = selectChild;
            selectChild.parentNode = selectParent;

            BaseNode joinParent = joinNode.parentNode;
            BaseNode joinInnerChild = joinNode.innerNode;
            BaseNode joinOuterChild = joinNode.outerNode;

            BaseNode newJoinNode;
            Column column1, column2;
            if (joinInnerChild.projectionInfo.containsKey(columnList.get(0).getWholeColumnName()) && joinOuterChild.projectionInfo.containsKey(columnList.get(1).getWholeColumnName())) {
                column1 = columnList.get(0);
                column2 = columnList.get(1);
            } else if (joinOuterChild.projectionInfo.containsKey(columnList.get(0).getWholeColumnName()) && joinInnerChild.projectionInfo.containsKey(columnList.get(1).getWholeColumnName())) {
                column2 = columnList.get(0);
                column1 = columnList.get(1);
            }
            else
                {
                    throw  new UnsupportedOperationException("join condition invalid");
                }
            if(mySchema.isInMem()) {
                EqualsTo filter = new EqualsTo();
                filter.setLeftExpression(column1);
                filter.setRightExpression(column2);
                newJoinNode = new HashJoinNode(joinInnerChild,joinOuterChild,filter);
            }
            else
                newJoinNode = new SortMergeJoinNode(joinInnerChild, joinOuterChild, column1, column2);
            newJoinNode.parentNode = joinParent;
            if (joinParent.innerNode == joinNode) {
                joinParent.innerNode = newJoinNode;
            } else {
                joinParent.outerNode = newJoinNode;
            }

            if(mySchema.isInMem())
            {
                newJoinNode.innerNode = joinInnerChild;
                newJoinNode.outerNode = joinOuterChild;
                return;
            }

            //add sort nodes
            OrderByElement innerOrderByElement = new OrderByElement();
            innerOrderByElement.setAsc(true);
            innerOrderByElement.setExpression(column1);
            BaseNode innerSortNode = new SortNode(new ArrayList<>(Arrays.asList(innerOrderByElement)), joinInnerChild);

            OrderByElement outerOrderByElement = new OrderByElement();
            outerOrderByElement.setAsc(true);
            outerOrderByElement.setExpression(column2);
            BaseNode outerSortNode = new SortNode(new ArrayList<>(Arrays.asList(outerOrderByElement)), joinOuterChild);

            newJoinNode.innerNode = innerSortNode;
            newJoinNode.outerNode = outerSortNode;
        }
    }

    public static BaseNode optimizePlan(BaseNode currentNode) {
        BaseNode optimizedPlan = currentNode;
        if (currentNode == null)
            return optimizedPlan;

        BaseNode inner = currentNode.innerNode;
        BaseNode outer = currentNode.outerNode;

        if (currentNode instanceof SelectNode &&((SelectNode)(SelectNode) currentNode).isOptimized == false ) {
            selectPushDown((SelectNode) currentNode);
            ((SelectNode)(SelectNode) currentNode).isOptimized = true;
            convertJoins((SelectNode) currentNode);

        }
        optimizePlan(inner);
        optimizePlan(outer);

        return optimizedPlan;
    }
}
