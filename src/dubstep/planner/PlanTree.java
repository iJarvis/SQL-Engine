package dubstep.planner;

import dubstep.executor.*;
import dubstep.storage.DubTable;
import dubstep.utils.GenerateAggregateNode;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.ArrayList;
import java.util.List;

import static dubstep.Main.mySchema;

public class PlanTree {

    private static BaseNode generateSelect(BaseNode lowerNode, Expression filter)
    {
        if(filter instanceof AndExpression)
        {
            BinaryExpression andFilter = (BinaryExpression) filter;
            lowerNode = generateSelect(lowerNode,andFilter.getLeftExpression());
            lowerNode = generateSelect(lowerNode,andFilter.getRightExpression());
        }
        else
            lowerNode = new SelectNode(filter,lowerNode);
        return lowerNode;
    }

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
            if (joins != null) {
                FromItem table2 = joins.get(0).getRightItem();
                mySchema.checkTableExists(table2);
                Expression joinFilter1 = joins.get(0).getOnExpression();
                BaseNode scanNode2 = new ScanNode(table2, null, mySchema);
                BaseNode joinNode;
                if (joinFilter1 != null && joinFilter1 instanceof EqualsTo) {
                    joinNode = new HashJoinNode(scanNode, scanNode2, joinFilter1);
                } else {
                    joinNode = new JoinNode(scanNode, scanNode2);
                }
                if (joins.size() == 1) {
                    scanRoot = joinNode;
                } else {
                    //handle 3 table join
                    FromItem table3 = joins.get(1).getRightItem();
                    Expression joinFilter2 = joins.get(1).getOnExpression();
                    if (mySchema.getTable(table3.toString()) == null) {
                        throw new IllegalStateException("Table not found");
                    }
                    BaseNode scanNode3 = new ScanNode(table3, null, mySchema);
                    BaseNode joinNode2;
                    if (joinFilter2 != null && joinFilter2 instanceof EqualsTo) {
                        joinNode2 = new HashJoinNode(scanNode3, joinNode, joinFilter2);
                    } else {
                        joinNode2 = new JoinNode(scanNode3, joinNode);
                    }
                    scanRoot = joinNode2;
                }
            } else {
                scanRoot = scanNode;
            }
        } else {
            throw new UnsupportedOperationException("We don't support this FROM clause");
        }

        //assuming there will always be a select node over our scan node
        Expression filter = plainSelect.getWhere();
        BaseNode projInnerNode;
        if(filter != null) {
            BaseNode selectNode = generateSelect(scanRoot,filter);

            projInnerNode = selectNode;
        }
        else
            projInnerNode = scanRoot;
        //handle order by
        if (plainSelect.getOrderByElements() != null) {
            SortNode sortNode = new SortNode(plainSelect.getOrderByElements(), projInnerNode);
            projInnerNode = sortNode;
        }

        //handle projection
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        GenerateAggregateNode genAgg = new GenerateAggregateNode(selectItems, projInnerNode);
        BaseNode projNode = genAgg.getAggregateNode();

        return projNode;
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

    private static BaseNode getResponsibleChild(BaseNode currentNode, List<String> columnList) {
        if (currentNode instanceof UnionNode || currentNode instanceof ScanNode)
            return currentNode;
        if (currentNode.innerNode != null && currentNode instanceof JoinNode) {
            boolean inner = false, outer = false;

            for (String column : columnList) {
                if (currentNode.innerNode.projectionInfo.contains(column))
                    inner = true;
                if (currentNode.outerNode.projectionInfo.contains(column))
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

    private static void selectPushDown(SelectNode selectNode) {
        Expression expression = selectNode.filter;
        List<String> columnList = new ArrayList<>();
        if (expression instanceof BinaryExpression) {
            BinaryExpression bin = (BinaryExpression) expression;
            if (bin.getRightExpression() instanceof Column)
                columnList.add(((Column) bin.getRightExpression()).getWholeColumnName());

            if (bin.getLeftExpression() instanceof Column)
                columnList.add(((Column) bin.getLeftExpression()).getWholeColumnName());
        }
        BaseNode newNode = getResponsibleChild(selectNode, columnList);
        if (newNode == selectNode)
            return;
        else {
            BaseNode parent = selectNode.parentNode;
            BaseNode child = selectNode.innerNode;
            parent.innerNode = child;
            child.parentNode = parent;

            BaseNode newParent = newNode.parentNode;
            newParent.innerNode = selectNode;
            selectNode.parentNode = newParent;
            selectNode.innerNode = newNode;
            newNode.parentNode = selectNode;
            selectNode.projectionInfo = selectNode.innerNode.projectionInfo;

        }
    }

    public static BaseNode optimizePlan(BaseNode currentNode) {
        BaseNode optimizedPlan = currentNode;
        if (currentNode == null)
            return optimizedPlan;

        BaseNode inner = currentNode.innerNode;
        BaseNode outer = currentNode.outerNode;

        if (currentNode instanceof SelectNode) {
            selectPushDown((SelectNode) currentNode);
        }
        optimizePlan(inner);
        optimizePlan(outer);

        return optimizedPlan;
    }
}
