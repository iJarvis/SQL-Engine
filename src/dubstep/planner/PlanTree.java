package dubstep.planner;

import dubstep.executor.*;
import dubstep.storage.DubTable;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.List;

import static dubstep.Main.mySchema;

public class PlanTree {

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
            String tableName = fromItem.toString();
            DubTable table = mySchema.getTable(tableName);
            if (table == null) {
                throw new IllegalStateException("DubTable not found in our schema");
            }
            BaseNode scanNode = new ScanNode(tableName, null, mySchema);
            List<Join> joins = plainSelect.getJoins();
            if (joins != null) {
                String table2Name = joins.get(0).toString();
                if (mySchema.getTable(table2Name) == null) {
                    throw new IllegalStateException("Table not found");
                }
                BaseNode scanNode2 = new ScanNode(table2Name, null, mySchema);
                JoinNode joinNode = new JoinNode(scanNode,scanNode2);

                if (joins.size() == 1) {
                    scanRoot = joinNode;
                } else {
                    //handle 3 table join
                    String table3Name = joins.get(1).toString();
                    if (mySchema.getTable(table3Name) == null) {
                        throw new IllegalStateException("Table not found");
                    }
                    BaseNode scanNode3 = new ScanNode(table3Name, null, mySchema);
                    JoinNode joinNode2 = new JoinNode(scanNode3, joinNode);
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
        BaseNode selectNode = new SelectNode(filter, scanRoot);
        selectNode.innerNode = scanRoot;

        //handle projection
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        BaseNode projNode = new ProjNode(selectItems, selectNode);

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

    public BaseNode optimizePlan(BaseNode generatedPlan) {
        BaseNode optimizedPlan = null;
        return optimizedPlan;
    }
}
