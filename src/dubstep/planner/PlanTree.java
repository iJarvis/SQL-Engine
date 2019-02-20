package dubstep.planner;

import dubstep.executor.*;
import dubstep.storage.DubTable;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.ArrayList;
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
                JoinNode joinNode = new JoinNode();
                joinNode.innerNode = scanNode;
                joinNode.outerNode = scanNode2;
                scanRoot = joinNode;
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

    public BaseNode optimizePlan(BaseNode generatedPlan) {
        BaseNode optimizedPlan = null;
        return optimizedPlan;
    }
}
