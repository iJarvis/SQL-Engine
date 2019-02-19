package dubstep.planner;

import dubstep.executor.BaseNode;
import dubstep.executor.ProjNode;
import dubstep.executor.ScanNode;
import dubstep.executor.SelectNode;
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
        BaseNode scanNode;
        if (fromItem instanceof SubSelect) {
            SelectBody selectBody = ((SubSelect) fromItem).getSelectBody();
            if (selectBody instanceof PlainSelect) {
                scanNode = generatePlan((PlainSelect) selectBody);
            } else {
                throw new UnsupportedOperationException("Subselect is of type other than PlainSelect");
            }
        } else if (fromItem instanceof Table) {
            String tableName = fromItem.toString();
            DubTable table = mySchema.getTable(tableName);
            if (table == null) {
                //shouldn't we through an error here?
                throw new IllegalStateException("DubTable not found in our schema");
            }
            scanNode = new ScanNode(tableName, null, mySchema);
        } else {
            throw new UnsupportedOperationException("We don't support this FROM clause");
        }

        //assuming there will always be a select node over our scan node
        Expression filter = plainSelect.getWhere();
        BaseNode selectNode = new SelectNode(filter, scanNode);
        selectNode.innerNode = scanNode;

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
