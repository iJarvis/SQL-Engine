package dubstep.planner;

import dubstep.executor.BaseNode;
import dubstep.executor.ScanNode;
import dubstep.storage.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;

import static dubstep.Main.mySchema;

public class PlanTree {

    public static BaseNode generatePlan(PlainSelect plainSelect) {
        String tableName = plainSelect.getFromItem().toString();
        Table table = mySchema.getTable(tableName);
        if (table == null) {
            //shouldn't we through an error here?
            throw new IllegalStateException("Table not found in our schema");
        }
        ScanNode scanNode = new ScanNode(tableName, null, mySchema);
        return scanNode;
    }

    public BaseNode optimizePlan(BaseNode generatedPlan) {
        BaseNode optimizedPlan = null;
        return optimizedPlan;
    }
}
