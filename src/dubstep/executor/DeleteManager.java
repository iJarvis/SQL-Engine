package dubstep.executor;

import dubstep.Main;
import dubstep.storage.DubTable;
import dubstep.utils.Evaluator;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static dubstep.planner.PlanTree.getSelectExprColumnStrList;

public class DeleteManager {

    public static void delete(FromItem fromItem, Expression filter) {
        Table table = (Table) fromItem;
        DubTable dubTable = Main.mySchema.getTable(table.getName());
        ScanNode scanNode = new ScanNode(fromItem, filter, Main.mySchema);
        scanNode.requiredList = new HashSet<>();
        scanNode.requiredList.addAll(getSelectExprColumnStrList(filter));
        scanNode.scanner.setupProjList(scanNode.requiredList);
        scanNode.projectionInfo = scanNode.scanTable.getColumnList1(scanNode.fromTable);
        Evaluator eval = new Evaluator(scanNode.projectionInfo);
        Tuple row = scanNode.getNextTuple();
        int i = 0;
        while (row != null) {
            eval.setTuple(row);
            try {
                PrimitiveValue value = eval.eval(filter);
                if (value!= null && value.toBool()) {
                    dubTable.deletedSet.add(row.tid);
                    long x = 1 << (i%64);
                    dubTable.isDeleted[i/64] |= x;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (NullPointerException e) {
                System.out.println("derp");
            }
            row = scanNode.getNextRow();
            ++i;
        }
    }

    public static boolean isDeleted(String tableName, int rowIdx) {
        return (Main.mySchema.getTable(tableName).isDeleted[rowIdx/64] & (1 << (rowIdx % 64))) != 0;
    }
}
