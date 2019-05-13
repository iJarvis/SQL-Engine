package dubstep.executor;

import com.sun.org.apache.xpath.internal.operations.Bool;
import dubstep.Main;
import dubstep.storage.DubTable;
import dubstep.utils.Evaluator;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;

import java.sql.SQLException;
import java.util.*;

import static dubstep.planner.PlanTree.getSelectExprColumnStrList;

public class DeleteManager {
    public ArrayList<Tuple> updated_tuples;

    public void delete(FromItem fromItem, Expression filter, List<Column> columnList, Boolean preserveRows) {
        Table table = (Table) fromItem;
        DubTable dubTable = Main.mySchema.getTable(table.getName());
        ScanNode scanNode = new ScanNode(fromItem, filter, Main.mySchema);
        scanNode.requiredList = new HashSet<>();
        if(preserveRows) {
            scanNode.requiredList.addAll(getSelectExprColumnStrList(filter));
            for(Column column : columnList)
            {
                scanNode.requiredList.add(dubTable.GetTableName()+"."+column.getColumnName());
            }
        }
        
        else
            scanNode.requiredList =  new HashSet<>(getSelectExprColumnStrList(filter));
        scanNode.scanner.setupProjList(scanNode.requiredList);
        scanNode.projectionInfo = scanNode.scanTable.getColumnList1(scanNode.fromTable);
        Evaluator eval = new Evaluator(scanNode.projectionInfo);
        updated_tuples = new ArrayList<>();
        Tuple row = scanNode.getNextTuple();
        int i = 0;
        while (row != null) {
            eval.setTuple(row);
            try {
                PrimitiveValue value = eval.eval(filter);
                if (value!= null && value.toBool() && !preserveRows) {
                    dubTable.deletedSet.add(row.tid);
                    if(preserveRows)
                        updated_tuples.add(row);
                    break;
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

}
