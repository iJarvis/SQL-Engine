package dubstep.executor;

import com.sun.org.apache.xpath.internal.operations.Bool;
import dubstep.Main;
import dubstep.storage.DubTable;
import dubstep.utils.Evaluator;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
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

        if(preserveRows == false &&  filter instanceof EqualsTo)
        {
            if(((BinaryExpression)filter).getLeftExpression() instanceof Column)
            {
               Column column = (Column) ((BinaryExpression)filter).getLeftExpression();
               if(dubTable.getColumnList1(scanNode.fromTable).get(column.getWholeColumnName()) == 0) {

                   PrimitiveValue val = (PrimitiveValue)((BinaryExpression) filter).getRightExpression();

                   ArrayList<Integer> indexList = new ArrayList<>();
                   try {
                       int Index = Collections.binarySearch(dubTable.primaryIndex,val.toLong());

                       if(Index >=0 )
                       {
                           while (Index >=0 && dubTable.primaryIndex.get(Index) == val.toLong())
                           {
                               Index--;
                           }
                           Index = Index+1;
                           while (dubTable.primaryIndex.size()  > Index && dubTable.primaryIndex.get(Index) == val.toLong())
                           {
                               indexList.add(Index);
                               Index++;
                           }

                       }
                   } catch (PrimitiveValue.InvalidPrimitive throwables) {
                       throwables.printStackTrace();
                   }

                   dubTable.deletedSet.addAll(indexList);
               return;
               }

            }
        }
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
