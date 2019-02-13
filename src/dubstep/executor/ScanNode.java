package dubstep.executor;

import dubstep.storage.Table;
import dubstep.storage.TableManager;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;

public class ScanNode extends BaseNode {
    Table scanTable;
    ArrayList<Tuple> tupleBuffer;
    Expression filter;



    ScanNode(String tableName, Expression filter, TableManager mySchema) {

        this.scanTable = mySchema.getTable(tableName);
        this.filter = filter;
        this.outerNode = null;
        this.innerNode = null;
        tupleBuffer = new ArrayList<>();
        scanTable.initRead();

    }


    @Override
    public Tuple GetNextRow() {

        return null;

    }

    @Override
    void ResetIterator() {

    }
}
