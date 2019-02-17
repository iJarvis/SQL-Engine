package dubstep.executor;

import com.sun.istack.internal.Nullable;
import dubstep.storage.Table;
import dubstep.storage.TableManager;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;

public class ScanNode extends BaseNode {

    Table scanTable;
    ArrayList<Tuple> tupleBuffer;
    Expression filter;
    int currentIndex = -1;

    public ScanNode(String tableName, Expression filter, TableManager mySchema) {
        this.scanTable = mySchema.getTable(tableName);
        this.filter = filter;
        this.outerNode = null;
        this.innerNode = null;
        tupleBuffer = new ArrayList<>();
        scanTable.initRead();
        ArrayList<Tuple> tupleBuffer = new ArrayList<Tuple>();
        scanTable.readTuples(20, tupleBuffer);
    }

    @Nullable
    @Override
    Tuple getNextRow() {
        if (tupleBuffer.size() <= currentIndex+1) {
            return null;
        }
        return tupleBuffer.get(++currentIndex);
    }

    @Override
    void resetIterator() {
        currentIndex = -1;
    }
}
