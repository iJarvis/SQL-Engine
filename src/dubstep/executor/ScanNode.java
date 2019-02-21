package dubstep.executor;

import dubstep.storage.DubTable;
import dubstep.storage.TableManager;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;

public class ScanNode extends BaseNode {

    DubTable scanTable;
    ArrayList<Tuple> tupleBuffer;
    Expression filter;
    int currentIndex = 0;
    Boolean ReadComplete = false;

    public ScanNode(String tableName, Expression filter, TableManager mySchema) {
        super();
        this.scanTable = mySchema.getTable(tableName);
        this.filter = filter;
        this.outerNode = null;
        this.innerNode = null;
        tupleBuffer = new ArrayList<>();
        scanTable.initRead();
        ReadComplete = scanTable.readTuples(20, tupleBuffer);
        this.initProjectionInfo();
    }

    @Override
    Tuple getNextRow() {
        if (tupleBuffer.size() < currentIndex + 1) {
            if (!ReadComplete) {
                ReadComplete = scanTable.readTuples(20, tupleBuffer);
                currentIndex = 0;
                return tupleBuffer.get(currentIndex++);
            } else
                return null;
        } else
            return tupleBuffer.get(currentIndex++);
    }

    @Override
    void resetIterator() {
        currentIndex = 0;
        if (ReadComplete) {
            ReadComplete = false;
            scanTable.initRead();
        } else {
            this.scanTable.resetRead();
        }
    }

    @Override
    void initProjectionInfo() {
        this.projectionInfo = scanTable.GetColumnList();
    }
}
