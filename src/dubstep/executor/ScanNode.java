package dubstep.executor;

import dubstep.storage.DubTable;
import dubstep.storage.TableManager;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;

import java.util.ArrayList;

public class ScanNode extends BaseNode {

    private DubTable scanTable;
    private ArrayList<Tuple> tupleBuffer;
    private Expression filter;
    private int currentIndex = 0;
    private boolean readComplete = false;
    private Table fromTable;

    public ScanNode(FromItem fromItem, Expression filter, TableManager mySchema) {
        super();
        if (!(fromItem instanceof Table)) {
            throw new UnsupportedOperationException("Scan node call without table");
        }
        this.fromTable = (Table) fromItem;
        String tableName = fromTable.getName();
        this.scanTable = mySchema.getTable(tableName);
        this.filter = filter;
        this.outerNode = null;
        this.innerNode = null;
        tupleBuffer = new ArrayList<>();
        scanTable.initRead();
        readComplete = scanTable.readTuples(20, tupleBuffer);
        this.initProjectionInfo();

    }

    @Override
    Tuple getNextRow() {
        while (1 == 1) {
            if (tupleBuffer.size() < currentIndex + 1) {
                if (!readComplete) {
                    readComplete = scanTable.readTuples(20, tupleBuffer);
                    currentIndex = 0;
                    continue;
                } else
                    return null;
            } else
                return tupleBuffer.get(currentIndex++);
        }
    }

    @Override
    void resetIterator() {
        currentIndex = 0;
        if (readComplete) {
            readComplete = false;
            scanTable.initRead();
        } else {
            this.scanTable.resetRead();
        }
    }

    @Override
    void initProjectionInfo() {
        this.projectionInfo = scanTable.GetColumnList(fromTable);
    }
}
