package dubstep.executor;

import dubstep.storage.DubTable;
import dubstep.storage.Scanner;
import dubstep.storage.TableManager;
import dubstep.utils.QueryTimer;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;

import java.nio.file.Path;
import java.util.ArrayList;

public class ScanNode extends BaseNode {

    private DubTable scanTable;
    private ArrayList<Tuple> tupleBuffer;
    private Expression filter;
    private int currentIndex = 0;
    private boolean readComplete = false;
    private Table fromTable;
    private Scanner scanner;
    private TableManager mySchema;
    public QueryTimer parsetimer;

    public ScanNode(FromItem fromItem, Expression filter, TableManager mySchema) {
        super();
        if (!(fromItem instanceof Table)) {
            throw new UnsupportedOperationException("Scan node call without table");
        }
        parsetimer = new QueryTimer();
        this.mySchema = mySchema;
        this.fromTable = (Table) fromItem;
        String tableName = fromTable.getName();
        this.scanTable = mySchema.getTable(tableName);
        this.filter = filter;
        this.outerNode = null;
        this.innerNode = null;
        tupleBuffer = new ArrayList<>();
        scanner = new Scanner(this.scanTable);
        scanner.initRead();
        readComplete = scanner.readTuples(10000, tupleBuffer, parsetimer);
        this.initProjectionInfo();
    }

    public String getScanTableName(){
        return this.scanTable.GetTableName();
    }

    @Override
    Tuple getNextRow() {
        while (1 == 1) {
            if (tupleBuffer.size() <= currentIndex) {
                if (!readComplete) {
                    if(mySchema.isInMem())
                        readComplete = scanner.readTuples(7000, tupleBuffer,this.parsetimer );
                    else
                        readComplete = scanner.readTuples(7000, tupleBuffer,parsetimer);

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
            scanner.initRead();
            tupleBuffer.clear();
        } else {
            this.scanner.resetRead();
        }
    }

    @Override
    void initProjectionInfo() {
        projectionInfo = scanTable.getColumnList(fromTable);
    }
}
