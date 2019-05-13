package dubstep.executor;

import dubstep.Main;
import dubstep.storage.DubTable;
import dubstep.storage.Scanner;
import dubstep.storage.TableManager;
import dubstep.utils.QueryTimer;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;

import java.util.ArrayList;

public class ScanNode extends BaseNode {

    public QueryTimer parsetimer;
    private DubTable scanTable;
    private ArrayList<Tuple> tupleBuffer;
    private Expression filter;
    private int currentIndex = 0;
    private int i = 0;
    private boolean readComplete = false;
    private Table fromTable;
    private Scanner scanner;
    private TableManager mySchema;
    private boolean isInit = false;

    public ScanNode(FromItem fromItem, Expression filter, TableManager mySchema) {
        this((Table) fromItem, filter);
    }

    public ScanNode(Table table, Expression filter) {
        super();
        parsetimer = new QueryTimer();
        this.mySchema = Main.mySchema;
        this.fromTable = table;
        String tableName = fromTable.getName();
        this.scanTable = mySchema.getTable(tableName);
        this.filter = filter;
        this.outerNode = null;
        this.innerNode = null;
        tupleBuffer = new ArrayList<>();
        scanner = new Scanner(this.scanTable);
        scanner.initRead();
        //  readComplete = scanner.readTuples(15000, tupleBuffer, parsetimer);
        this.initProjectionInfo();
    }

    public String getScanTableName() {
        return this.scanTable.GetTableName();
    }

    @Override
    Tuple getNextRow() {
        if (!isInit) {
            readComplete = scanner.readTuples(80, tupleBuffer, parsetimer);
            isInit = true;
        }

        while (1 == 1) {
            ++i;
            if (tupleBuffer.size() <= currentIndex) {
                if (!readComplete) {
                    if (mySchema.isInMem())
                        readComplete = scanner.readTuples(8000, tupleBuffer, this.parsetimer);
                    else
                        readComplete = scanner.readTuples(8000, tupleBuffer, parsetimer);

                    currentIndex = 0;
                    continue;
                } else
                    return null;
            }
//            else if (DeleteManager.isDeleted(fromTable.getName(), i-1)) {
//                ++currentIndex;
//            }
//
            else {
                return tupleBuffer.get(currentIndex++);
            }
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
        this.typeList = scanTable.typeList;
    }

    @Override
    public void initProjPushDownInfo() {
        this.requiredList = this.parentNode.requiredList;
        scanner.setupProjList(this.requiredList);
    }
}
