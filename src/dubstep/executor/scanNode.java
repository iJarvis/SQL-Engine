package dubstep.executor;

import dubstep.storage.table;
import dubstep.storage.tableManager;
import dubstep.utils.tuple;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;

import static dubstep.Main.scanBufferSize;

public class scanNode extends baseNode {
    table scanTable;
    ArrayList<tuple> tupleBuffer;
    Expression filter;



    scanNode(String tableName, Expression filter, tableManager mySchema) {

        this.scanTable = mySchema.getTable(tableName);
        this.filter = filter;
        this.outerNode = null;
        this.innerNode = null;
        tupleBuffer = new ArrayList<>();
        scanTable.initRead();

    }


    @Override
    public tuple GetNextRow() {

        return null;

    }

    @Override
    void ResetIterator() {

    }
}
