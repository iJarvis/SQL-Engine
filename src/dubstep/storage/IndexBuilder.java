package dubstep.storage;

import dubstep.executor.ScanNode;
import dubstep.executor.SortNode;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static dubstep.Main.mySchema;

public class IndexBuilder {

    private List<Index> indexes;
    private Table table;

    private static final String PRIMARY_INDEX = "PRIMARY KEY";
    private static final String SORTED_DIR = "data/sorted";

    public IndexBuilder(Table table, List<Index> indexes) {
        this.indexes = indexes;
        this.table = table;
    }

    public void build() {
        for (int i = 0; i < indexes.size(); ++i) {
            Index index = indexes.get(i);
            if (index.getType().equals(PRIMARY_INDEX)) {
                //sort the table as well
                try {
                    sortTable(index);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void sortTable(Index primaryIndex) throws IOException {
        File sortedDir = new File(SORTED_DIR);
        if (!sortedDir.exists()) {
            sortedDir.mkdirs();
        }
        File tableFile = new File(sortedDir, table.getName() + ".csv");
        if (tableFile.exists()) {
            tableFile.delete();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(tableFile));
        List<String> primaryColumns = primaryIndex.getColumnsNames();
        List<OrderByElement> orderByElements = new ArrayList<>();
        for (String primaryColumn: primaryColumns) {
            OrderByElement orderByElement = new OrderByElement();
            Column c = new Column(table, primaryColumn);
            orderByElement.setExpression(c);
            orderByElements.add(orderByElement);
        }
        ScanNode scanNode = new ScanNode(table, null, mySchema);
        SortNode sortNode = new SortNode(orderByElements, scanNode);
        Tuple nextTuple = sortNode.getNextTuple();
        while (nextTuple != null) {
            writer.write(nextTuple.toString() + "\n");
            nextTuple = sortNode.getNextTuple();
        }
        writer.close();
        mySchema.getTable(table.getName()).setDataFile(tableFile.getPath());
    }
}
