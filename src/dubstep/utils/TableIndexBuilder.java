package dubstep.utils;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;

import java.io.File;
import java.util.List;

public class TableSplitter {

    private static File splitDir = new File("split");

    public static void split(Table table) {
        File outputDir = new File(splitDir, table.getName());
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }
        
    }
}
