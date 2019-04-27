package dubstep.utils;

import dubstep.Main;
import dubstep.storage.DubTable;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Utils {

    public static int getRandomNumber(int min, int max) {
        return (int) (Math.random() * ((max - min) + 1)) + min;
    }

    public static void mapPutAll(Map<String, Integer> source, Map<String, Integer> target) {
        int diff = target.size();
        for (Map.Entry<String, Integer> e : source.entrySet()) {
            target.put(e.getKey(), e.getValue() + diff);
        }
    }

    public static void deleteDir(File file) {
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                deleteDir(f);
            }
        }
        file.delete();
    }

    public static boolean isSortNeeded(FromItem fromItem, List<OrderByElement> orderByElements) {
        if (!(fromItem instanceof Table)) {
            return true;
        }
        Table table = (Table) fromItem;
        Index primaryIndex = Main.mySchema.getTable(table.getName()).getPrimaryIndex();
        Map<String, Boolean> tempMap = new HashMap<>();
        for (OrderByElement orderByElement: orderByElements) {
            Column c = (Column) orderByElement.getExpression();
            tempMap.put(c.getColumnName(), false);
        }
        for (String columnName: primaryIndex.getColumnsNames()) {
            tempMap.put(columnName, true);
        }
        for (Boolean b: tempMap.values()) {
            if (!b) return true;
        }
        return false;
    }
}
