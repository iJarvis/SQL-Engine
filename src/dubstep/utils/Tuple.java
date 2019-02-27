package dubstep.utils;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.ArrayList;
import java.util.List;

public class Tuple {
    int tid;
    private ArrayList<PrimitiveValue> valueArray = new ArrayList<>();

    public Tuple(String csv_string, int tid, List<ColumnDefinition> columnDefinitions) {
        String[] args = csv_string.split("\\|");
        tid = this.tid;
        for (int i = 0; i < args.length; i++) {
            String dataType = columnDefinitions.get(i).getColDataType().getDataType();
            if (dataType.equals("int"))
                valueArray.add(new LongValue(args[i]));

            else if (dataType.equals("string") || dataType.equals("varchar") || dataType.equals("char"))
                valueArray.add(new StringValue(args[i]));

            else if (dataType.equals("decimal"))
                valueArray.add(new DoubleValue(args[i]));

            else if (dataType.equals("date"))
                valueArray.add(new DateValue(args[i]));

            else {
                System.err.println("data type " + dataType + "not found");
                break;
            }
        }
    }

    public Tuple(List<PrimitiveValue> tempvalueArray) {
        this.tid = -1;
        this.valueArray.addAll(tempvalueArray);
    }

    public Tuple(Tuple innerTup, Tuple outerTuple) {

        this.tid = -1;
        this.valueArray.addAll(innerTup.valueArray);
        this.valueArray.addAll(outerTuple.valueArray);
    }


    public String GetProjection() {
        String output = "";
        for (PrimitiveValue value : valueArray) {
            output = output + value.toString() + "|";
        }
        output = output.substring(0, output.length() - 1);
        return output;
    }

    public PrimitiveValue getValue(String columnName1, String columnName2, List<String> projInfo) {
        String findStr = columnName1;
        String findStr1 = columnName2;
        int index;
        boolean found = false;
        int final_index = 0;
        for (String col : projInfo) {
            if ((col.equals(findStr)) || (col.equals(findStr1))) {
                found = true;
                break;
            }
            if (col.indexOf('.') >= 0)
                col = col.split("\\.")[1];
            if (col.equals(findStr1)) {
                found = true;
                break;
            }

            final_index++;
        }

        if (!found)
            throw new UnsupportedOperationException("column not found tuple.getvalue");
        return valueArray.get(final_index);
    }

    public PrimitiveValue getValue(Column column, ArrayList<String> projInfo) {
        return getValue(column.getWholeColumnName(), column.getColumnName(), projInfo);
    }
}

