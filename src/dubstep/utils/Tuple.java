package dubstep.utils;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.ArrayList;
import java.util.List;

public class Tuple {
    int tid;
    private ArrayList<PrimitiveValue> valueArray = new ArrayList<>();
    private List<ColumnDefinition> columnDefinitions;

    public Tuple(String csv_string, int tid, List<ColumnDefinition> columnDefinitions) {
        this.columnDefinitions = columnDefinitions;
        String[] args = csv_string.split("\\|");
        tid = this.tid;
        for (int i = 0; i < args.length; i++) {
            String dataType = columnDefinitions.get(i).getColDataType().getDataType().toLowerCase();
            if (args[i].equals("null"))
                valueArray.add(null);
            else if (dataType.equalsIgnoreCase("int"))
                valueArray.add(new LongValue(args[i]));

            else if (dataType.equalsIgnoreCase("string") || dataType.equalsIgnoreCase("varchar") || dataType.equalsIgnoreCase("char"))
                valueArray.add(new StringValue(args[i]));

            else if (dataType.equalsIgnoreCase("decimal"))
                valueArray.add(new DoubleValue(args[i]));

            else if (dataType.equalsIgnoreCase("date"))
                valueArray.add(new DateValue(args[i]));

            else {
                System.err.println("data type " + dataType + " not found");
                break;
            }
        }
    }

    public Tuple(String tupleSting) {

    }

    public Tuple(List<PrimitiveValue> tempvalueArray) {
        this.tid = -1;
        this.valueArray.addAll(tempvalueArray);
    }

    public Tuple(PrimitiveValue[] tempvalueArray) {
        this.tid = -1;
        for (PrimitiveValue val : tempvalueArray){
            this.valueArray.add(val);
        }
    }

    public Tuple(Tuple innerTup, Tuple outerTuple) {

        this.tid = -1;
        this.valueArray.addAll(innerTup.valueArray);
        this.valueArray.addAll(outerTuple.valueArray);
        this.columnDefinitions = new ArrayList<>();
        this.columnDefinitions.addAll(innerTup.columnDefinitions);
        this.columnDefinitions.addAll(outerTuple.columnDefinitions);

    }


    public String getProjection() {
        String output = "";
        for (PrimitiveValue value : valueArray) {
            if (value == null) {
                output = output + "null" + "|";
                continue;
            }
            output = output + value.toString() + "|";
        }
        output = output.substring(0, output.length() - 1);
        return output;
    }

    public  void setValue(int index, PrimitiveValue value){
        valueArray.set(index, value);
    }

    public PrimitiveValue getValue(String columnName1, String columnName2, List<String> projInfo) {
        //TODO: optimize it
        String findStr = columnName1;
        String findStr1 = columnName2;
        int index;
        boolean found = false;
        int final_index = 0;
        for (String col : projInfo) {
            String col1 = (col.indexOf('.') > -1) ? col.split("\\.")[1] : col;
            int index1 = findStr.indexOf('.');
            if (col.equals(findStr) || ((index1 < 0) && (col1.equals(findStr1)))) {

                found = true;
                break;
            }
            final_index++;
        }

        if (!found) {
            return null;
        }
//            throw new UnsupportedOperationException("column not found tuple.getvalue");
        return valueArray.get(final_index);
    }

    public PrimitiveValue getValue(Column column, List<String> projInfo) {
        return getValue(column.getWholeColumnName(), column.getColumnName(), projInfo);
    }

    @Override
    public String toString() {
        return getProjection();
    }

    public List<ColumnDefinition> getColumnDefinitions() {
        return columnDefinitions;
    }
}

