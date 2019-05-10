package dubstep.utils;

import dubstep.storage.datatypes;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Tuple {
    static int length = 0;
    public PrimitiveValue[] valueArray;
    int tid;


    public Tuple(String csv_string, int tid, List<datatypes> typeList) {

        String[] args = new String[typeList.size()];//csv_string.split("\\|");
        int index = 0;
        int end_index = 0;
        int runnind_index = 0;

        end_index = csv_string.indexOf('|');
        while (end_index > 0) {

            args[runnind_index] = csv_string.substring(index, end_index);
            index = end_index + 1;
            end_index = csv_string.indexOf('|', index);
            runnind_index++;
        }
        args[runnind_index] = csv_string.substring(index);

        tid = this.tid;
        valueArray = new PrimitiveValue[typeList.size()];

        for (int i = 0; i < typeList.size(); i++) {


            switch (typeList.get(i)) {

                case INT_TYPE:
                    valueArray[i] = new LongValue(args[i]);
                    break;
                case STRING_TYPE:
                    valueArray[i] = (new StringValue(args[i]));
                    break;
                case DOUBLE_TYPE:
                    valueArray[i] = (new DoubleValue(args[i]));
                    break;
                case DATE_TYPE:

                    valueArray[i] = (new DateValue(args[i]));


                    break;

            }
        }
    }

    public Tuple(PrimitiveValue[] values) {
        tid = -1;
        valueArray = values;
    }


    public Tuple(Tuple innerTup, Tuple outerTuple) {
        tid = -1;
        int innerlen = innerTup.valueArray.length;
        int outerlen = outerTuple.valueArray.length;

        valueArray = new PrimitiveValue[innerTup.valueArray.length + outerTuple.valueArray.length];
        for (int i = 0; i < innerlen; i++)
            valueArray[i] = innerTup.valueArray[i];
        for (int j = 0; j < outerlen; j++)
            valueArray[innerlen + j] = outerTuple.valueArray[j];
    }

    public static Tuple deserializeTuple(String serializedString) {

        String[] values = serializedString.split(("\\|"));
        PrimitiveValue[] columnValues = new PrimitiveValue[values.length];
        int i = 0;
        for (String value : values) {
            String[] column = value.split("\\^");
            if (column[1].equals("l"))
                columnValues[i] = (new LongValue(column[0]));
            else if (column[1].equals("f"))
                columnValues[i] = (new DoubleValue(column[0]));
            else if (column[1].equals("d"))
                columnValues[i] = (new DateValue(column[0]));
            else if (column[1].equals("s"))
                columnValues[i] = (new StringValue(column[0]));
            i++;
        }
        return new Tuple(columnValues);
    }

    public static Tuple deserializeTuple(String serializedString, int maxlength) {

        String[] values = serializedString.split(("\\|"));
        PrimitiveValue[] columnValues = new PrimitiveValue[values.length + maxlength];
        int i = 0;
        for (String value : values) {
            String[] column = value.split("\\^");
            if (column[1].equals("l"))
                columnValues[i] = (new LongValue(column[0]));
            else if (column[1].equals("f"))
                columnValues[i] = (new DoubleValue(column[0]));
            else if (column[1].equals("d"))
                columnValues[i] = (new DateValue(column[0]));
            else if (column[1].equals("s"))
                columnValues[i] = (new StringValue(column[0]));
            i++;
        }
        length = values.length;
        return new Tuple(columnValues);
    }

    public static Tuple deserializeTuple2(String serializedString, ArrayList<datatypes> types) {
        PrimitiveValue[] columnValues = new PrimitiveValue[types.size()];
        String[] values = serializedString.split(("\\|"));
        int index = 0;
        for (String value : values) {
            datatypes type = types.get(index);
            switch (type) {
                case DATE_TYPE:
                    columnValues[index] = new DateValue(value);
                    break;
                case INT_TYPE:
                    columnValues[index] = new LongValue(value);
                    break;
                case STRING_TYPE:
                    columnValues[index] = new StringValue(value.substring(1, value.length() - 1));
                    break;
                case DOUBLE_TYPE:
                    columnValues[index] = new DoubleValue(value);
                    break;

            }
            index++;

        }
        return new Tuple(columnValues);
    }

    public String getProjection() {
        String output = "";
        for (PrimitiveValue value : valueArray) {
            if (value == null) {
                output = output + "null" + "|";
                continue;
            }
            if (value instanceof DateValue) {
                if (value.toString().contains("\""))
                    output = output + value.toString().substring(1, value.toString().length() - 1) + "|";
                else
                    output = output + value.toString() + "|";

                continue;
            }
            output = output + value.toString() + "|";
        }
        output = output.substring(0, output.length() - 1);
        return output;
    }

    public PrimitiveValue getValue(Integer index) {
        return valueArray[index];
    }

    public PrimitiveValue getValue(String columnName1, String columnName2, Map<String, Integer> projInfo) {

        if (projInfo.containsKey(columnName1)) {
            return valueArray[projInfo.get(columnName1)];
        }
        if (projInfo.containsKey(columnName2)) {
            return valueArray[projInfo.get(columnName2)];
        }
        return null;
    }

    public PrimitiveValue getValue(Column column, Map<String, Integer> projInfo) {
        return getValue(column.getWholeColumnName(), column.getColumnName(), projInfo);
    }

    public Integer GetPosition(Column column, Map<String, Integer> projInfo) {
        String columnName1 = column.getWholeColumnName();
        String columnName2 = column.getColumnName();
        if (projInfo.containsKey(columnName1)) {
            return projInfo.get(columnName1);
        }
        if (projInfo.containsKey(columnName2)) {
            return projInfo.get(columnName2);
        }
        return null;

    }

    @Override
    public String toString() {
        return getProjection();
    }

    public void addValueItem(PrimitiveValue primitiveValue) {
        valueArray[length++] = primitiveValue;
    }

    public String serializeTuple() {
        String serializedString = "";
        for (PrimitiveValue value : this.valueArray) {
            char dt = ' ';
            if (value instanceof LongValue)
                dt = 'l';
            else if (value instanceof DoubleValue)
                dt = 'f';
            else if (value instanceof StringValue)
                dt = 's';
            else if (value instanceof DateValue)
                dt = 'd';
            if (dt == 's') {
                String newVal = value.toString();
                serializedString += newVal.substring(1, newVal.length() - 1) + "^" + dt + "|";
            } else if (value == null)
                serializedString += "" + "^" + dt + "|";
            else
                serializedString += value.toString() + "^" + dt + "|";
        }
        return serializedString;
    }

    public String serializeTuple1() {
        String serializedString = "";
        for (PrimitiveValue value : this.valueArray) {
            if (value instanceof DateValue)
                serializedString += value.toRawString() + "|";
            else
                serializedString += value.toString() + "|";
        }
        return serializedString;
    }
}

