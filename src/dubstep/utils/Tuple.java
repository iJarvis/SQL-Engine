package dubstep.utils;

import dubstep.storage.datatypes;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.text.SimpleDateFormat;
import java.util.*;

import static net.sf.jsqlparser.expression.DateValue.parseEscaped;

public class Tuple {
    int tid;
    public ArrayList<PrimitiveValue> valueArray = new ArrayList<>();

    public Tuple(String csv_string,List<datatypes> typeList,boolean isLineItem)
    {
        String[] args = new String[typeList.size()];//csv_string.split("\\|");
        int index = 0;
        int end_index =0;
        int runnind_index = 0;

        end_index = csv_string.indexOf('|',0);
        while (end_index > 0)
        {

            args[runnind_index] = csv_string.substring(index,end_index);
            index = end_index+1;
            end_index = csv_string.indexOf('|',index );
            runnind_index++;
        }
        args[runnind_index] = csv_string.substring(index);


                this.valueArray.add(  new LongValue(args[0]));
                this.valueArray.add( new LongValue(args[1]));
                this.valueArray.add( new LongValue(args[2]));
                this.valueArray.add( new LongValue(args[3]));
                this.valueArray.add( new DoubleValue(args[4]));
                this.valueArray.add( new DoubleValue(args[5]));
                this.valueArray.add( new DoubleValue(args[6]));
                this.valueArray.add( new DoubleValue(args[7]));
                this.valueArray.add(  new StringValue(args[8]));
                this.valueArray.add( new StringValue(args[9]));
                this.valueArray.add( new DateValue(args[10]));
                this.valueArray.add( new DateValue(args[11]));
                this.valueArray.add( new DateValue(args[12]));
                this.valueArray.add(  new StringValue(args[13]));
                this.valueArray.add( new  StringValue(args[14]));
                this.valueArray.add( new StringValue(args[15]));
    }


    public Tuple(String csv_string, int tid, List<datatypes> typeList) {

        String[] args = new String[typeList.size()];//csv_string.split("\\|");
        int index = 0;
        int end_index =0;
        int runnind_index = 0;

        end_index = csv_string.indexOf('|',0);
        while (end_index > 0)
        {

            args[runnind_index] = csv_string.substring(index,end_index);
            index = end_index+1;
            end_index = csv_string.indexOf('|',index );
            runnind_index++;
        }
        args[runnind_index] = csv_string.substring(index);

        tid = this.tid;

        for (int i = 0; i < typeList.size() ; i++) {


            switch (typeList.get(i)) {

                case INT_TYPE:
                    valueArray.add(new LongValue(args[i]));
                    break;
                case STRING_TYPE:
                    valueArray.add(new StringValue(args[i]));
                    break;
                case DOUBLE_TYPE:
                    valueArray.add(new DoubleValue(args[i]));
                    break;
                case DATE_TYPE:

                    valueArray.add(new DateValue(args[i]));


                    break;

            }
        }
    }

//    public Tuple(List<PrimitiveValue> tempvalueArray) {
    public Tuple(ArrayList<PrimitiveValue> values) {
        tid = -1;
        valueArray = values;
    }

    public Tuple(List<PrimitiveValue> tempvalueArray, List<ColumnDefinition> definition) {
        tid = -1;
        valueArray.addAll(tempvalueArray);
    }

    public Tuple(Tuple innerTup, Tuple outerTuple) {
        tid = -1;
        valueArray.addAll(innerTup.valueArray);
        valueArray.addAll(outerTuple.valueArray);
    }

    public String getProjection() {
        String output = "";
        for (PrimitiveValue value : valueArray) {
            if (value == null) {
                output = output + "null" + "|";
                continue;
            }
            if (value instanceof DateValue) {
                if(value.toString().contains("\""))
                    output = output + value.toString().substring(1, value.toString().length() - 1) + "|";
                else
                    output = output + value.toString()+"|";

                continue;
            }
            output = output + value.toString() + "|";
        }
        output = output.substring(0, output.length() - 1);
        return output;
    }

    public void setValue(int index, PrimitiveValue value) {
        valueArray.set(index, value);
    }


    public PrimitiveValue getValue(String columnName1, String columnName2, Map<String, Integer> projInfo) {
       
        if (projInfo.containsKey(columnName1)) {
            return valueArray.get(projInfo.get(columnName1));
        }
        if (projInfo.containsKey(columnName2)) {
            return valueArray.get(projInfo.get(columnName2));
        }
        return null;
    }

    public PrimitiveValue getValue(Column column, Map<String, Integer> projInfo) {
        return getValue(column.getWholeColumnName(), column.getColumnName(), projInfo);
    }

    @Override
    public String toString() {
        return getProjection();
    }

    public void addValueItem(PrimitiveValue primitiveValue) {
        valueArray.add(primitiveValue);
    }

    public String serializeTuple()
    {
        String serializedString = "";
        for(PrimitiveValue value : this.valueArray )
        {
            char dt =' ';
            if(value instanceof LongValue)
                dt = 'l';
            else if(value instanceof DoubleValue )
                dt = 'f';
            else if (value instanceof StringValue)
                dt = 's';
            else if (value instanceof DateValue )
                dt = 'd';
            if(dt == 's') {
                String newVal = value.toString();
                serializedString += newVal.substring(1, newVal.length() - 1) + "^" + dt + "|";
            }
            else if(value == null)
                serializedString += ""+"^"+dt+"|";
            else
                serializedString += value.toString()+"^"+dt+"|";
        }
        return  serializedString;
    }

    public static Tuple deserializeTuple (String serializedString)
    {
        ArrayList<PrimitiveValue> columnValues = new ArrayList<>();
        String values[] = serializedString.split(("\\|"));
        for(String value : values)
        {
            String column[] = value.split("\\^");
            if(column[1].equals("l"))
                columnValues.add(new LongValue(column[0]));
            else if(column[1].equals("f"))
                columnValues.add(new DoubleValue(column[0]));
            else if(column[1].equals("d"))
                columnValues.add(new DateValue(column[0]));
            else if(column[1].equals("s"))
                columnValues.add(new StringValue(column[0]));
        }
        return  new Tuple(columnValues);
    }


    public String serializeTuple1()
    {
        String serializedString = "";
        for(PrimitiveValue value : this.valueArray ) {
            if(value instanceof DateValue )
                serializedString +=  value.toRawString()+"|";
            else
            serializedString += value.toString() + "|";
        }
        return serializedString;
    }

    public static Tuple deserializeTuple2(String serializedString,ArrayList<datatypes> types)
    {
        ArrayList<PrimitiveValue> columnValues = new ArrayList<>();
        String values[] = serializedString.split(("\\|"));
        int index = 0;
        for(String value : values)
        {
            datatypes type = types.get(index);
            switch (type)
            {
                case DATE_TYPE:
                    columnValues.add(new DateValue(value));
                    break;
                case INT_TYPE:
                    columnValues.add(new LongValue(value));
                    break;
                case STRING_TYPE:
                    columnValues.add(new StringValue(value.substring(1,value.length()-1)));
                    break;
                case DOUBLE_TYPE:
                    columnValues.add(new DoubleValue(value));
                    break;

            }
            index++;

        }
        return  new Tuple(columnValues);
    }
}

