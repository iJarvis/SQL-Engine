package dubstep.utils;

import net.sf.jsqlparser.expression.*;
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

    public Tuple(Tuple inputTuple, List<Integer> ProjectionVector) {
        this.tid = -1;
        for (Integer columnIndex : ProjectionVector) {
            this.valueArray.add(inputTuple.valueArray.get(columnIndex));
        }
    }

    public Tuple(Tuple innerTup, Tuple outerTuple) {

        this.tid = -1;
        this.valueArray.addAll(innerTup.valueArray);
        this.valueArray.addAll(outerTuple.valueArray);
    }

    public String getProjection(Integer[] projVector) {
        String output = "";
        for (int i = 0; i < projVector.length; i++) {
            output = output + valueArray.get(projVector[i]).toString();
        }
        return output;
    }

    public String GetProjection() {
        String output = "";
        for (PrimitiveValue value : valueArray) {
            output = output + value.toString() + "|";
        }
        output = output.substring(0, output.length() - 1);
        return output;
    }
}

