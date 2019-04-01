package dubstep.utils;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.PrimitiveType;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TupleComparator {

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public static int compare(PrimitiveValue leftPV, PrimitiveValue rightPV, boolean isAsc) {
        try {
            if (leftPV.getType() == PrimitiveType.DOUBLE) {
                if (leftPV.toDouble() < rightPV.toDouble()) {
                    return isAsc? -1: 1;
                } else if (leftPV.toDouble() > rightPV.toDouble()) {
                    return isAsc? 1: -1;
                }
            } else if (leftPV.getType() == PrimitiveType.LONG) {
                if (leftPV.toLong() < rightPV.toLong()) {
                    return isAsc? -1: 1;
                } else if (leftPV.toLong() > rightPV.toLong()) {
                    return isAsc? 1: -1;
                }
            } else if (leftPV.getType() == PrimitiveType.DATE) {
                Date leftDate = dateFormat.parse(leftPV.toRawString());
                Date rightDate = dateFormat.parse(rightPV.toRawString());
                if (isAsc) {
                    return leftDate.compareTo(rightDate);
                } else {
                    return rightDate.compareTo(leftDate);
                }
            }
            else if(leftPV.getType() == PrimitiveType.STRING )
            {
                String leftValue = leftPV.toString();
                String rightValue =rightPV.toString();

                if (isAsc) {
                    return  leftValue.compareTo(rightValue);
                }
                else
                    return  rightValue.compareTo(leftValue);
            }
        } catch (PrimitiveValue.InvalidPrimitive | ParseException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static int compare(PrimitiveValue leftPV, PrimitiveValue rightPV) {
        return compare(leftPV, rightPV, true);
    }
}
