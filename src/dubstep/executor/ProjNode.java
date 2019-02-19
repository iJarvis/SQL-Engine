package dubstep.executor;

import dubstep.utils.Tuple;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

public class ProjNode extends BaseNode {


    ArrayList<Integer> ProjectionVector = new ArrayList<>();
    Boolean IsCompleteProjection; // handle * cases
    List<SelectItem> SelectItems; //used for building of projection info


    public ProjNode(List<SelectItem> selectItems, BaseNode InnerNode) {
        super();
        this.innerNode = InnerNode;
        this.SelectItems = selectItems;

        for (SelectItem item : selectItems) {
            if (!(item instanceof SelectExpressionItem)) {
                // case select * from table
                if (item.toString().equals("*"))
                    this.IsCompleteProjection = true;
                else
                    throw new UnsupportedOperationException("We don't support this column type yet");
            } else {
                this.IsCompleteProjection = false;
                String col_name = item.toString();
                // case select db.col from table
                if (col_name.indexOf('.') >= 0) {
                    if (this.innerNode.ProjectionInfo.indexOf(col_name) >= 0)
                        ProjectionVector.add(this.innerNode.ProjectionInfo.indexOf(col_name));
                    else
                        throw new UnsupportedOperationException("Unknown column name" + col_name);
                }
                // case select col from table 
                else {
                    boolean found = false;
                    int currentIndex = 0, foundIndex = -1;
                    for (String col : this.innerNode.ProjectionInfo) {
                        if (col_name.equals(col.split("\\.")[1])) {
                            if (found)
                                throw new UnsupportedOperationException("Ambiguous column name" + col_name);
                            else {
                                found = true;
                                foundIndex = currentIndex;
                            }

                        }
                        currentIndex++;
                    }
                    if (found)
                        ProjectionVector.add(foundIndex);
                    else
                        throw new UnsupportedOperationException("Unknown column name" + col_name);
                }


            }

        }
        InitProjectionInfo();


    }

    @Override
    Tuple getNextRow() {
        Tuple nextRow = this.innerNode.getNextRow();
        if (nextRow == null)
            return null;

        if (IsCompleteProjection)
            return nextRow;
        else
            return new Tuple(nextRow, ProjectionVector);


    }

    @Override
    void resetIterator() {

    }

    @Override
    void InitProjectionInfo() {
        if (IsCompleteProjection)
            this.ProjectionInfo = this.innerNode.ProjectionInfo;
        else {
            ProjectionInfo = new ArrayList<>();
            for (Object selectItem : this.SelectItems) {
                ProjectionInfo.add(selectItem.toString());
            }
        }

    }


}
