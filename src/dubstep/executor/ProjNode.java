package dubstep.executor;

import dubstep.utils.Tuple;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

public class ProjNode extends BaseNode {

    private List<Integer> projectionVector = new ArrayList<>();
    private boolean isCompleteProjection; // handle * cases
    private List<SelectItem> selectItems; //used for building of projection info

    public ProjNode(List<SelectItem> selectItems, BaseNode InnerNode) {
        super();
        this.innerNode = InnerNode;
        this.selectItems = selectItems;

        for (SelectItem item : selectItems) {
            if (!(item instanceof SelectExpressionItem)) {
                // case select * from table
                if (item.toString().equals("*"))
                    this.isCompleteProjection = true;
                else
                    throw new UnsupportedOperationException("We don't support this column type yet");
            }
            // All other projection cases
            else {

                this.isCompleteProjection = false;
                String col_name = item.toString();
                // case select db.col from table
                if (col_name.indexOf('.') >= 0) {
                    if (this.innerNode.projectionInfo.indexOf(col_name) >= 0)
                        projectionVector.add(this.innerNode.projectionInfo.indexOf(col_name));
                    else
                        throw new UnsupportedOperationException("Unknown column name" + col_name);
                }
                // case select col from table 
                else {
                    boolean found = false;
                    int currentIndex = 0, foundIndex = -1;
                    for (String col : this.innerNode.projectionInfo) {

                        if ((col.indexOf('.') >= 0 && col_name.equals(col.split("\\.")[1])) || (col.indexOf('.') < 0 && col_name.equals(col)) ) {
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
                        projectionVector.add(foundIndex);
                    else
                        throw new UnsupportedOperationException("Unknown column name" + col_name);
                }
            }
        }

        initProjectionInfo();
    }

    @Override
    Tuple getNextRow() {
        Tuple nextRow = this.innerNode.getNextRow();
        if (nextRow == null)
            return null;

        if (isCompleteProjection)
            return nextRow;
        else
            return new Tuple(nextRow, projectionVector);
    }

    @Override
    void resetIterator() {

    }

    @Override
    void initProjectionInfo() {
        if (isCompleteProjection)
            this.projectionInfo = this.innerNode.projectionInfo;
        else {
            projectionInfo = new ArrayList<>();
            for (Object selectItem : this.selectItems) {
                projectionInfo.add(selectItem.toString());
            }
        }
    }
}
