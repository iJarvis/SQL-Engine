package dubstep.utils;

import dubstep.Main;
import dubstep.storage.datatypes;
import net.sf.jsqlparser.expression.*;

import java.io.*;
import java.util.*;

public class BPlusTree implements Serializable {

    private int dataFileIndex = 0;
    private File bPlusDir;
    private PVComparator comparator = new PVComparator();
    private boolean isInMem;
    private ArrayList<datatypes> serTypes = new ArrayList<>();
    private boolean isTypeInit = false;
    private PrimitiveValue lastInsertedPV;

    /**
     * The branching factor used when none specified in constructor.
     */
    private static final int DEFAULT_BRANCHING_FACTOR = 128;

    /**
     * The branching factor for the B+ tree, that measures the capacity of nodes
     * (i.e., the number of children nodes) for internal nodes in the tree.
     */
    private int branchingFactor;

    /**
     * The root node of the B+ tree.
     */
    private Node root;

    public BPlusTree(String tableName) {
        this(DEFAULT_BRANCHING_FACTOR, tableName);
    }

    public BPlusTree(int branchingFactor, String tableName) {
        if (branchingFactor <= 2)
            throw new IllegalArgumentException("Illegal branching factor: "
                    + branchingFactor);
        this.branchingFactor = branchingFactor;
        root = new LeafNode();
        bPlusDir = new File("data/bPlus/" + tableName);
        if (!bPlusDir.exists()) {
            bPlusDir.mkdirs();
        }
    }

    /**
     * Returns the value to which the specified key is associated, or
     * {@code null} if this tree contains no association for the key.
     *
     * <p>
     * A return value of {@code null} does not <i>necessarily</i> indicate that
     * the tree contains no association for the key; it's also possible that the
     * tree explicitly associates the key to {@code null}.
     *
     * @param key
     *            the key whose associated value is to be returned
     *
     * @return the value to which the specified key is associated, or
     *         {@code null} if this tree contains no association for the key
     */
    public Tuple search(PrimitiveValue key) {
        return root.getValue(key);
    }

    /**
     * Returns the values associated with the keys specified by the range:
     * {@code key1} and {@code key2}.
     *
     * @param key1
     *            the start key of the range
     * @param key2
     *            the end end of the range
     * @return the values associated with the keys specified by the range:
     *         {@code key1} and {@code key2}
     */
    public BPIterator searchRange(PrimitiveValue key1, PrimitiveValue key2) {
        return root.getRange(key1, key2);
    }

    /**
     * Associates the specified value with the specified key in this tree. If
     * the tree previously contained a association for the key, the old value is
     * replaced.
     *
     * @param key
     *            the key with which the specified value is to be associated
     * @param value
     *            the value to be associated with the specified key
     */
    public void insert(PrimitiveValue key, Tuple value) {
        root.insertValue(key, value);
    }

    /**
     * Removes the association for the specified key from this tree if present.
     *
     * @param key
     *            the key whose association is to be removed from the tree
     */
    public void delete(PrimitiveValue key) {
        root.deleteValue(key);
    }

    public String toString() {
        Queue<List<Node>> queue = new LinkedList<List<Node>>();
        queue.add(Arrays.asList(root));
        StringBuilder sb = new StringBuilder();
        while (!queue.isEmpty()) {
            Queue<List<Node>> nextQueue = new LinkedList<List<Node>>();
            while (!queue.isEmpty()) {
                List<Node> nodes = queue.remove();
                sb.append('{');
                Iterator<Node> it = nodes.iterator();
                while (it.hasNext()) {
                    Node node = it.next();
                    sb.append(node.toString());
                    if (it.hasNext())
                        sb.append(", ");
                    if (node instanceof BPlusTree.InternalNode)
                        nextQueue.add(((InternalNode) node).children);
                }
                sb.append('}');
                if (!queue.isEmpty())
                    sb.append(", ");
                else
                    sb.append('\n');
            }
            queue = nextQueue;
        }

        return sb.toString();
    }

    private abstract class Node {
        private int keyCount = 0;
        List<PrimitiveValue> keys;

        public Node() {
        }

        int keyNumber() {
            return keyCount;
        }

        abstract Tuple getValue(PrimitiveValue key);

        abstract void deleteValue(PrimitiveValue key);

        abstract void insertValue(PrimitiveValue key, Tuple value);

        abstract PrimitiveValue getFirstLeafKey();

        abstract BPIterator getRange(PrimitiveValue key1, PrimitiveValue key2);

        abstract void merge(Node sibling);

        abstract Node split();

        abstract boolean isOverflow();

        abstract boolean isUnderflow();

//        public String toString() {
//            return keys.toString();
//        }
    }

    private class InternalNode extends Node {
        List<Node> children;

        public InternalNode() {
            this.children = new ArrayList<Node>();
            keys = new ArrayList<>();
        }

        @Override
        Tuple getValue(PrimitiveValue key) {
            return getChild(key).getValue(key);
        }

        @Override
        void deleteValue(PrimitiveValue key) {
            Node child = getChild(key);
            child.deleteValue(key);
            if (child.isUnderflow()) {
                Node childLeftSibling = getChildLeftSibling(key);
                Node childRightSibling = getChildRightSibling(key);
                Node left = childLeftSibling != null ? childLeftSibling : child;
                Node right = childLeftSibling != null ? child
                        : childRightSibling;
                left.merge(right);
                deleteChild(right.getFirstLeafKey());
                if (left.isOverflow()) {
                    Node sibling = left.split();
                    insertChild(sibling.getFirstLeafKey(), sibling);
                }
                if (root.keyNumber() == 0)
                    root = left;
            }
        }

        @Override
        void insertValue(PrimitiveValue key, Tuple value) {
            Node child = getChild(key);
            child.insertValue(key, value);
            if (child.isOverflow()) {
                Node sibling = child.split();
                insertChild(sibling.getFirstLeafKey(), sibling);
                if (child instanceof LeafNode) {
                    try {
                        ((LeafNode) child).writeDataToDisk();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            if (root.isOverflow()) {
                Node sibling = split();
                InternalNode newRoot = new InternalNode();
                newRoot.keys.add(sibling.getFirstLeafKey());
                newRoot.children.add(this);
                newRoot.children.add(sibling);
                root = newRoot;
            }
        }

        @Override
        PrimitiveValue getFirstLeafKey() {
            return children.get(0).getFirstLeafKey();
        }

        @Override
        BPIterator getRange(PrimitiveValue key1, PrimitiveValue key2) {
            return getChild(key1).getRange(key1, key2);
        }

        @Override
        void merge(Node sibling) {
            @SuppressWarnings("unchecked")
            InternalNode node = (InternalNode) sibling;
            keys.add(node.getFirstLeafKey());
            keys.addAll(node.keys);
            children.addAll(node.children);
        }

        @Override
        Node split() {
            int from = keyNumber() / 2 + 1, to = keyNumber();
            InternalNode sibling = new InternalNode();
            sibling.keys.addAll(keys.subList(from, to));
            sibling.children.addAll(children.subList(from, to + 1));

            keys.subList(from - 1, to).clear();
            children.subList(from, to + 1).clear();

            return sibling;
        }

        @Override
        boolean isOverflow() {
            return children.size() > branchingFactor;
        }

        @Override
        boolean isUnderflow() {
            return children.size() < (branchingFactor + 1) / 2;
        }

        Node getChild(PrimitiveValue key) {
            if (key == null) {
                return children.get(0);
            }
            int loc = Collections.binarySearch(keys, key, comparator);
            int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
            return children.get(childIndex);
        }

        void deleteChild(PrimitiveValue key) {
            int loc = Collections.binarySearch(keys, key, comparator);
            if (loc >= 0) {
                keys.remove(loc);
                children.remove(loc + 1);
            }
        }

        void insertChild(PrimitiveValue key, Node child) {
            int loc = Collections.binarySearch(keys, key, comparator);
            int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
            if (loc >= 0) {
                children.set(childIndex, child);
            } else {
                keys.add(childIndex, key);
                children.add(childIndex + 1, child);
            }
        }

        Node getChildLeftSibling(PrimitiveValue key) {
            int loc = Collections.binarySearch(keys, key, comparator);
            int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
            if (childIndex > 0)
                return children.get(childIndex - 1);

            return null;
        }

        Node getChildRightSibling(PrimitiveValue key) {
            int loc = Collections.binarySearch(keys, key, comparator);
            int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
            if (childIndex < keyNumber())
                return children.get(childIndex + 1);

            return null;
        }
    }

    private class LeafNode extends Node {
        List<Tuple> values;
        LeafNode next;
        boolean isDataLoaded = false;
        private File dataFile;
        private boolean isOverFlow = false;

        LeafNode() {
            keys = new ArrayList<PrimitiveValue>();
            values = new ArrayList<Tuple>();
            dataFile = new File(bPlusDir, String.valueOf(++dataFileIndex));
        }

        @Override
        Tuple getValue(PrimitiveValue key) {
            int loc = Collections.binarySearch(keys, key, comparator);
            return loc >= 0 ? values.get(loc) : null;
        }

        @Override
        void deleteValue(PrimitiveValue key) {
            int loc = Collections.binarySearch(keys, key, comparator);
            if (loc >= 0) {
                keys.remove(loc);
                values.remove(loc);
            }
        }

        @Override
        void insertValue(PrimitiveValue key, Tuple value) {
//            int loc = Collections.binarySearch(keys, key, comparator);
//            int valueIndex = loc >= 0 ? loc : -loc - 1;
//            if (loc >= 0) {
//                values.set(valueIndex, value);
//            } else {
//                keys.add(valueIndex, key);
//                values.add(valueIndex, value);
//            }
            keys.add(key);
            values.add(value);
            if (values.size() > branchingFactor && TupleComparator.compare(lastInsertedPV, key) != 0) {
                isOverFlow = true;
                if (root.isOverflow()) {
                    Node sibling = split();
                    InternalNode newRoot = new InternalNode();
                    newRoot.keys.add(sibling.getFirstLeafKey());
                    newRoot.children.add(this);
                    newRoot.children.add(sibling);
                    root = newRoot;
                }
            }
            lastInsertedPV = key;
        }

        @Override
        PrimitiveValue getFirstLeafKey() {
            return keys.get(0);
        }

        @Override
        BPIterator getRange(PrimitiveValue key1, PrimitiveValue key2) {
            return new BPIterator(this, key1, key2);
        }

        @Override
        void merge(Node sibling) {
            @SuppressWarnings("unchecked")
            LeafNode node = (LeafNode) sibling;
            keys.addAll(node.keys);
            values.addAll(node.values);
            next = node.next;
        }

        @Override
        Node split() {
            LeafNode sibling = new LeafNode();
            sibling.keys.add(keys.get(keys.size()-1));
            sibling.values.add(values.get(values.size()-1));

            keys.remove(keys.size()-1);
            values.remove(values.size()-1);

            sibling.next = next;
            next = sibling;
            return sibling;
        }

        @Override
        boolean isOverflow() {
            return isOverFlow;
        }

        @Override
        boolean isUnderflow() {
            return values.size() < branchingFactor / 2;
        }

        void readDataFromDisk() throws IOException {
            BufferedReader br = new BufferedReader(new FileReader(dataFile), 1000);
            String line = br.readLine();
            while (line != null) {
                values.add(Tuple.deserializeTuple2(br.readLine(), serTypes));
            }
            isDataLoaded = true;
        }

        void writeDataToDisk() throws IOException {
            if (!isTypeInit) {
                isTypeInit = true;
                Tuple tuple = values.get(0);
                for (PrimitiveValue val : tuple.valueArray) {
                    if (val instanceof LongValue)
                        serTypes.add(datatypes.INT_TYPE);

                    else if (val instanceof StringValue)
                        serTypes.add(datatypes.STRING_TYPE);

                    else if (val instanceof DateValue)
                        serTypes.add(datatypes.DATE_TYPE);

                    else if (val instanceof DoubleValue)
                        serTypes.add(datatypes.DOUBLE_TYPE);
                }
            }
            BufferedWriter writer = new BufferedWriter(new FileWriter(dataFile));
            for (int i = 0; i < values.size(); ++i) {
                writer.write(values.get(i).serializeTuple1() + "\n");
            }
            writer.close();
        }

        void freeDataFromMemory() {
            isDataLoaded = false;
            values.clear();
        }
    }

    public static class BPIterator {

        private LeafNode node;
        private Iterator<PrimitiveValue> kIt;
        private Iterator<Tuple> vIt;
        private PrimitiveValue lowerBound;
        private PrimitiveValue upperBound;
        private boolean isInitDone = false;

        public BPIterator(LeafNode node, PrimitiveValue lowerBound, PrimitiveValue upperBound) {
            this.node = node;
            kIt = node.keys.iterator();
            vIt = node.values.iterator();
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        public Tuple next() {
            if (!isInitDone && lowerBound != null) {
                PrimitiveValue key = kIt.next();
                while (TupleComparator.compare(lowerBound, key) > 0) {
                    key = kIt.next();
                }
                isInitDone = true;
            }
            if (!kIt.hasNext()) {
                node.freeDataFromMemory();
                node = node.next;
                if (node == null) {
                    return null;
                }
                try {
                    node.readDataFromDisk();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                kIt = node.keys.iterator();
                vIt = node.values.iterator();
            }
            PrimitiveValue key = kIt.next();
            if (upperBound != null) {
                if (TupleComparator.compare(key, upperBound) > 0) {
                    return null;
                }
            }
            return vIt.next();
        }
    }

    private class PVComparator implements Comparator<PrimitiveValue> {

        @Override
        public int compare(PrimitiveValue left, PrimitiveValue right) {
            return TupleComparator.compare(left, right);
        }
    }
}