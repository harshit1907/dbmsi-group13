package ZIndex;

import bufmgr.PageNotReadException;
import edgeheap.EdgeHeapFile;
import global.AttrType;
import global.IndexType;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;
import iterator.*;
import zbtree.IndexFileScan;
import zbtree.ZBTFileScan;
import zbtree.ZBTreeFile;
import zbtree.ZIndexFile;

import java.io.IOException;

/**
 * Created by prakhar on 3/10/17.
 */
public class ZIndexScan extends Iterator {
    public FldSpec[] perm_mat;
    private ZIndexFile indFile;
    private IndexFileScan indScan;
    private AttrType[] _types;
    private short[] _s_sizes;
    private CondExpr[] _selects;
    private int _noInFlds;
    private int _noOutFlds;
    private EdgeHeapFile f;
    private Tuple tuple1;
    private Tuple Jtuple;
    private int t1_size;
    private int _fldNum;
    private boolean index_only;

    public ZIndexScan(
            IndexType index,
            final String relName,
            final String indName,
            AttrType types[],
            short str_sizes[],
            int noInFlds,
            int noOutFlds,
            FldSpec outFlds[],
            CondExpr selects[],
            final int fldNum,
            final boolean indexOnly
    ) throws IndexException, InvalidTypeException,
            InvalidTupleSizeException, UnknownIndexTypeException, IOException {
        _fldNum = fldNum;
        _noInFlds = noInFlds;
        _types = types;
        _s_sizes = str_sizes;

        AttrType[] Jtypes = new AttrType[noOutFlds];
        short[] ts_sizes;
        Jtuple = new Tuple();

        try {
            ts_sizes = TupleUtils.setup_op_tuple(Jtuple, Jtypes, types, noInFlds, str_sizes, outFlds, noOutFlds);
        } catch (TupleUtilsException e) {
            throw new IndexException(e, "IndexScan.java: TupleUtilsException caught from TupleUtils.setup_op_tuple()");
        } catch (InvalidRelation e) {
            throw new IndexException(e, "IndexScan.java: InvalidRelation caught from TupleUtils.setup_op_tuple()");
        }

        _selects = selects;
        perm_mat = outFlds;
        _noOutFlds = noOutFlds;
        tuple1 = new Tuple();
        try {
            tuple1.setHdr((short) noInFlds, types, str_sizes);
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: Heapfile error");
        }

        t1_size = tuple1.size();
        index_only = indexOnly;  // added by bingjie miao

        try {
            f = new EdgeHeapFile(relName);
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: Heapfile not created");
        }

        switch(index.indexType) {
            // linear hashing is not yet implemented
            case IndexType.ZIndex:
                // error check the select condition
                // must be of the type: value op symbol || symbol op value
                // but not symbol op symbol || value op value
                try {
                    indFile = new ZBTreeFile(indName);
                } catch (Exception e) {
                    throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from BTreeFile constructor");
                }

                try {
                    indScan = (ZBTFileScan) ZIndexUtils.BTree_scan(selects, indFile);
                } catch (Exception e) {
                    throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
                }

                break;
            case IndexType.None:
            default:
                throw new UnknownIndexTypeException("Only BTree index is supported so far");
        }
    }

    @Override
    public Tuple get_next() throws IOException, JoinsException, IndexException,
            InvalidTupleSizeException, InvalidTypeException, PageNotReadException,
            TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType,
            UnknownKeyTypeException, Exception {
        return null;
    }

    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {

    }
}
