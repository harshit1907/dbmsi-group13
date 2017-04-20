package ZIndex;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.IndexType;
import global.NID;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;
import iterator.*;
import nodeheap.NodeHeapFile;
import zbtree.*;

import java.io.IOException;

import btree.BTFileScan;

/**
 * Created by prakhar on 3/10/17.
 */
public class ZIndexScan extends Iterator {
    public FldSpec[] perm_mat;
    private zbtree.ZBTreeFile indFile;
    private IndexFileScan indScan;
    private AttrType[] _types;
    private short[] _s_sizes;
    private CondExpr[] _selects;
    private int _noInFlds;
    private int _noOutFlds;
    private NodeHeapFile f;
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
    ) throws ZIndexException, InvalidTypeException,
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
            throw new ZIndexException(e, "IndexScan.java: TupleUtilsException caught from TupleUtils.setup_op_tuple()");
        } catch (InvalidRelation e) {
            throw new ZIndexException(e, "IndexScan.java: InvalidRelation caught from TupleUtils.setup_op_tuple()");
        }

        _selects = selects;
        perm_mat = outFlds;
        _noOutFlds = noOutFlds;
        tuple1 = new Tuple();
        try {
            tuple1.setHdr((short) noInFlds, types, str_sizes);
        } catch (Exception e) {
            throw new ZIndexException(e, "IndexScan.java: Heapfile error");
        }

        t1_size = tuple1.size();
        index_only = indexOnly;  // added by bingjie miao

        try {
            f = new NodeHeapFile(relName);
        } catch (Exception e) {
            throw new ZIndexException(e, "IndexScan.java: Heapfile not created");
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
                    throw new ZIndexException(e, "IndexScan.java: BTreeFile exceptions caught from BTreeFile constructor");
                }

                try {
                    indScan = ZIndexUtils.ZBTree_scan(selects, indFile);
                } catch (Exception e) {
                    throw new ZIndexException(e, "IndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
                }

                break;
            case IndexType.None:
            default:
                throw new UnknownIndexTypeException("Only BTree index is supported so far");
        }
    }

    @Override
    public Tuple get_next() throws IOException, JoinsException, ZIndexException,
            InvalidTupleSizeException, InvalidTypeException, PageNotReadException,
            TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType,
            UnknownKeyTypeException, Exception {
        NID nid;
        int unused;
        DescriptorDataEntry nextentry = null;

        try {
            nextentry = indScan.get_next();
        } catch (Exception e) {
            throw new ZIndexException(e, "IndexScan.java: BTree error");
        }

        while (nextentry != null) {
            if (index_only) {
                // only need to return the key
                AttrType[] attrType = new AttrType[1];
                short[] s_sizes = new short[1];

                if (_types[_fldNum -1].attrType == AttrType.attrDesc) {
                    attrType[0] = new AttrType(AttrType.attrDesc);
                    try {
                        Jtuple.setHdr((short) 1, attrType, s_sizes);
                    } catch (Exception e) {
                        throw new ZIndexException(e, "IndexScan.java: Heapfile error");
                    }

                    try {
                        Jtuple.setDescFld(1, ((DescriptorKey)nextentry.key).getDesc());
                    } catch (Exception e) {
                        throw new ZIndexException(e, "ZIndexScan.java: NodeHeapfile error");
                    }
                } else {
                    // attrReal not supported for now
                    throw new UnknownKeyTypeException("Only Integer and String keys are supported so far");
                }
                return Jtuple;
            }

            // not index_only, need to return the whole tuple
            nid = ((LeafData)nextentry.data).getData();
            try {
                tuple1 = f.getNode(nid);
            } catch (Exception e) {
                throw new ZIndexException(e, "IndexScan.java: getRecord failed");
            }

            try {
                tuple1.setHdr((short) _noInFlds, _types, _s_sizes);
            } catch (Exception e) {
                throw new ZIndexException(e, "IndexScan.java: Heapfile error");
            }

            boolean eval;
            try {
                eval = PredEval.Eval(_selects, tuple1, null, _types, null);
            } catch (Exception e) {
                throw new ZIndexException(e, "IndexScan.java: Heapfile error");
            }

            if (eval) {
                // need projection.java
                try {
                    Projection.Project(tuple1, _types, Jtuple, perm_mat, _noOutFlds);
                } catch (Exception e) {
                    throw new ZIndexException(e, "IndexScan.java: Heapfile error");
                }
                return Jtuple;
            }

            try {
                nextentry = indScan.get_next();
            } catch (Exception e) {
                throw new ZIndexException(e, "IndexScan.java: BTree error");
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException, JoinsException, SortException, ZIndexException {
            
        if (!closeFlag) {
            if (indScan instanceof ZBTFileScan) {
                try {
                    indFile.close();
                    ((ZBTFileScan)indScan).DestroyZBTreeFileScan();
                } catch(Exception e) {
                    throw new ZIndexException(e, "ZBTree error in destroying index scan.");
                }
            }
            closeFlag = true;
        }
    }
}
