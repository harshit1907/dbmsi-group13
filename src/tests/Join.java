package tests;
import java.io.IOException;
import java.security.SecureRandom;

import btree.BTreeFile;
import btree.IntegerKey;
import btree.StringKey;
import bufmgr.PageNotReadException;
import edgeheap.EScan;
import edgeheap.Edge;
import global.*;
import heap.*;
import index.IndexException;
import index.IndexScan;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.NestedLoopsJoins;
import iterator.PredEvalException;
import iterator.RelSpec;
import iterator.Sort;
import iterator.SortException;
import iterator.SortMerge;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;
import queryPojo.EdgeQueryPojo;
import queryPojo.NodeQueryPojo;

public class Join {

    public NestedLoopsJoins joinEdgeEdgeNested(String name, EdgeQueryPojo edgeQueryPojo) throws JoinsException, IndexException, PageNotReadException,
    TupleUtilsException, PredEvalException, SortException, LowMemException,
    UnknowAttrType, UnknownKeyTypeException, Exception {
        final boolean OK = true;
        final boolean FAIL = false;
        boolean status = true;

        CondExpr[] outfilter = new CondExpr[2];
        outfilter[0] = new CondExpr();
        outfilter[1] = new CondExpr();

        outfilter[0].next = null;
        outfilter[0].op = new AttrOperator(AttrOperator.aopEQ);
        outfilter[0].type1 = new AttrType(AttrType.attrSymbol);
        outfilter[0].type2 = new AttrType(AttrType.attrSymbol);
        outfilter[0].operand1.symbol = new FldSpec(new
                RelSpec(RelSpec.outer), 9);
        outfilter[0].operand2.symbol = new
                FldSpec(new RelSpec(RelSpec.innerRel), 8);
        outfilter[1] = null;

        AttrType[] etypes = new AttrType[9];
        etypes[0] = new AttrType(AttrType.attrInteger);
        etypes[1] = new AttrType(AttrType.attrInteger);
        etypes[2] = new AttrType(AttrType.attrInteger);
        etypes[3] = new AttrType(AttrType.attrInteger);
        etypes[4] = new AttrType(AttrType.attrInteger);
        etypes[5] = new AttrType(AttrType.attrInteger);
        etypes[6] = new AttrType(AttrType.attrString);
        etypes[7] = new AttrType(AttrType.attrString);
        etypes[8] = new AttrType(AttrType.attrString);

        short[] esizes = new short[3];
        esizes[0] = 20;
        esizes[1] = 20;
        esizes[2] = 20;

        FldSpec[] edgeFileProjList = new FldSpec[9];
        RelSpec relN = new RelSpec(RelSpec.innerRel);
        edgeFileProjList[0] = new FldSpec(relN, 1);
        edgeFileProjList[1] = new FldSpec(relN, 2);
        edgeFileProjList[2] = new FldSpec(relN, 3);
        edgeFileProjList[3] = new FldSpec(relN, 4);
        //RelSpec relN = new RelSpec(RelSpec.innerRel);
        edgeFileProjList[4] = new FldSpec(relN, 5);
        edgeFileProjList[5] = new FldSpec(relN, 7);
        edgeFileProjList[6] = new FldSpec(relN, 8);
        edgeFileProjList[7] = new FldSpec(relN, 9);
        edgeFileProjList[8] = new FldSpec(relN, 6);
        

        CondExpr[] condEdge = new CondExpr[2];
        condEdge[0] = new CondExpr();
        if (edgeQueryPojo.getKey() == 1) {
            condEdge[0].op = new AttrOperator(AttrOperator.aopEQ);
            condEdge[0].next = null;
            condEdge[0].type1 = new AttrType(AttrType.attrSymbol);
            condEdge[0].type2 = new AttrType(AttrType.attrString);
            condEdge[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 7);
            condEdge[0].operand2.string = edgeQueryPojo.getEdgelabel();
            
        } else if (edgeQueryPojo.getKey() == 2) {
            condEdge[0].op = new AttrOperator(AttrOperator.aopLE);
            condEdge[0].next = null;
            condEdge[0].type1 = new AttrType(AttrType.attrSymbol);
            condEdge[0].type2 = new AttrType(AttrType.attrInteger);
            condEdge[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            condEdge[0].operand2.integer = edgeQueryPojo.getWeight();
            
        }
        else if (edgeQueryPojo.getKey() == 6) {
            condEdge[0].op = new AttrOperator(AttrOperator.aopEQ);
            condEdge[0].next = null;
            condEdge[0].type1 = new AttrType(AttrType.attrSymbol);
            condEdge[0].type2 = new AttrType(AttrType.attrInteger);
            condEdge[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 6);
            condEdge[0].operand2.integer = edgeQueryPojo.getUniqueKey();
        }
        else {
            throw new Exception("Condition not valid; Need Edge Source");
        }
        condEdge[1] = null;

        FldSpec[] projlist = new FldSpec[9];
        RelSpec rel1 = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel1, 1);
        projlist[1] = new FldSpec(rel1, 2);
        projlist[2] = new FldSpec(rel1, 3);
        projlist[3] = new FldSpec(rel1, 4);
        projlist[4] = new FldSpec(rel1, 5);
        projlist[5] = new FldSpec(rel1, 6);
        projlist[6] = new FldSpec(rel1, 7);
        projlist[7] = new FldSpec(rel1, 8);
        projlist[8] = new FldSpec(rel1, 9);
        Iterator edgeIterator = null;

        IndexType b_index = new IndexType(IndexType.B_Index);
        if (edgeQueryPojo.getKey() == 1) {
            try {
                edgeIterator = new IndexScan(b_index, "UniqueEdgeEid",
                        "EdgeLabelIndexEid", etypes, esizes, 9, 9,
                        projlist, condEdge, 7, false);
            } catch (Exception e) {
                System.err.println("*** Error creating scan for Index scan");
                System.err.println("" + e);
                Runtime.getRuntime().exit(1);
            }
        }
        else if (edgeQueryPojo.getKey() == 2) 
        {
            try {
                edgeIterator = new IndexScan(b_index, "UniqueEdgeEid",
                        "EdgeWeightIndexEid", etypes, esizes, 9, 9,
                        projlist, condEdge, 1, false);
            } catch (Exception e) {
                System.err.println("*** Error creating scan for Index scan");
                System.err.println("" + e);
                Runtime.getRuntime().exit(1);
            }
        }else if (edgeQueryPojo.getKey() == 6) 
        {
            try {
            	 edgeIterator = new IndexScan(b_index, "UniqueEdgeEid",
                        "EdgeEidIndexEid", etypes, esizes, 9, 9,
                        projlist, condEdge, 6, false);
            } catch (Exception e) {
                System.err.println("*** Error creating scan for Index scan");
                System.err.println("" + e);
                Runtime.getRuntime().exit(1);
            }
        }
        
//          Tuple test = new Tuple();
//          while((test = edgeIterator.get_next()) != null) {
//              System.out.println("HERE");
//              System.out.println(test.getIntFld(6));
//          }

        NestedLoopsJoins nlj = null;
        try {
            nlj = new NestedLoopsJoins(etypes, 9, esizes,
                    etypes, 9, esizes,
                    100,
                    edgeIterator, "UniqueEdgeEid",
                    outfilter, null, edgeFileProjList, 9);
        } catch (Exception e) {
            System.err.println("*** Error preparing for nested_loop_join");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        return nlj;
    }

    public void joinEdgeEdge(String name) throws JoinsException, IndexException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {

        final boolean OK = true;
        final boolean FAIL = false;
        boolean status = true;
        String graphDbName = name;
        Tuple t = new Tuple();
        t = null;
        AttrType[] etypes = new AttrType[7];
        etypes[0] = new AttrType(AttrType.attrInteger);
        etypes[1] = new AttrType(AttrType.attrInteger);
        etypes[2] = new AttrType(AttrType.attrInteger);
        etypes[3] = new AttrType(AttrType.attrInteger);
        etypes[4] = new AttrType(AttrType.attrInteger);
        etypes[5] = new AttrType(AttrType.attrString);
        etypes[6] = new AttrType(AttrType.attrString);

        AttrType[] ntypes = {
                new AttrType(AttrType.attrDesc),
                new AttrType(AttrType.attrString),
        };
        short[] esizes = new short[2];
        esizes[0] = 20;
        esizes[1] = 20;
        short[] nsizes = new short[1];
        nsizes[0] = 20;
        AttrType[] Jtypes = {
                new AttrType(AttrType.attrString),
                new AttrType(AttrType.attrString)
        };
        short[] Jsizes = new short[2];
        Jsizes[0] = 20;
        Jsizes[1] = 20;
        FldSpec[] proj1 = new FldSpec[7];
        //       proj1[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        //       proj1[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        //       proj1[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
        //       proj1[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
        proj1[0] = new FldSpec(new RelSpec(RelSpec.outer), 5);
        proj1[1] = new FldSpec(new RelSpec(RelSpec.outer), 6);
        proj1[2] = new FldSpec(new RelSpec(RelSpec.outer), 7);
        //         proj1[7] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        //         proj1[8] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
        //         proj1[9] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
        proj1[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 4);
        proj1[4] = new FldSpec(new RelSpec(RelSpec.innerRel), 5);
        proj1[5] = new FldSpec(new RelSpec(RelSpec.innerRel), 6);
        proj1[6] = new FldSpec(new RelSpec(RelSpec.innerRel), 7);


        FldSpec[] projlist = new FldSpec[7];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);
        projlist[3] = new FldSpec(rel, 4);
        projlist[4] = new FldSpec(rel, 5);
        projlist[5] = new FldSpec(rel, 6);
        projlist[6] = new FldSpec(rel, 7);
        FldSpec[] projlist2 = new FldSpec[7];
        RelSpec rel2 = new RelSpec(RelSpec.outer);
        projlist2[0] = new FldSpec(rel2, 1);
        projlist2[1] = new FldSpec(rel2, 2);
        projlist2[2] = new FldSpec(rel2, 3);
        projlist2[3] = new FldSpec(rel2, 4);
        projlist2[4] = new FldSpec(rel2, 5);
        projlist2[5] = new FldSpec(rel2, 6);
        projlist2[6] = new FldSpec(rel2, 7);
        //CondExpr [] selects= new CondExpr[1];
        //selects[0]=null;
        //
        //      iterator.Iterator am = null;
        //      /*BTreeFile btf = null;
        //        try {
        //            btf = new BTreeFile(SystemDefs.JavabaseDBName+"_BTreeNodeIndex", AttrType.attrString, 32, 1);
        //        }
        //        catch (Exception e) {
        //            status = FAIL;
        //            e.printStackTrace();
        //            Runtime.getRuntime().exit(1);
        //        }*/
        //      IndexType b_index = new IndexType (IndexType.B_Index);
        //      try {
        //          am = new IndexScan( b_index, SystemDefs.JavabaseDBName+"_Node",
        //                  SystemDefs.JavabaseDBName+"_BTreeNodeIndex", ntypes, nsizes, 2, 2,
        //                  proj1, null, 2, false);
        //      }
        //
        //      catch (Exception e) {
        //          System.err.println ("*** Error creating scan for Index scan");
        //          System.err.println (""+e);
        //          Runtime.getRuntime().exit(1);
        //      }

        //new hesp file

        Tuple nhp = new Tuple();
        AttrType[] attrType = new AttrType[7];
        attrType[0] = new AttrType(AttrType.attrInteger);
        attrType[1] = new AttrType(AttrType.attrInteger);
        attrType[2] = new AttrType(AttrType.attrInteger);
        attrType[3] = new AttrType(AttrType.attrInteger);
        attrType[4] = new AttrType(AttrType.attrInteger);
        attrType[5] = new AttrType(AttrType.attrString);
        attrType[6] = new AttrType(AttrType.attrString);

        short[] attrSize = new short[2];
        attrSize[0] = 20;
        attrSize[1] = 20;
        nhp.setHdr((short) 7, attrType, attrSize);

        EScan scan = null;
        boolean statusN = OK;


        Heapfile f = new Heapfile(SystemDefs.JavabaseDBName + "p3tempEfile11.in");

        if (statusN == OK) {
            System.out.println("  - Scan the records just inserted\n");

            try {
                scan = SystemDefs.JavabaseDB.ehfile.openScan();
            } catch (Exception e) {
                statusN = FAIL;
                System.err.println("*** Error opening scan\n");
                e.printStackTrace();
            }

            if (statusN == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                    == SystemDefs.JavabaseBM.getNumBuffers()) {
                System.err.println("*** The heap-file scan has not pinned the first page\n");
                statusN = FAIL;
            }
        }
        EID eidTmp = new EID();


        if (status == OK) {
            Edge edge = null;

            boolean done = false;
            while (!done) {
                edge = scan.getNext(eidTmp);
                if (edge == null) {
                    done = true;
                    break;
                }

                nhp.setIntFld(1, edge.getWeight());
                nhp.setIntFld(2, edge.getSource().pageNo.pid);
                nhp.setIntFld(3, edge.getSource().slotNo);
                nhp.setIntFld(4, edge.getDestination().pageNo.pid);
                nhp.setIntFld(5, edge.getDestination().slotNo);
                nhp.setStrFld(6, edge.getLabel());
                nhp.setStrFld(7, SystemDefs.JavabaseDB.nhfile.getNode(edge.getDestination()).getLabel());
                f.insertRecord(nhp.getTupleByteArray());
            }
        }
        scan.closescan();


        // new edge headp 2 file


        Tuple newT = new Tuple();
        AttrType[] attrTypeN = new AttrType[7];
        attrTypeN[0] = new AttrType(AttrType.attrInteger);
        attrTypeN[1] = new AttrType(AttrType.attrInteger);
        attrTypeN[2] = new AttrType(AttrType.attrInteger);
        attrTypeN[3] = new AttrType(AttrType.attrInteger);
        attrTypeN[4] = new AttrType(AttrType.attrInteger);
        attrTypeN[5] = new AttrType(AttrType.attrString);
        attrTypeN[6] = new AttrType(AttrType.attrString);

        short[] attrSizeN = new short[2];
        attrSizeN[0] = 20;
        attrSizeN[1] = 20;
        newT.setHdr((short) 7, attrTypeN, attrSizeN);

        EScan scanNew = null;
        boolean statusNe = OK;


        Heapfile fN = new Heapfile(SystemDefs.JavabaseDBName + "p3tempEfile22.in");

        if (statusNe == OK) {
            System.out.println("  - Scan the records just inserted\n");

            try {
                scanNew = SystemDefs.JavabaseDB.ehfile.openScan();
            } catch (Exception e) {
                statusNe = FAIL;
                System.err.println("*** Error opening scan\n");
                e.printStackTrace();
            }

            if (statusNe == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                    == SystemDefs.JavabaseBM.getNumBuffers()) {
                System.err.println("*** The heap-file scan has not pinned the first page\n");
                statusNe = FAIL;
            }
        }
        EID eidT = new EID();


        if (statusNe == OK) {
            Edge edgeT = null;

            boolean done = false;
            while (!done) {
                edgeT = scanNew.getNext(eidT);
                if (edgeT == null) {
                    done = true;
                    break;
                }

                newT.setIntFld(1, edgeT.getWeight());
                newT.setIntFld(2, edgeT.getSource().pageNo.pid);
                newT.setIntFld(3, edgeT.getSource().slotNo);
                newT.setIntFld(4, edgeT.getDestination().pageNo.pid);
                newT.setIntFld(5, edgeT.getDestination().slotNo);
                newT.setStrFld(6, edgeT.getLabel());
                newT.setStrFld(7, SystemDefs.JavabaseDB.nhfile.getNode(edgeT.getSource()).getLabel());
                //System.out.println(newT.getTupleByteArray());
                fN.insertRecord(newT.getTupleByteArray());
            }
        }
        scanNew.closescan();

        TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
        CondExpr[] outfilter = new CondExpr[2];
        outfilter[0] = new CondExpr();
        outfilter[1] = new CondExpr();

        outfilter[0].next = null;
        outfilter[0].op = new AttrOperator(AttrOperator.aopEQ);
        outfilter[0].type1 = new AttrType(AttrType.attrSymbol);
        outfilter[0].type2 = new AttrType(AttrType.attrSymbol);
        outfilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 7);
        outfilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 7);

        outfilter[1] = null;

        FileScan am2 = null;
        try {
            am2 = new FileScan(SystemDefs.JavabaseDBName + "p3tempEfile11.in", etypes, esizes,
                    (short) 7, (short) 7,
                    projlist2, null);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("" + e);
        }

        if (status != OK) {
            //bail out
            System.err.println("*** Error setting up scan for reserves");
            Runtime.getRuntime().exit(1);
        }

        FileScan am = null;
        try {
            am = new FileScan(SystemDefs.JavabaseDBName + "p3tempEfile22.in", etypes, esizes,
                    (short) 7, (short) 7,
                    projlist, null);
        } catch (Exception e) {
            status = FAIL;
            System.err.println("" + e);
        }

        if (status != OK) {
            //bail out
            System.err.println("*** Error setting up scan for sailors");
            Runtime.getRuntime().exit(1);
        }

        SortMerge sm = null;
        try {
            sm = new SortMerge(etypes, 7, esizes,
                    etypes, 7, esizes,
                    7, 20,
                    7, 20,
                    2000,
                    am, am2,
                    false, false, ascending,
                    outfilter, proj1, 7);
        } catch (Exception e) {
            System.err.println("*** Error preparing for nested_loop_join");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }


        t = null;

        try {
            while ((t = sm.get_next()) != null) {
                System.out.println(t.getIntFld(1) + " " + t.getStrFld(2) + " " + t.getStrFld(3) + " " + t.getIntFld(4) + " " + t.getIntFld(5) + " " + t.getStrFld(6) + " " + t.getStrFld(7));

            }
        } catch (Exception e) {
            System.err.println("" + e);
            e.printStackTrace();
            status = FAIL;
        }
        if (status != OK) {
            //bail out
            System.err.println("*** Error in get next tuple ");
            Runtime.getRuntime().exit(1);
        }
        try {
            sm.close();
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        System.out.println("\n");
        if (status != OK) {
            //bail out
            System.err.println("*** Error in closing ");
            Runtime.getRuntime().exit(1);
        }
        f.deleteFile();
        fN.deleteFile();
    }

    public NestedLoopsJoins joinNodeDEdge(String name, NodeQueryPojo nodeQueryPojo) throws JoinsException,
    IndexException, PageNotReadException,
    TupleUtilsException, PredEvalException, SortException, LowMemException,
    UnknowAttrType, UnknownKeyTypeException, Exception {
        final boolean OK = true;
        final boolean FAIL = false;
        boolean status = true;

        String graphDbName = name;
        CondExpr[] outfilter = new CondExpr[2];
        outfilter[0] = new CondExpr();
        outfilter[1] = new CondExpr();

        outfilter[0].next = null;
        outfilter[0].op = new AttrOperator(AttrOperator.aopEQ);
        outfilter[0].type1 = new AttrType(AttrType.attrSymbol);
        outfilter[0].type2 = new AttrType(AttrType.attrSymbol);
        outfilter[0].operand1.symbol = new FldSpec(new
                RelSpec(RelSpec.outer), 2);
        outfilter[0].operand2.symbol = new
                FldSpec(new RelSpec(RelSpec.innerRel), 8);
        outfilter[1] = null;

        Tuple t = new Tuple();
        AttrType[] etypes = new AttrType[8];
        etypes[0] = new AttrType(AttrType.attrInteger);
        etypes[1] = new AttrType(AttrType.attrInteger);
        etypes[2] = new AttrType(AttrType.attrInteger);
        etypes[3] = new AttrType(AttrType.attrInteger);
        etypes[4] = new AttrType(AttrType.attrInteger);
        etypes[5] = new AttrType(AttrType.attrString);
        etypes[6] = new AttrType(AttrType.attrString);
        etypes[7] = new AttrType(AttrType.attrString);

        AttrType[] ntypes = {
                new AttrType(AttrType.attrDesc),
                new AttrType(AttrType.attrString),
        };
        short[] esizes = new short[3];
        esizes[0] = 20;
        esizes[1] = 20;
        esizes[2] = 20;
        short[] nsizes = new short[1];
        nsizes[0] = 20;

        FldSpec[] proj1 = {
                new FldSpec(new RelSpec(RelSpec.outer), 1),
                new FldSpec(new RelSpec(RelSpec.outer), 2)
        };
        FldSpec[] projlist = new FldSpec[9];
        RelSpec rel = new RelSpec(RelSpec.innerRel);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);
        projlist[3] = new FldSpec(rel, 4);
        projlist[4] = new FldSpec(rel, 5);
        projlist[5] = new FldSpec(rel, 6);
        projlist[6] = new FldSpec(rel, 7);
        projlist[7] = new FldSpec(rel, 8);
        projlist[8] = new FldSpec(new RelSpec(RelSpec.outer), 1);

        CondExpr[] condNode = new CondExpr[2];
        condNode[0] = new CondExpr();
        condNode[0].op = new AttrOperator(AttrOperator.aopEQ);
        condNode[0].next = null;
        condNode[0].type1 = new AttrType(AttrType.attrSymbol);
        if (nodeQueryPojo.getKey() == 1) {
            condNode[0].type2 = new AttrType(AttrType.attrString);
            condNode[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
            condNode[0].operand2.string = nodeQueryPojo.getLabel();
        } else if (nodeQueryPojo.getKey() == 2) {
            condNode[0].type2 = new AttrType(AttrType.attrDesc);
            condNode[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            condNode[0].operand2.desc = nodeQueryPojo.getDesc();
        } else {
            throw new Exception("Condition not valid");
        }
        condNode[1] = null;

        iterator.Iterator nodeIndexIterator = null;

        IndexType b_index = new IndexType(IndexType.B_Index);
        if (nodeQueryPojo.getKey() == 1) {
            try {
                nodeIndexIterator = new IndexScan(b_index, SystemDefs.JavabaseDBName + "_Node",
                        SystemDefs.JavabaseDBName + "_BTreeNodeIndex", ntypes, nsizes, 2, 2,
                        proj1, condNode, 2, false);
            } catch (Exception e) {
                System.err.println("*** Error creating scan for Index scan");
                System.err.println("" + e);
                Runtime.getRuntime().exit(1);
            }
        } else if (nodeQueryPojo.getKey() == 2) {
            try {
                nodeIndexIterator = new ZIndex.ZIndexScan(b_index, SystemDefs.JavabaseDBName + "_Node",
                        SystemDefs.JavabaseDBName + "_ZTreeNodeIndex", ntypes, nsizes, 2, 2,
                        proj1, condNode, 1, false);
            } catch (Exception e) {
                System.err.println("*** Error creating scan for Index scan");
                System.err.println("" + e);
                Runtime.getRuntime().exit(1);
            }
        }

        NestedLoopsJoins nlj = null;
        try {
            nlj = new NestedLoopsJoins(ntypes, 2, nsizes,
                    etypes, 8, esizes,
                    100,
                    nodeIndexIterator, "UniqueEdge",
                    outfilter, null, projlist, 9);
        } catch (Exception e) {
            System.err.println("*** Error preparing for nested_loop_join");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        return nlj;
    }
    
    
    
    
    public NestedLoopsJoins joinNodeSEdgeEid(String name, NodeQueryPojo nodeQueryPojo) throws JoinsException, IndexException, PageNotReadException,
    TupleUtilsException, PredEvalException, SortException, LowMemException,
    UnknowAttrType, UnknownKeyTypeException, Exception {
        final boolean OK = true;
        final boolean FAIL = false;
        boolean status = true;

        CondExpr[] outfilter = new CondExpr[2];
        outfilter[0] = new CondExpr();
        outfilter[1] = new CondExpr();

        outfilter[0].next = null;
        outfilter[0].op = new AttrOperator(AttrOperator.aopEQ);
        outfilter[0].type1 = new AttrType(AttrType.attrSymbol);
        outfilter[0].type2 = new AttrType(AttrType.attrSymbol);
        outfilter[0].operand1.symbol = new FldSpec(new
                RelSpec(RelSpec.outer), 2);
        outfilter[0].operand2.symbol = new
                FldSpec(new RelSpec(RelSpec.innerRel), 8);
        outfilter[1] = null;

        AttrType[] etypes = new AttrType[9];
        etypes[0] = new AttrType(AttrType.attrInteger);
        etypes[1] = new AttrType(AttrType.attrInteger);
        etypes[2] = new AttrType(AttrType.attrInteger);
        etypes[3] = new AttrType(AttrType.attrInteger);
        etypes[4] = new AttrType(AttrType.attrInteger);
        etypes[5] = new AttrType(AttrType.attrInteger);
        etypes[6] = new AttrType(AttrType.attrString);
        etypes[7] = new AttrType(AttrType.attrString);
        etypes[8] = new AttrType(AttrType.attrString);

        AttrType[] ntypes = {
                new AttrType(AttrType.attrDesc),
                new AttrType(AttrType.attrString),
        };
        short[] esizes = new short[3];
        esizes[0] = 20;
        esizes[1] = 20;
        esizes[2] = 20;
        short[] nsizes = new short[1];
        nsizes[0] = 20;

        FldSpec[] proj1 = {
                new FldSpec(new RelSpec(RelSpec.outer), 1),
                new FldSpec(new RelSpec(RelSpec.outer), 2)
        };

        FldSpec[] projlist = new FldSpec[10];
        RelSpec rel = new RelSpec(RelSpec.innerRel);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);
        projlist[3] = new FldSpec(rel, 4);
        projlist[4] = new FldSpec(rel, 5);
        projlist[5] = new FldSpec(rel, 7);
        projlist[6] = new FldSpec(rel, 8);
        projlist[7] = new FldSpec(rel, 9);
        projlist[8] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        projlist[9] = new FldSpec(rel, 6);

        CondExpr[] condNode = new CondExpr[2];
        condNode[0] = new CondExpr();
        condNode[0].op = new AttrOperator(AttrOperator.aopEQ);
        condNode[0].next = null;
        condNode[0].type1 = new AttrType(AttrType.attrSymbol);
        if (nodeQueryPojo.getKey() == 1) {
            condNode[0].type2 = new AttrType(AttrType.attrString);
            condNode[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
            condNode[0].operand2.string = nodeQueryPojo.getLabel();
        } else if (nodeQueryPojo.getKey() == 2) {
            condNode[0].type2 = new AttrType(AttrType.attrDesc);
            condNode[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            condNode[0].operand2.desc = nodeQueryPojo.getDesc();
        } else {
            throw new Exception("Condition not valid; Need Edge Source");
        }
        condNode[1] = null;

        iterator.Iterator nodeIndexIterator = null;

        if (nodeQueryPojo.getKey() == 1) {
            IndexType b_index = new IndexType(IndexType.B_Index);
            try {
                nodeIndexIterator = new IndexScan(b_index, SystemDefs.JavabaseDBName + "_Node",
                        SystemDefs.JavabaseDBName + "_BTreeNodeIndex", ntypes, nsizes, 2, 2,
                        proj1, condNode, 2, false);
            } catch (Exception e) {
                System.err.println("*** Error creating scan for Index scan");
                System.err.println("" + e);
                Runtime.getRuntime().exit(1);
            }
        } else if (nodeQueryPojo.getKey() == 2) {
            IndexType b_index = new IndexType(IndexType.ZIndex);
            try {
                nodeIndexIterator = new ZIndex.ZIndexScan(b_index, SystemDefs.JavabaseDBName + "_Node",
                        SystemDefs.JavabaseDBName + "_ZTreeNodeIndex", ntypes, nsizes, 2, 2,
                        proj1, condNode, 1, false);
            } catch (Exception e) {
                System.err.println("*** Error creating scan for Index scan");
                System.err.println("" + e);
                Runtime.getRuntime().exit(1);
            }
        }

        NestedLoopsJoins nlj = null;
        try {
            nlj = new NestedLoopsJoins(ntypes, 2, nsizes,
                    etypes, 9, esizes,
                    100,
                    nodeIndexIterator, "UniqueEdgeEid",
                    outfilter, null, projlist, 10);
        } catch (Exception e) {
            System.err.println("*** Error preparing for nested_loop_join");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        return nlj;
    }
    
    
    
    
    

    public NestedLoopsJoins joinNodeSEdge(String name, NodeQueryPojo nodeQueryPojo) throws JoinsException, IndexException, PageNotReadException,
    TupleUtilsException, PredEvalException, SortException, LowMemException,
    UnknowAttrType, UnknownKeyTypeException, Exception {
        final boolean OK = true;
        final boolean FAIL = false;
        boolean status = true;

        CondExpr[] outfilter = new CondExpr[2];
        outfilter[0] = new CondExpr();
        outfilter[1] = new CondExpr();

        outfilter[0].next = null;
        outfilter[0].op = new AttrOperator(AttrOperator.aopEQ);
        outfilter[0].type1 = new AttrType(AttrType.attrSymbol);
        outfilter[0].type2 = new AttrType(AttrType.attrSymbol);
        outfilter[0].operand1.symbol = new FldSpec(new
                RelSpec(RelSpec.outer), 2);
        outfilter[0].operand2.symbol = new
                FldSpec(new RelSpec(RelSpec.innerRel), 7);
        outfilter[1] = null;

        AttrType[] etypes = new AttrType[8];
        etypes[0] = new AttrType(AttrType.attrInteger);
        etypes[1] = new AttrType(AttrType.attrInteger);
        etypes[2] = new AttrType(AttrType.attrInteger);
        etypes[3] = new AttrType(AttrType.attrInteger);
        etypes[4] = new AttrType(AttrType.attrInteger);
        etypes[5] = new AttrType(AttrType.attrString);
        etypes[6] = new AttrType(AttrType.attrString);
        etypes[7] = new AttrType(AttrType.attrString);

        AttrType[] ntypes = {
                new AttrType(AttrType.attrDesc),
                new AttrType(AttrType.attrString),
        };
        short[] esizes = new short[3];
        esizes[0] = 20;
        esizes[1] = 20;
        esizes[2] = 20;
        short[] nsizes = new short[1];
        nsizes[0] = 20;

        FldSpec[] proj1 = {
                new FldSpec(new RelSpec(RelSpec.outer), 1),
                new FldSpec(new RelSpec(RelSpec.outer), 2)
        };

        FldSpec[] projlist = new FldSpec[9];
        RelSpec rel = new RelSpec(RelSpec.innerRel);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);
        projlist[3] = new FldSpec(rel, 4);
        projlist[4] = new FldSpec(rel, 5);
        projlist[5] = new FldSpec(rel, 6);
        projlist[6] = new FldSpec(rel, 7);
        projlist[7] = new FldSpec(rel, 8);
        projlist[8] = new FldSpec(new RelSpec(RelSpec.outer), 1);

        CondExpr[] condNode = new CondExpr[2];
        condNode[0] = new CondExpr();
        condNode[0].op = new AttrOperator(AttrOperator.aopEQ);
        condNode[0].next = null;
        condNode[0].type1 = new AttrType(AttrType.attrSymbol);
        if (nodeQueryPojo.getKey() == 1) {
            condNode[0].type2 = new AttrType(AttrType.attrString);
            condNode[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
            condNode[0].operand2.string = nodeQueryPojo.getLabel();
        } else if (nodeQueryPojo.getKey() == 2) {
            condNode[0].type2 = new AttrType(AttrType.attrDesc);
            condNode[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            condNode[0].operand2.desc = nodeQueryPojo.getDesc();
        } else {
            throw new Exception("Condition not valid; Need Edge Source");
        }
        condNode[1] = null;

        iterator.Iterator nodeIndexIterator = null;

        if (nodeQueryPojo.getKey() == 1) {
            IndexType b_index = new IndexType(IndexType.B_Index);
            try {
                nodeIndexIterator = new IndexScan(b_index, SystemDefs.JavabaseDBName + "_Node",
                        SystemDefs.JavabaseDBName + "_BTreeNodeIndex", ntypes, nsizes, 2, 2,
                        proj1, condNode, 2, false);
            } catch (Exception e) {
                System.err.println("*** Error creating scan for Index scan");
                System.err.println("" + e);
                Runtime.getRuntime().exit(1);
            }
        } else if (nodeQueryPojo.getKey() == 2) {
            IndexType b_index = new IndexType(IndexType.ZIndex);
            try {
                nodeIndexIterator = new ZIndex.ZIndexScan(b_index, SystemDefs.JavabaseDBName + "_Node",
                        SystemDefs.JavabaseDBName + "_ZTreeNodeIndex", ntypes, nsizes, 2, 2,
                        proj1, condNode, 1, false);
            } catch (Exception e) {
                System.err.println("*** Error creating scan for Index scan");
                System.err.println("" + e);
                Runtime.getRuntime().exit(1);
            }
        }

        NestedLoopsJoins nlj = null;
        try {
            nlj = new NestedLoopsJoins(ntypes, 2, nsizes,
                    etypes, 8, esizes,
                    100,
                    nodeIndexIterator, "UniqueEdge",
                    outfilter, null, projlist, 9);
        } catch (Exception e) {
            System.err.println("*** Error preparing for nested_loop_join");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        return nlj;
    }

    public NestedLoopsJoins joinEdgeSNode(String name, EdgeQueryPojo edgeQueryPojo) throws JoinsException, IndexException, PageNotReadException,
    TupleUtilsException, PredEvalException, SortException, LowMemException,
    UnknowAttrType, UnknownKeyTypeException, Exception {
        final boolean OK = true;
        final boolean FAIL = false;
        boolean status = true;

        CondExpr[] outfilter = new CondExpr[2];
        outfilter[0] = new CondExpr();
        outfilter[1] = new CondExpr();

        outfilter[0].next = null;
        outfilter[0].op = new AttrOperator(AttrOperator.aopEQ);
        outfilter[0].type1 = new AttrType(AttrType.attrSymbol);
        outfilter[0].type2 = new AttrType(AttrType.attrSymbol);
        outfilter[0].operand1.symbol = new FldSpec(new
                RelSpec(RelSpec.outer), 7);
        outfilter[0].operand2.symbol = new
                FldSpec(new RelSpec(RelSpec.innerRel), 2);
        outfilter[1] = null;

        AttrType[] etypes = new AttrType[8];
        etypes[0] = new AttrType(AttrType.attrInteger);
        etypes[1] = new AttrType(AttrType.attrInteger);
        etypes[2] = new AttrType(AttrType.attrInteger);
        etypes[3] = new AttrType(AttrType.attrInteger);
        etypes[4] = new AttrType(AttrType.attrInteger);
        etypes[5] = new AttrType(AttrType.attrString);
        etypes[6] = new AttrType(AttrType.attrString);
        etypes[7] = new AttrType(AttrType.attrString);

        AttrType[] ntypes = {
                new AttrType(AttrType.attrDesc),
                new AttrType(AttrType.attrString),
        };
        short[] esizes = new short[3];
        esizes[0] = 20;
        esizes[1] = 20;
        esizes[2] = 20;
        short[] nsizes = new short[1];
        nsizes[0] = 20;

        FldSpec[] edgeFileProjList = new FldSpec[9];
        RelSpec rel = new RelSpec(RelSpec.outer);
        edgeFileProjList[0] = new FldSpec(rel, 1);
        edgeFileProjList[1] = new FldSpec(rel, 2);
        edgeFileProjList[2] = new FldSpec(rel, 3);
        edgeFileProjList[3] = new FldSpec(rel, 4);
        edgeFileProjList[4] = new FldSpec(rel, 5);
        edgeFileProjList[5] = new FldSpec(rel, 6);
        edgeFileProjList[6] = new FldSpec(rel, 7);
        edgeFileProjList[7] = new FldSpec(rel, 8);
        edgeFileProjList[8] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);

        CondExpr[] condEdge = new CondExpr[2];
        condEdge[0] = new CondExpr();
        if (edgeQueryPojo.getKey() == 1) {
            condEdge[0].op = new AttrOperator(AttrOperator.aopEQ);
            condEdge[0].next = null;
            condEdge[0].type1 = new AttrType(AttrType.attrSymbol);
            condEdge[0].type2 = new AttrType(AttrType.attrString);
            condEdge[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 6);
            condEdge[0].operand2.string = edgeQueryPojo.getEdgelabel();
        } else if (edgeQueryPojo.getKey() == 2) {
            condEdge[0].op = new AttrOperator(AttrOperator.aopLE);
            condEdge[0].next = null;
            condEdge[0].type1 = new AttrType(AttrType.attrSymbol);
            condEdge[0].type2 = new AttrType(AttrType.attrInteger);
            condEdge[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            condEdge[0].operand2.integer = edgeQueryPojo.getWeight();
        } else {
            throw new Exception("Condition not valid; Need Edge Source");
        }
        condEdge[1] = null;

        FldSpec[] projlist = new FldSpec[8];
        RelSpec rel1 = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel1, 1);
        projlist[1] = new FldSpec(rel1, 2);
        projlist[2] = new FldSpec(rel1, 3);
        projlist[3] = new FldSpec(rel1, 4);
        projlist[4] = new FldSpec(rel1, 5);
        projlist[5] = new FldSpec(rel1, 6);
        projlist[6] = new FldSpec(rel1, 7);
        projlist[7] = new FldSpec(rel1, 8);
        Iterator edgeIterator = null;

        IndexType b_index = new IndexType(IndexType.B_Index);
        if (edgeQueryPojo.getKey() == 1) {
            try {
                edgeIterator = new IndexScan(b_index, "UniqueEdge",
                        "EdgeLabelIndex", etypes, esizes, 8, 8,
                        projlist, condEdge, 6, false);
            } catch (Exception e) {
                System.err.println("*** Error creating scan for Index scan");
                System.err.println("" + e);
                Runtime.getRuntime().exit(1);
            }
        }
        else if (edgeQueryPojo.getKey() == 2) 
        {
            try {
                edgeIterator = new IndexScan(b_index, "UniqueEdge",
                        "EdgeWeightIndex", etypes, esizes, 8, 8,
                        projlist, condEdge, 1, false);
            } catch (Exception e) {
                System.err.println("*** Error creating scan for Index scan");
                System.err.println("" + e);
                Runtime.getRuntime().exit(1);
            }
        }
        //
        //        Tuple test = new Tuple();
        //        while((test = edgeIterator.get_next()) != null) {
        //            System.out.println("HERE");
        //            System.out.println(test.getStrFld(6));
        //        }

        NestedLoopsJoins nlj = null;
        try {
            nlj = new NestedLoopsJoins(etypes, 8, esizes,
                    ntypes, 2, nsizes,
                    100,
                    edgeIterator, SystemDefs.JavabaseDBName + "_Node",
                    outfilter, null, edgeFileProjList, 9);
        } catch (Exception e) {
            System.err.println("*** Error preparing for nested_loop_join");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        return nlj;
    }

    public NestedLoopsJoins joinEdgeDNode(String name, EdgeQueryPojo edgeQueryPojo) throws JoinsException, IndexException, PageNotReadException,
    TupleUtilsException, PredEvalException, SortException, LowMemException,
    UnknowAttrType, UnknownKeyTypeException, Exception {
        final boolean OK = true;
        final boolean FAIL = false;
        boolean status = true;

        CondExpr[] outfilter = new CondExpr[2];
        outfilter[0] = new CondExpr();
        outfilter[1] = new CondExpr();

        outfilter[0].next = null;
        outfilter[0].op = new AttrOperator(AttrOperator.aopEQ);
        outfilter[0].type1 = new AttrType(AttrType.attrSymbol);
        outfilter[0].type2 = new AttrType(AttrType.attrSymbol);
        outfilter[0].operand1.symbol = new FldSpec(new
                RelSpec(RelSpec.outer), 8);
        outfilter[0].operand2.symbol = new
                FldSpec(new RelSpec(RelSpec.innerRel), 2);
        outfilter[1] = null;

        AttrType[] etypes = new AttrType[8];
        etypes[0] = new AttrType(AttrType.attrInteger);
        etypes[1] = new AttrType(AttrType.attrInteger);
        etypes[2] = new AttrType(AttrType.attrInteger);
        etypes[3] = new AttrType(AttrType.attrInteger);
        etypes[4] = new AttrType(AttrType.attrInteger);
        etypes[5] = new AttrType(AttrType.attrString);
        etypes[6] = new AttrType(AttrType.attrString);
        etypes[7] = new AttrType(AttrType.attrString);

        AttrType[] ntypes = {
                new AttrType(AttrType.attrDesc),
                new AttrType(AttrType.attrString),
        };
        short[] esizes = new short[3];
        esizes[0] = 20;
        esizes[1] = 20;
        esizes[2] = 20;
        short[] nsizes = new short[1];
        nsizes[0] = 20;

        FldSpec[] edgeFileProjList = new FldSpec[9];
        RelSpec rel = new RelSpec(RelSpec.outer);
        edgeFileProjList[0] = new FldSpec(rel, 1);
        edgeFileProjList[1] = new FldSpec(rel, 2);
        edgeFileProjList[2] = new FldSpec(rel, 3);
        edgeFileProjList[3] = new FldSpec(rel, 4);
        edgeFileProjList[4] = new FldSpec(rel, 5);
        edgeFileProjList[5] = new FldSpec(rel, 6);
        edgeFileProjList[6] = new FldSpec(rel, 7);
        edgeFileProjList[7] = new FldSpec(rel, 8);
        edgeFileProjList[8] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);

        CondExpr[] condEdge = new CondExpr[2];
        condEdge[0] = new CondExpr();
        if (edgeQueryPojo.getKey() == 1) {
            condEdge[0].op = new AttrOperator(AttrOperator.aopEQ);
            condEdge[0].next = null;
            condEdge[0].type1 = new AttrType(AttrType.attrSymbol);
            condEdge[0].type2 = new AttrType(AttrType.attrString);
            condEdge[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 6);
            condEdge[0].operand2.string = edgeQueryPojo.getEdgelabel();
        } else if (edgeQueryPojo.getKey() == 2) {
            condEdge[0].op = new AttrOperator(AttrOperator.aopLE);
            condEdge[0].next = null;
            condEdge[0].type1 = new AttrType(AttrType.attrSymbol);
            condEdge[0].type2 = new AttrType(AttrType.attrInteger);
            condEdge[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            condEdge[0].operand2.integer = edgeQueryPojo.getWeight();
        } else {
            throw new Exception("Condition not valid; Need Edge Source");
        }
        condEdge[1] = null;

        FldSpec[] projlist = new FldSpec[8];
        RelSpec rel1 = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel1, 1);
        projlist[1] = new FldSpec(rel1, 2);
        projlist[2] = new FldSpec(rel1, 3);
        projlist[3] = new FldSpec(rel1, 4);
        projlist[4] = new FldSpec(rel1, 5);
        projlist[5] = new FldSpec(rel1, 6);
        projlist[6] = new FldSpec(rel1, 7);
        projlist[7] = new FldSpec(rel1, 8);
        Iterator edgeIterator = null;

        IndexType b_index = new IndexType(IndexType.B_Index);
        if (edgeQueryPojo.getKey() == 1) {
            try {
                edgeIterator = new IndexScan(b_index, "UniqueEdge",
                        "EdgeLabelIndex", etypes, esizes, 8, 8,
                        projlist, condEdge, 6, false);
            } catch (Exception e) {
                System.err.println("*** Error creating scan for Index scan");
                System.err.println("" + e);
                Runtime.getRuntime().exit(1);
            }
        }
        else if (edgeQueryPojo.getKey() == 2) 
        {
            try {
                edgeIterator = new IndexScan(b_index, "UniqueEdge",
                        "EdgeWeightIndex", etypes, esizes, 8, 8,
                        projlist, condEdge, 1, false);
            } catch (Exception e) {
                System.err.println("*** Error creating scan for Index scan");
                System.err.println("" + e);
                Runtime.getRuntime().exit(1);
            }
        }
        //
        //          Tuple test = new Tuple();
        //          while((test = edgeIterator.get_next()) != null) {
        //              System.out.println("HERE");
        //              System.out.println(test.getStrFld(6));
        //          }

        NestedLoopsJoins nlj = null;
        try {
            nlj = new NestedLoopsJoins(etypes, 8, esizes,
                    ntypes, 2, nsizes,
                    100,
                    edgeIterator, SystemDefs.JavabaseDBName + "_Node",
                    outfilter, null, edgeFileProjList, 9);
        } catch (Exception e) {
            System.err.println("*** Error preparing for nested_loop_join");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        return nlj;
    }


    public static void createEdgeIndexEid() throws InvalidSlotNumberException, Exception {
        final boolean OK = true;
        final boolean FAIL = false;
        boolean status = true;
        SecureRandom sr1 = new SecureRandom();
         
        Tuple t = new Tuple();
        AttrType[] etypes = new AttrType[9];
        etypes[0] = new AttrType(AttrType.attrInteger);
        etypes[1] = new AttrType(AttrType.attrInteger);
        etypes[2] = new AttrType(AttrType.attrInteger);
        etypes[3] = new AttrType(AttrType.attrInteger);
        etypes[4] = new AttrType(AttrType.attrInteger);
        etypes[5] = new AttrType(AttrType.attrInteger);
        etypes[6] = new AttrType(AttrType.attrString);
        etypes[7] = new AttrType(AttrType.attrString);
        etypes[8] = new AttrType(AttrType.attrString);

        short[] esizes = new short[3];
        esizes[0] = 20;
        esizes[1] = 20;
        esizes[2] = 20;

        Tuple nhp = new Tuple();
        nhp.setHdr((short) 9, etypes, esizes);

        EScan scan = null;
        boolean statusN = OK;


        Heapfile f = new Heapfile("UniqueEdgeEid");
        // create the index file
        BTreeFile btf1 = null;
        BTreeFile btf2 = null;
        BTreeFile btf3 = null;
        BTreeFile btf4 = null;
        BTreeFile btf5 = null;

        try {
            btf1 = new BTreeFile("EdgeSourceIndexEid", AttrType.attrString, 20, 1);
            btf2 = new BTreeFile("EdgeDestinationIndexEid", AttrType.attrString, 20, 1);
            btf3 = new BTreeFile("EdgeLabelIndexEid", AttrType.attrString, 20, 1);
            btf4 = new BTreeFile("EdgeWeightIndexEid", AttrType.attrInteger, 4, 1);
            btf5 = new BTreeFile("EdgeEidIndexEid", AttrType.attrInteger, 4, 1);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        RID rid = new RID();
        String key = "";
        String key2 = "";
        String key3 = "";
        Integer key4 = 0;
        Integer key5 = 0;
        Tuple temp = null;


        if (statusN == OK) {
            System.out.println("  - Scan the records just inserted\n");

            try {
                scan = SystemDefs.JavabaseDB.ehfile.openScan();
            } catch (Exception e) {
                statusN = FAIL;
                System.err.println("*** Error opening scan\n");
                e.printStackTrace();
            }

            if (statusN == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                    == SystemDefs.JavabaseBM.getNumBuffers()) {
                System.err.println("*** The heap-file scan has not pinned the first page\n");
                statusN = FAIL;
            }
        }
        EID eidTmp = new EID();


        if (status == OK) {
            Edge edge = null;

            boolean done = false;
            while (!done) {
                edge = scan.getNext(eidTmp);
                if (edge == null) {
                    done = true;
                    break;
                }

                nhp.setIntFld(1, edge.getWeight());
                nhp.setIntFld(2, edge.getSource().pageNo.pid);
                nhp.setIntFld(3, edge.getSource().slotNo);
                nhp.setIntFld(4, edge.getDestination().pageNo.pid);
                nhp.setIntFld(5, edge.getDestination().slotNo);
                nhp.setIntFld(6, sr1.nextInt(Integer.MAX_VALUE));
                nhp.setStrFld(7, edge.getLabel());
                nhp.setStrFld(8, SystemDefs.JavabaseDB.nhfile.getNode(edge.getSource()).getLabel());
                nhp.setStrFld(9, SystemDefs.JavabaseDB.nhfile.getNode(edge.getDestination()).getLabel());
                
                rid = f.insertRecord(nhp.getTupleByteArray());

                try {
                    key = nhp.getStrFld(8);
                    key2 = nhp.getStrFld(9);
                    key3 = nhp.getStrFld(7);
                    key4 = nhp.getIntFld(1);
                    key5 = nhp.getIntFld(6);
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

                try {
                    btf1.insert(new StringKey(key), rid);
                    btf2.insert(new StringKey(key2), rid);
                    btf3.insert(new StringKey(key3), rid);
                    btf4.insert(new IntegerKey(key4), rid);
                    btf5.insert(new IntegerKey(key5), rid);
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            }
        }
        btf1.close();
        btf2.close();
        btf3.close();
        btf4.close();
        btf5.close();
        
    }
    
    
public static void createEdgeIndex() throws InvalidSlotNumberException, Exception {
        
    final boolean OK = true;
    final boolean FAIL = false;
    boolean status = true;

    Tuple t = new Tuple();
    AttrType[] etypes = new AttrType[8];
    etypes[0] = new AttrType(AttrType.attrInteger);
    etypes[1] = new AttrType(AttrType.attrInteger);
    etypes[2] = new AttrType(AttrType.attrInteger);
    etypes[3] = new AttrType(AttrType.attrInteger);
    etypes[4] = new AttrType(AttrType.attrInteger);
    etypes[5] = new AttrType(AttrType.attrString);
    etypes[6] = new AttrType(AttrType.attrString);
    etypes[7] = new AttrType(AttrType.attrString);

    short[] esizes = new short[3];
    esizes[0] = 20;
    esizes[1] = 20;
    esizes[2] = 20;

    Tuple nhp = new Tuple();
    nhp.setHdr((short) 8, etypes, esizes);

    EScan scan = null;
    boolean statusN = OK;


    Heapfile f = new Heapfile("UniqueEdge");
    // create the index file
    BTreeFile btf1 = null;
    BTreeFile btf2 = null;
    BTreeFile btf3 = null;
    BTreeFile btf4 = null;

    try {
        btf1 = new BTreeFile("EdgeSourceIndex", AttrType.attrString, 20, 1);
        btf2 = new BTreeFile("EdgeDestinationIndex", AttrType.attrString, 20, 1);
        btf3 = new BTreeFile("EdgeLabelIndex", AttrType.attrString, 20, 1);
        btf4 = new BTreeFile("EdgeWeightIndex", AttrType.attrInteger, 4, 1);
    } catch (Exception e) {
        status = FAIL;
        e.printStackTrace();
        Runtime.getRuntime().exit(1);
    }

    RID rid = new RID();
    String key = "";
    String key2 = "";
    String key3 = "";
    Integer key4 = 0;
    Tuple temp = null;


    if (statusN == OK) {
        System.out.println("  - Scan the records just inserted\n");

        try {
            scan = SystemDefs.JavabaseDB.ehfile.openScan();
        } catch (Exception e) {
            statusN = FAIL;
            System.err.println("*** Error opening scan\n");
            e.printStackTrace();
        }

        if (statusN == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                == SystemDefs.JavabaseBM.getNumBuffers()) {
            System.err.println("*** The heap-file scan has not pinned the first page\n");
            statusN = FAIL;
        }
    }
    EID eidTmp = new EID();


    if (status == OK) {
        Edge edge = null;

        boolean done = false;
        while (!done) {
            edge = scan.getNext(eidTmp);
            if (edge == null) {
                done = true;
                break;
            }

            nhp.setIntFld(1, edge.getWeight());
            nhp.setIntFld(2, edge.getSource().pageNo.pid);
            nhp.setIntFld(3, edge.getSource().slotNo);
            nhp.setIntFld(4, edge.getDestination().pageNo.pid);
            nhp.setIntFld(5, edge.getDestination().slotNo);
            nhp.setStrFld(6, edge.getLabel());
            nhp.setStrFld(7, SystemDefs.JavabaseDB.nhfile.getNode(edge.getSource()).getLabel());
            nhp.setStrFld(8, SystemDefs.JavabaseDB.nhfile.getNode(edge.getDestination()).getLabel());
            rid = f.insertRecord(nhp.getTupleByteArray());

            try {
                key = nhp.getStrFld(7);
                key2 = nhp.getStrFld(8);
                key3 = nhp.getStrFld(6);
                key4 = nhp.getIntFld(1);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                btf1.insert(new StringKey(key), rid);
                btf2.insert(new StringKey(key2), rid);
                btf3.insert(new StringKey(key3), rid);
                btf4.insert(new IntegerKey(key4), rid);
            } catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }
    }
    btf1.close();
    btf2.close();
    btf3.close();
    btf4.close();
}



}