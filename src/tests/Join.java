package tests;
import java.io.IOException;

import btree.BTreeFile;
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
    //    public static void createDB(String graphDbName) {
    //        SystemDefs sysdef = new SystemDefs(graphDbName,10000,40,"Clock",0);
    //    }
    public void joinEdgeEdge(String name) throws JoinsException, IndexException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {

        final boolean OK   = true;
        final boolean FAIL = false;
        boolean status=true;
        String graphDbName = name;
        Tuple t = new Tuple();
        t=null;
        AttrType[] etypes = new AttrType[7];
        etypes[0] = new AttrType(AttrType.attrInteger);
        etypes[1] = new AttrType(AttrType.attrInteger);
        etypes[2] = new AttrType(AttrType.attrInteger);
        etypes[3] = new AttrType(AttrType.attrInteger);
        etypes[4] = new AttrType(AttrType.attrInteger);
        etypes[5] = new AttrType(AttrType.attrString);
        etypes[6] = new AttrType(AttrType.attrString);

        AttrType [] ntypes={
                new AttrType(AttrType.attrDesc),
                new AttrType(AttrType.attrString),
        };
        short[] esizes = new short[2];
        esizes[0] = 20;
        esizes[1] = 20;
        short [] nsizes= new short[1];
        nsizes[0]=20;
        AttrType [] Jtypes={
                new AttrType(AttrType.attrString),
                new AttrType(AttrType.attrString)
        };
        short [] Jsizes=new short[2];
        Jsizes[0]=20;
        Jsizes[1]=20;
        FldSpec [] proj1 = new FldSpec[7];
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

        Tuple nhp =new Tuple();
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
        nhp.setHdr((short)7, attrType, attrSize);

        EScan scan = null;
        boolean statusN = OK;


        Heapfile f = new Heapfile(SystemDefs.JavabaseDBName+"p3tempEfile11.in");

        if ( statusN == OK ) {  
            System.out.println ("  - Scan the records just inserted\n");

            try {
                scan = SystemDefs.JavabaseDB.ehfile.openScan();
            }
            catch (Exception e) {
                statusN = FAIL;
                System.err.println ("*** Error opening scan\n");
                e.printStackTrace();
            }

            if ( statusN == OK &&  SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                    == SystemDefs.JavabaseBM.getNumBuffers() ) {
                System.err.println ("*** The heap-file scan has not pinned the first page\n");
                statusN = FAIL;
            }
        }
        EID eidTmp = new EID();



        if ( status == OK ) {
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


        Tuple newT =new Tuple();
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
        newT.setHdr((short)7, attrTypeN, attrSizeN);

        EScan scanNew = null;
        boolean statusNe = OK;


        Heapfile fN = new Heapfile(SystemDefs.JavabaseDBName+"p3tempEfile22.in");

        if ( statusNe == OK ) {  
            System.out.println ("  - Scan the records just inserted\n");

            try {
                scanNew = SystemDefs.JavabaseDB.ehfile.openScan();
            }
            catch (Exception e) {
                statusNe = FAIL;
                System.err.println ("*** Error opening scan\n");
                e.printStackTrace();
            }

            if ( statusNe == OK &&  SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                    == SystemDefs.JavabaseBM.getNumBuffers() ) {
                System.err.println ("*** The heap-file scan has not pinned the first page\n");
                statusNe = FAIL;
            }
        }
        EID eidT = new EID();



        if ( statusNe == OK ) {
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
        CondExpr [] outfilter=new CondExpr[2];
        outfilter[0]=new CondExpr();
        outfilter[1]=new CondExpr();

        outfilter[0].next=null;
        outfilter[0].op=new AttrOperator(AttrOperator.aopEQ);
        outfilter[0].type1=new AttrType(AttrType.attrSymbol);
        outfilter[0].type2=new AttrType(AttrType.attrSymbol);
        outfilter[0].operand1.symbol=new FldSpec(new RelSpec(RelSpec.outer),7);
        outfilter[0].operand2.symbol=new FldSpec(new RelSpec(RelSpec.innerRel),7);

        outfilter[1]=null;

        FileScan am2 = null;
        try {
            am2 = new FileScan(SystemDefs.JavabaseDBName+"p3tempEfile11.in", etypes, esizes, 
                    (short)7, (short) 7,
                    projlist2, null);
        }
        catch (Exception e) {
            status = FAIL;
            System.err.println (""+e);
        }

        if (status != OK) {
            //bail out
            System.err.println ("*** Error setting up scan for reserves");
            Runtime.getRuntime().exit(1);
        }

        FileScan am = null;
        try {
            am  = new FileScan(SystemDefs.JavabaseDBName+"p3tempEfile22.in", etypes, esizes, 
                    (short)7, (short)7,
                    projlist, null);
        }
        catch (Exception e) {
            status = FAIL;
            System.err.println (""+e);
        }

        if (status != OK) {
            //bail out
            System.err.println ("*** Error setting up scan for sailors");
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
        }
        catch (Exception e) {
            System.err.println ("*** Error preparing for nested_loop_join");
            System.err.println (""+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

  
        t = null;

        try {
            while ((t = sm.get_next()) != null) {
                System.out.println(t.getIntFld(1)+" "+t.getStrFld(2)+" "+t.getStrFld(3)+" "+t.getIntFld(4)+" "+t.getIntFld(5)+" "+t.getStrFld(6)+" "+t.getStrFld(7));

            }
        }
        catch (Exception e) {
            System.err.println (""+e);
            e.printStackTrace();
            status = FAIL;
        }
        if (status != OK) {
            //bail out
            System.err.println ("*** Error in get next tuple ");
            Runtime.getRuntime().exit(1);
        }
        try {
            sm.close();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        System.out.println ("\n"); 
        if (status != OK) {
            //bail out
            System.err.println ("*** Error in closing ");
            Runtime.getRuntime().exit(1);
        }
        f.deleteFile();
        fN.deleteFile();
    }    

    public void joinNodeDEdge(String name, EdgeQueryPojo edgeQueryPojo) throws JoinsException,
            IndexException, PageNotReadException,
           TupleUtilsException, PredEvalException, SortException, LowMemException,
            UnknowAttrType, UnknownKeyTypeException, Exception {
        final boolean OK   = true;
        final boolean FAIL = false;
        boolean status=true;

        String graphDbName = name;
        CondExpr [] outfilter=new CondExpr[3];
        outfilter[0]=new CondExpr();
        outfilter[1]=new CondExpr();

        outfilter[0].next= null;
        outfilter[0].op=new AttrOperator(AttrOperator.aopEQ);
        outfilter[0].type1=new AttrType(AttrType.attrSymbol);
        outfilter[0].type2=new AttrType(AttrType.attrSymbol);
        outfilter[0].operand1.symbol=new FldSpec(new
                RelSpec(RelSpec.outer),2);
        outfilter[0].operand2.symbol=new
                FldSpec(new RelSpec(RelSpec.innerRel),7);

        outfilter[1].op    = new AttrOperator(AttrOperator.aopEQ);
        outfilter[1].next  = null;
        outfilter[1].type1 = new AttrType(AttrType.attrSymbol);
        outfilter[1].type2 = new AttrType(AttrType.attrString);
        if (edgeQueryPojo.getKey() == 4) {
            outfilter[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel), 7);
            outfilter[1].operand2.string = edgeQueryPojo.getDestLabel();
        } else {
            throw new Exception("Condition not valid; Need Edge Destination");
        }
        outfilter[2] = null;

//        Descriptor tempDesc = new Descriptor();
//        tempDesc.set(10,4, 10, 44, 5);
//        outfilter[1].op    = new AttrOperator(AttrOperator.aopEQ);
//        outfilter[1].next  = null;
//        outfilter[1].type1 = new AttrType(AttrType.attrSymbol);
//        outfilter[1].type2 = new AttrType(AttrType.attrDesc);
//        outfilter[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),7);
//        outfilter[1].operand2.desc =  tempDesc;

        Tuple t = new Tuple();
        AttrType[] etypes = new AttrType[7];
        etypes[0] = new AttrType(AttrType.attrInteger);
        etypes[1] = new AttrType(AttrType.attrInteger);
        etypes[2] = new AttrType(AttrType.attrInteger);
        etypes[3] = new AttrType(AttrType.attrInteger);
        etypes[4] = new AttrType(AttrType.attrInteger);
        etypes[5] = new AttrType(AttrType.attrString);
        etypes[6] = new AttrType(AttrType.attrString);

        AttrType [] ntypes={
                new AttrType(AttrType.attrDesc),
                new AttrType(AttrType.attrString),
        };
        short[] esizes = new short[2];
        esizes[0] = 20;
        esizes[1] = 20;
        short [] nsizes= new short[1];
        nsizes[0]=20;

        FldSpec[] proj1 = {
                new FldSpec(new RelSpec(RelSpec.outer),1),
                new FldSpec(new RelSpec(RelSpec.outer),2)
        };
        FldSpec[] projlist = new FldSpec[7];
        RelSpec rel = new RelSpec(RelSpec.innerRel);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);
        projlist[3] = new FldSpec(rel, 4);
        projlist[4] = new FldSpec(rel, 5);
        projlist[5] = new FldSpec(rel, 6);
        projlist[6] = new FldSpec(rel, 7);
        CondExpr [] selects= new CondExpr[1];
        selects[0]=null;

        iterator.Iterator nodeIndexIterator = null;

     //   SystemDefs.JavabaseBM.flushAllPages();

//        System.out.println("Unpinned pagesSHO: " + SystemDefs.JavabaseBM.getNumUnpinnedBuffers());



        IndexType b_index = new IndexType (IndexType.B_Index);
        try {
            nodeIndexIterator = new IndexScan( b_index, SystemDefs.JavabaseDBName+"_Node",
                    SystemDefs.JavabaseDBName+"_BTreeNodeIndex", ntypes, nsizes, 2, 2,
                    proj1, null, 2, false);
        }

        catch (Exception e) {
            System.err.println ("*** Error creating scan for Index scan");
            System.err.println (""+e);
            Runtime.getRuntime().exit(1);
        }


        //new hesp file

        Tuple nhp =new Tuple();
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
        nhp.setHdr((short)7, attrType, attrSize);

        EScan scan = null;
        boolean statusN = OK;


        Heapfile f = new Heapfile(null);

        if ( statusN == OK ) {  
            System.out.println ("  - Scan the records just inserted\n");

            try {
                scan = SystemDefs.JavabaseDB.ehfile.openScan();
            }
            catch (Exception e) {
                statusN = FAIL;
                System.err.println ("*** Error opening scan\n");
                e.printStackTrace();
            }

            if ( statusN == OK &&  SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                    == SystemDefs.JavabaseBM.getNumBuffers() ) {
                System.err.println ("*** The heap-file scan has not pinned the first page\n");
                statusN = FAIL;
            }
        }
        EID eidTmp = new EID();


        if ( status == OK ) {
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

//        System.out.println("Unpinned pagesB: " + SystemDefs.JavabaseBM.getNumUnpinnedBuffers());


        NestedLoopsJoins nlj=null;
        try {
            nlj = new NestedLoopsJoins(ntypes, 2, nsizes,
                    etypes, 7, esizes,
                    100,
                    nodeIndexIterator, f._fileName,
                    outfilter, null, projlist, 7);
        }
        catch (Exception e) {
            System.err.println ("*** Error preparing for nested_loop_join");
            System.err.println (""+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        Tuple node = null;

        iterator.Iterator am2=(iterator.Iterator)nlj;
        while ((t = am2.get_next()) != null) {
            System.out.println("Label:\t"+t.getStrFld(6)+ " "+t.getStrFld(7));
        }


        nlj.close();


    

        //        TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
        //        Sort sort_names = null;
        //        try {
        //            sort_names = new Sort (attrType, (short)7, attrSize,
        //                    (iterator.Iterator) nlj, 7, ascending, attrSize[1], 100);
        //        }
        //        catch (Exception e) {
        //            System.err.println ("*** Error preparing for nested_loop_join");
        //            System.err.println (""+e);
        //            Runtime.getRuntime().exit(1);
        //        }
        //
        //
        //        t = null;
        //        try {
        //            while ((t = sort_names.get_next()) != null) {
        //                System.out.println("Label:\t"+t.getStrFld(6)+ " "+t.getStrFld(7));
        //            }
        //        }
        //        catch (Exception e) {
        //            System.err.println (""+e);
        //            e.printStackTrace();
        //            Runtime.getRuntime().exit(1);
        //        }

        //  sort_names.close();
        f.deleteFile();
    }

    public void joinNodeSEdge(String name, EdgeQueryPojo edgeQueryPojo) throws JoinsException, IndexException, PageNotReadException,
            TupleUtilsException, PredEvalException, SortException, LowMemException,
            UnknowAttrType, UnknownKeyTypeException, Exception {
        final boolean OK   = true;
        final boolean FAIL = false;
        boolean status=true;

        CondExpr [] outfilter=new CondExpr[3];
        outfilter[0]=new CondExpr();
        outfilter[1]=new CondExpr();

        outfilter[0].next= null;
        outfilter[0].op=new AttrOperator(AttrOperator.aopEQ);
        outfilter[0].type1=new AttrType(AttrType.attrSymbol);
        outfilter[0].type2=new AttrType(AttrType.attrSymbol);
        outfilter[0].operand1.symbol=new FldSpec(new
                RelSpec(RelSpec.outer),2);
        outfilter[0].operand2.symbol=new
                FldSpec(new RelSpec(RelSpec.innerRel),7);

        outfilter[1].op    = new AttrOperator(AttrOperator.aopEQ);
        outfilter[1].next  = null;
        outfilter[1].type1 = new AttrType(AttrType.attrSymbol);
        outfilter[1].type2 = new AttrType(AttrType.attrString);
        if (edgeQueryPojo.getKey() == 3) {
            outfilter[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel), 7);
            outfilter[1].operand2.string = edgeQueryPojo.getSourceLabel();
        } else {
            throw new Exception("Condition not valid; Need Edge Source");
        }
        outfilter[2] = null;

//        Descriptor tempDesc = new Descriptor();
//        tempDesc.set(10,4, 10, 44, 5);
//        outfilter[1].op    = new AttrOperator(AttrOperator.aopEQ);
//        outfilter[1].next  = null;
//        outfilter[1].type1 = new AttrType(AttrType.attrSymbol);
//        outfilter[1].type2 = new AttrType(AttrType.attrDesc);
//        outfilter[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),7);
//        outfilter[1].operand2.desc =  tempDesc;

        Tuple t = new Tuple();
        AttrType[] etypes = new AttrType[7];
        etypes[0] = new AttrType(AttrType.attrInteger);
        etypes[1] = new AttrType(AttrType.attrInteger);
        etypes[2] = new AttrType(AttrType.attrInteger);
        etypes[3] = new AttrType(AttrType.attrInteger);
        etypes[4] = new AttrType(AttrType.attrInteger);
        etypes[5] = new AttrType(AttrType.attrString);
        etypes[6] = new AttrType(AttrType.attrString);

        AttrType [] ntypes={
                new AttrType(AttrType.attrDesc),
                new AttrType(AttrType.attrString),
        };
        short[] esizes = new short[2];
        esizes[0] = 20;
        esizes[1] = 20;
        short [] nsizes= new short[1];
        nsizes[0]=20;

        FldSpec[] proj1 = {
                new FldSpec(new RelSpec(RelSpec.outer),1),
                new FldSpec(new RelSpec(RelSpec.outer),2)
        };
        FldSpec[] projlist = new FldSpec[7];
        RelSpec rel = new RelSpec(RelSpec.innerRel);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);
        projlist[3] = new FldSpec(rel, 4);
        projlist[4] = new FldSpec(rel, 5);
        projlist[5] = new FldSpec(rel, 6);
        projlist[6] = new FldSpec(rel, 7);
        CondExpr [] selects= new CondExpr[1];
        selects[0]=null;

        iterator.Iterator nodeIndexIterator = null;

        //   SystemDefs.JavabaseBM.flushAllPages();

//        System.out.println("Unpinned pagesSHO: " + SystemDefs.JavabaseBM.getNumUnpinnedBuffers());



        IndexType b_index = new IndexType (IndexType.B_Index);
        try {
            nodeIndexIterator = new IndexScan( b_index, SystemDefs.JavabaseDBName+"_Node",
                    SystemDefs.JavabaseDBName+"_BTreeNodeIndex", ntypes, nsizes, 2, 2,
                    proj1, null, 2, false);
        }

        catch (Exception e) {
            System.err.println ("*** Error creating scan for Index scan");
            System.err.println (""+e);
            Runtime.getRuntime().exit(1);
        }


        //new hesp file

        Tuple nhp =new Tuple();
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
        nhp.setHdr((short)7, attrType, attrSize);

        EScan scan = null;
        boolean statusN = OK;


        Heapfile f = new Heapfile(null);

        if ( statusN == OK ) {
            System.out.println ("  - Scan the records just inserted\n");

            try {
                scan = SystemDefs.JavabaseDB.ehfile.openScan();
            }
            catch (Exception e) {
                statusN = FAIL;
                System.err.println ("*** Error opening scan\n");
                e.printStackTrace();
            }

            if ( statusN == OK &&  SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                    == SystemDefs.JavabaseBM.getNumBuffers() ) {
                System.err.println ("*** The heap-file scan has not pinned the first page\n");
                statusN = FAIL;
            }
        }
        EID eidTmp = new EID();


        if ( status == OK ) {
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
                f.insertRecord(nhp.getTupleByteArray());
            }
        }
        scan.closescan();

//        System.out.println("Unpinned pagesB: " + SystemDefs.JavabaseBM.getNumUnpinnedBuffers());


        NestedLoopsJoins nlj=null;
        try {
            nlj = new NestedLoopsJoins(ntypes, 2, nsizes,
                    etypes, 7, esizes,
                    100,
                    nodeIndexIterator, f._fileName,
                    outfilter, null, projlist, 7);
        }
        catch (Exception e) {
            System.err.println ("*** Error preparing for nested_loop_join");
            System.err.println (""+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        Tuple node = null;

        iterator.Iterator am2=(iterator.Iterator)nlj;
        while ((t = am2.get_next()) != null) {
            System.out.println("Label:\t"+t.getStrFld(6)+ " "+t.getStrFld(7));
        }
        nlj.close();
        f.deleteFile();
    }

    public void joinEdgeSNode(String name, NodeQueryPojo nodeQueryPojo) throws JoinsException, IndexException, PageNotReadException,
            TupleUtilsException, PredEvalException, SortException, LowMemException,
            UnknowAttrType, UnknownKeyTypeException, Exception {
        final boolean OK   = true;
        final boolean FAIL = false;
        boolean status=true;

        CondExpr [] outfilter=new CondExpr[3];
        outfilter[0]=new CondExpr();
        outfilter[1]=new CondExpr();

        outfilter[0].next= null;
        outfilter[0].op=new AttrOperator(AttrOperator.aopEQ);
        outfilter[0].type1=new AttrType(AttrType.attrSymbol);
        outfilter[0].type2=new AttrType(AttrType.attrSymbol);
        outfilter[0].operand1.symbol=new FldSpec(new
                RelSpec(RelSpec.outer),7);
        outfilter[0].operand2.symbol=new
                FldSpec(new RelSpec(RelSpec.innerRel),2);

        outfilter[1].op    = new AttrOperator(AttrOperator.aopEQ);
        outfilter[1].next  = null;
        outfilter[1].type1 = new AttrType(AttrType.attrSymbol);
        if (nodeQueryPojo.getKey() == 1) {
            outfilter[1].type2 = new AttrType(AttrType.attrString);
            outfilter[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel), 2);
            outfilter[1].operand2.string = nodeQueryPojo.getLabel();
        } else if (nodeQueryPojo.getKey() == 2) {
            outfilter[1].type2 = new AttrType(AttrType.attrDesc);
            outfilter[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel), 1);
            outfilter[1].operand2.desc = nodeQueryPojo.getDesc();
        } else {
            throw new Exception("Condition not valid; Need Edge Source");
        }
        outfilter[2] = null;

        Tuple t = new Tuple();
        AttrType[] etypes = new AttrType[7];
        etypes[0] = new AttrType(AttrType.attrInteger);
        etypes[1] = new AttrType(AttrType.attrInteger);
        etypes[2] = new AttrType(AttrType.attrInteger);
        etypes[3] = new AttrType(AttrType.attrInteger);
        etypes[4] = new AttrType(AttrType.attrInteger);
        etypes[5] = new AttrType(AttrType.attrString);
        etypes[6] = new AttrType(AttrType.attrString);

        AttrType [] ntypes={
                new AttrType(AttrType.attrDesc),
                new AttrType(AttrType.attrString),
        };
        short[] esizes = new short[2];
        esizes[0] = 20;
        esizes[1] = 20;
        short [] nsizes= new short[1];
        nsizes[0]=20;

        FldSpec[] proj1 = {
                new FldSpec(new RelSpec(RelSpec.outer),1),
                new FldSpec(new RelSpec(RelSpec.outer),2)
        };
        FldSpec[] projlist = new FldSpec[3];
        projlist[0] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        projlist[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
        projlist[2] = new FldSpec(new RelSpec(RelSpec.outer), 6);
        CondExpr [] selects= new CondExpr[1];
        selects[0]=null;

        //new hesp file

        Tuple nhp =new Tuple();
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
        nhp.setHdr((short)7, attrType, attrSize);

        EScan scan = null;
        boolean statusN = OK;


        Heapfile f = new Heapfile(null);
        // create the index file
        BTreeFile btf = null;
        try {
            btf = new BTreeFile("EdgeSourceIndex", AttrType.attrString, 20, 1);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        RID rid = new RID();
        String key = "";
        Tuple temp = null;


        if ( statusN == OK ) {
            System.out.println ("  - Scan the records just inserted\n");

            try {
                scan = SystemDefs.JavabaseDB.ehfile.openScan();
            }
            catch (Exception e) {
                statusN = FAIL;
                System.err.println ("*** Error opening scan\n");
                e.printStackTrace();
            }

            if ( statusN == OK &&  SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                    == SystemDefs.JavabaseBM.getNumBuffers() ) {
                System.err.println ("*** The heap-file scan has not pinned the first page\n");
                statusN = FAIL;
            }
        }
        EID eidTmp = new EID();


        if ( status == OK ) {
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
                rid = f.insertRecord(nhp.getTupleByteArray());

                try {
                    key = nhp.getStrFld(7);
                }
                catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

                try {
                    btf.insert(new StringKey(key), rid);
                } catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }
            }
        }
        btf.close();
        scan.closescan();

        FldSpec[] edgeFileProjList = new FldSpec[7];
        RelSpec rel = new RelSpec(RelSpec.outer);
        edgeFileProjList[0] = new FldSpec(rel, 1);
        edgeFileProjList[1] = new FldSpec(rel, 2);
        edgeFileProjList[2] = new FldSpec(rel, 3);
        edgeFileProjList[3] = new FldSpec(rel, 4);
        edgeFileProjList[4] = new FldSpec(rel, 5);
        edgeFileProjList[5] = new FldSpec(rel, 6);
        edgeFileProjList[6] = new FldSpec(rel, 7);

        Iterator edgeIterator = null;

        IndexType b_index = new IndexType (IndexType.B_Index);
        try {
            edgeIterator = new IndexScan( b_index, f._fileName,
                    "EdgeSourceIndex", etypes, esizes, 7, 7,
                    edgeFileProjList, null, 7, false);
        } catch (Exception e) {
            System.err.println ("*** Error creating scan for Index scan");
            System.err.println (""+e);
            Runtime.getRuntime().exit(1);
        }

//        NestedLoopsJoins nlj=null;
//        try {
//            nlj = new NestedLoopsJoins(etypes, 7, esizes,
//                    ntypes, 2, nsizes,
//                    100, edgeIterator
//                    , SystemDefs.JavabaseDBName + "_Node",
//                    outfilter, null, projlist, 3);
//        }
//        catch (Exception e) {
//            System.err.println ("*** Error preparing for nested_loop_join");
//            System.err.println (""+e);
//            e.printStackTrace();
//            Runtime.getRuntime().exit(1);
//        }

//        Iterator am2 = (Iterator)nlj;
        while ((t = edgeIterator.get_next()) != null) {
            System.out.println(t.getStrFld(7));
//            System.out.println("Descriptor :\t"+
//                    t.getDescFld(1).toString()+
//                    " Node Label"+ t.getStrFld(2) +
//                    " Edge Label : " + t.getStrFld(3));
        }

//        nlj.close();
        f.deleteFile();
    }
}
