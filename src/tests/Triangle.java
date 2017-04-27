package tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import bufmgr.PageNotReadException;
import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.TupleOrder;
import heap.Heapfile;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;
import iterator.CondExpr;
import iterator.DuplElim;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.NestedLoopsJoins;
import iterator.PredEvalException;
import iterator.RelSpec;
import iterator.Sort;
import iterator.SortException;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;
import queryPojo.EdgeQueryPojo;
import queryPojo.NodeQueryPojo;
import queryPojo.Pair;

public class Triangle {
    public void triangleJoin(String name, List<EdgeQueryPojo> edgeQueryPojoList, String Type) throws JoinsException, IndexException, PageNotReadException,
            TupleUtilsException, PredEvalException, SortException, LowMemException,
            UnknowAttrType, UnknownKeyTypeException, Exception {
        EdgeQueryPojo edgeQueryPojo = edgeQueryPojoList.get(0);
        EdgeQueryPojo edgeQueryPojo1 = edgeQueryPojoList.get(1);
        EdgeQueryPojo edgeQueryPojo2 = edgeQueryPojoList.get(2);
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

        short[] esizes = new short[3];
        esizes[0] = 20;
        esizes[1] = 20;
        esizes[2] = 20;
        short[] nsizes = new short[1];
        nsizes[0] = 20;

        FldSpec[] edgeFileProjList = new FldSpec[8];
        RelSpec rel = new RelSpec(RelSpec.outer);
        edgeFileProjList[0] = new FldSpec(rel, 1);
        edgeFileProjList[1] = new FldSpec(rel, 6);
        edgeFileProjList[2] = new FldSpec(rel, 7);
        edgeFileProjList[3] = new FldSpec(rel, 8);
        RelSpec relN = new RelSpec(RelSpec.innerRel);
        edgeFileProjList[4] = new FldSpec(relN, 1);
        edgeFileProjList[5] = new FldSpec(relN, 6);
        edgeFileProjList[6] = new FldSpec(relN, 7);
        edgeFileProjList[7] = new FldSpec(relN, 8);


        AttrType[] nlj1types = new AttrType[8];
        nlj1types[0] = new AttrType(AttrType.attrInteger);
        nlj1types[1] = new AttrType(AttrType.attrString);
        nlj1types[2] = new AttrType(AttrType.attrString);
        nlj1types[3] = new AttrType(AttrType.attrString);
        nlj1types[4] = new AttrType(AttrType.attrInteger);
        nlj1types[5] = new AttrType(AttrType.attrString);
        nlj1types[6] = new AttrType(AttrType.attrString);
        nlj1types[7] = new AttrType(AttrType.attrString);

        short[] nlj1sizes = new short[6];
        nlj1sizes[0] = 20;
        nlj1sizes[1] = 20;
        nlj1sizes[2] = 20;
        nlj1sizes[3] = 20;
        nlj1sizes[4] = 20;
        nlj1sizes[5] = 20;

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
        iterator.Iterator edgeIterator = null;

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
        NestedLoopsJoins nlj = null;
        try {
            nlj = new NestedLoopsJoins(etypes, 8, esizes,
                    etypes, 8, esizes,
                    100,
                    edgeIterator, "UniqueEdge",
                    outfilter, null, edgeFileProjList, 8);
        } catch (Exception e) {
            System.err.println("*** Error preparing for nested_loop_join");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        CondExpr[] outfilter1 = new CondExpr[5];
        outfilter1[0] = new CondExpr();
        outfilter1[1] = new CondExpr();
        outfilter1[2] = new CondExpr();
        outfilter1[3] = new CondExpr();
        outfilter1[4] = new CondExpr();

        outfilter1[0].next = null;
        outfilter1[0].op = new AttrOperator(AttrOperator.aopEQ);
        outfilter1[0].type1 = new AttrType(AttrType.attrSymbol);
        outfilter1[0].type2 = new AttrType(AttrType.attrSymbol);
        outfilter1[0].operand1.symbol = new FldSpec(new
                RelSpec(RelSpec.outer), 8);
        outfilter1[0].operand2.symbol = new
                FldSpec(new RelSpec(RelSpec.innerRel), 7);
        outfilter1[4] = null;

        if (edgeQueryPojo1.getKey() == 1) {
            outfilter1[1].op = new AttrOperator(AttrOperator.aopEQ);
            outfilter1[1].next = null;
            outfilter1[1].type1 = new AttrType(AttrType.attrSymbol);
            outfilter1[1].type2 = new AttrType(AttrType.attrString);
            outfilter1[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 6);
            outfilter1[1].operand2.string = edgeQueryPojo1.getEdgelabel();
        } else if (edgeQueryPojo1.getKey() == 2) {
            outfilter1[1].op = new AttrOperator(AttrOperator.aopLE);
            outfilter1[1].next = null;
            outfilter1[1].type1 = new AttrType(AttrType.attrSymbol);
            outfilter1[1].type2 = new AttrType(AttrType.attrInteger);
            outfilter1[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 5);
            outfilter1[1].operand2.integer = edgeQueryPojo1.getWeight();
        } else {
            throw new Exception("Condition not valid; Need Edge Source");
        }
        if (edgeQueryPojo2.getKey() == 1) {
            outfilter1[2].op = new AttrOperator(AttrOperator.aopEQ);
            outfilter1[2].next = null;
            outfilter1[2].type1 = new AttrType(AttrType.attrSymbol);
            outfilter1[2].type2 = new AttrType(AttrType.attrString);
            outfilter1[2].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 6);
            outfilter1[2].operand2.string = edgeQueryPojo2.getEdgelabel();
        } else if (edgeQueryPojo2.getKey() == 2) {
            outfilter1[2].op = new AttrOperator(AttrOperator.aopLE);
            outfilter1[2].next = null;
            outfilter1[2].type1 = new AttrType(AttrType.attrSymbol);
            outfilter1[2].type2 = new AttrType(AttrType.attrInteger);
            outfilter1[2].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
            outfilter1[2].operand2.integer = edgeQueryPojo2.getWeight();
        } else {
            throw new Exception("Condition not valid; Need Edge Source");
        }

        outfilter1[3].op = new AttrOperator(AttrOperator.aopEQ);
        outfilter1[3].next = null;
        outfilter1[3].type1 = new AttrType(AttrType.attrSymbol);
        outfilter1[3].type2 = new AttrType(AttrType.attrSymbol);
        outfilter1[3].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 8);
        outfilter1[3].operand2.symbol =  new FldSpec(new RelSpec(RelSpec.outer), 3);

        FldSpec[] edgeFileProjListFinal = new FldSpec[9];
        edgeFileProjListFinal[0] = new FldSpec(rel, 1);
        edgeFileProjListFinal[1] = new FldSpec(rel, 6);
        edgeFileProjListFinal[2] = new FldSpec(rel, 7);
        edgeFileProjListFinal[3] = new FldSpec(rel, 8);
        edgeFileProjListFinal[4] = new FldSpec(rel, 3);
        edgeFileProjListFinal[5] = new FldSpec(relN, 1);
        edgeFileProjListFinal[6] = new FldSpec(relN, 6);
        edgeFileProjListFinal[7] = new FldSpec(relN, 7);
        edgeFileProjListFinal[8] = new FldSpec(relN, 8);
        NestedLoopsJoins nlj2 = null ;
        try {
            nlj2 = new NestedLoopsJoins (nlj1types, 8, nlj1sizes,
                    etypes, 8, esizes,
                    100,
                    nlj, "UniqueEdge",
                    outfilter1, null, edgeFileProjListFinal, 9);
        }
        catch (Exception e) {
            System.err.println ("*** Error preparing for nested_loop_join");
            System.err.println (""+e);
            Runtime.getRuntime().exit(1);
        }

        Tuple t = new Tuple();
        List<String> ansNids= new ArrayList<String>();

        while ((t = nlj2.get_next()) != null) {
            String Tri =  t.getStrFld(5)+" "+t.getStrFld(3)+" "+t.getStrFld(8);
            ansNids.add(Tri);
            if(Type == "a") {
                System.out.print(t.getStrFld(5)+" -> ");
                System.out.print(t.getStrFld(3)+" -> ");
                System.out.println(t.getStrFld(8));
            }
            //System.out.println("Labels: "+ t.getStrFld(5) + " ---> "+t.getStrFld(3)+ " ----> "+t.getStrFld(8)+"  "+t.getIntFld(1)+"  "+t.getIntFld(6));
        }

        nlj.close();
        nlj2.close();
        edgeIterator.close();

        if (Type.equalsIgnoreCase("a")) {
            return;
        }

        if (Type.equalsIgnoreCase("b")) {
            // CREATE Temp Heap File
            Tuple nhp =new Tuple();
            AttrType[] attrType = new AttrType[1];
            attrType[0] = new AttrType(AttrType.attrString);

            short[] attrSize = new short[1];
            attrSize[0] = 30;
            nhp.setHdr((short)1, attrType, attrSize);

            Heapfile f= new Heapfile("triangleTemp");

            for (int j=0;j<ansNids.size();j++) {
                String []tokens = ansNids.get(j).trim().split(" ");
                String formatted = String.format("%05d", Integer.parseInt(tokens[0]))
                        + "_" + String.format("%05d", Integer.parseInt(tokens[1]))
                        + "_" + String.format("%05d", Integer.parseInt(tokens[2]));
                nhp.setStrFld(1, formatted);
                f.insertRecord(nhp.getTupleByteArray());
            }

            // SORT the file just created
            FldSpec[] projlistTemp = new FldSpec[1];
            projlistTemp[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
            TupleOrder[] order = new TupleOrder[1];
            order[0] = new TupleOrder(TupleOrder.Ascending);
            int numPages = 30;

            FileScan fscan = null;
            try {
                fscan = new FileScan(f._fileName, attrType, attrSize, (short) 1, 1, projlistTemp, null);
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            Sort sort = null;
            try {
                sort = new Sort(attrType, (short) 1, attrSize, fscan, 1, order[0], 30, numPages);
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            t = null;

            try {
                t = sort.get_next();
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            boolean flag = true;
            while (t != null) {
                try {
                    System.out.print(String.valueOf(Integer.parseInt(t.getStrFld(1).substring(0,5)))+" -> ");
                    System.out.print(String.valueOf(Integer.parseInt(t.getStrFld(1).substring(6,11)))+" -> ");
                    System.out.println(String.valueOf(Integer.parseInt(t.getStrFld(1).substring(12,17))));
                    t = sort.get_next();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
            // clean up
            try {
                f.deleteFile();
                fscan.close();
                sort.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (Type.equalsIgnoreCase("c")) {
            // CREATE Temp Heap File
            Tuple nhp =new Tuple();
            AttrType[] attrType = new AttrType[1];
            attrType[0] = new AttrType(AttrType.attrString);
            int numPages = 30;
            TupleOrder[] order = new TupleOrder[1];
            order[0] = new TupleOrder(TupleOrder.Ascending);
            FldSpec[] projlistTemp = new FldSpec[1];
            projlistTemp[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
         
        
            short[] attrSize = new short[1];
            attrSize[0] = 30;
            nhp.setHdr((short)1, attrType, attrSize);

            Heapfile f= new Heapfile("triangleTemp");

            for (int j=0;j<ansNids.size();j++) {
                String []tokens = ansNids.get(j).trim().split(" ");
                
                
                Tuple cur =new Tuple();
                AttrType[] attrTypeNode = new AttrType[1];
                attrTypeNode[0] = new AttrType(AttrType.attrInteger);
                short[] attrSizData = new short[0];
                
                cur.setHdr((short)1, attrTypeNode, attrSizData);

                Heapfile fNew= new Heapfile("triangelTripleData");
                cur.setIntFld(1, Integer.parseInt(tokens[0]));
                fNew.insertRecord(cur.getTupleByteArray());
                cur.setIntFld(1, Integer.parseInt(tokens[1]));
                fNew.insertRecord(cur.getTupleByteArray());
                cur.setIntFld(1, Integer.parseInt(tokens[2]));
                fNew.insertRecord(cur.getTupleByteArray());
               
                FileScan fscanD = null;
                try {
                	fscanD = new FileScan(fNew._fileName, attrTypeNode, attrSizData, (short) 1, 1, projlistTemp, null);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }

                Sort sortD = null;
                try {
                	sortD = new Sort(attrTypeNode, (short) 1, attrSizData, fscanD, 1, order[0], 30, numPages);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                cur = null;
                String tripleData = "";
                while ((cur = sortD.get_next()) != null) {
                tripleData += cur.getIntFld(1) + " ";
                }
                fscanD.close();
                sortD.close();
                fNew.deleteFile();
                
                tokens = tripleData.trim().split(" ");
                String formatted = String.format("%05d", Integer.parseInt(tokens[0]))
                        + "_" + String.format("%05d", Integer.parseInt(tokens[1]))
                        + "_" + String.format("%05d", Integer.parseInt(tokens[2]));

                nhp.setStrFld(1, formatted);
                f.insertRecord(nhp.getTupleByteArray());
            }

            // SORT the file just created
           
            FileScan fscan = null;
            try {
                fscan = new FileScan(f._fileName, attrType, attrSize, (short) 1, 1, projlistTemp, null);
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            Sort sort = null;
            try {
                sort = new Sort(attrType, (short) 1, attrSize, fscan, 1, order[0], 30, numPages);
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            // Type = "c"
            DuplElim ed = null;
            try {
                ed = new DuplElim(attrType, (short)1, attrSize, sort, 10, true);
            }
            catch (Exception e) {
                System.err.println (""+e);
                Runtime.getRuntime().exit(1);
            }

            int count = 0;
            t = null;
            String outval = null;

            try {
                t = ed.get_next();
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            boolean flag = true;
            while (t != null) {
                try {
                    System.out.print(String.valueOf(Integer.parseInt(t.getStrFld(1).substring(0,5)))+" -> ");
                    System.out.print(String.valueOf(Integer.parseInt(t.getStrFld(1).substring(6,11)))+" -> ");
                    System.out.println(String.valueOf(Integer.parseInt(t.getStrFld(1).substring(12,17))));
                    t = ed.get_next();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }

            // clean up
            try {
                fscan.close();
                sort.close();
                f.deleteFile();
                ed.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}