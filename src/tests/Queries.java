package tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import bufmgr.PageNotReadException;
import diskmgr.PCounter;
import edgeheap.EScan;
import edgeheap.Edge;
import global.AttrType;
import global.Descriptor;
import global.EID;
import global.NID;
import global.PageId;
import global.SystemDefs;
import global.TupleOrder;
import heap.Heapfile;
import heap.Tuple;
import index.IndexException;
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
import iterator.SortMerge;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;
import queryPojo.EdgeQueryPojo;
import queryPojo.NodeQueryPojo;
import queryPojo.Pair;

public class Queries {

    public static List<Pair<NodeQueryPojo, NodeQueryPojo>> queryPQ1(String name, List<NodeQueryPojo> nodeQuery, String Type) throws JoinsException, IndexException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        List<Pair<NodeQueryPojo, NodeQueryPojo>> ansNids = new ArrayList<Pair<NodeQueryPojo, NodeQueryPojo>>();
        int i = 0;
        List<Pair<NodeQueryPojo, NodeQueryPojo>> childQuery = new ArrayList<Pair<NodeQueryPojo, NodeQueryPojo>>();

        while (nodeQuery.size() > 1) {
        	int reads = PCounter.readCounter, writes = PCounter.writeCounter;
            if (i == 0) {
                i = 1;

                childQuery.add(new Pair<NodeQueryPojo, NodeQueryPojo>(nodeQuery.get(0), nodeQuery.get(0)));
            }
            Iterator<Pair<NodeQueryPojo, NodeQueryPojo>> iterQuery = childQuery.iterator();
            List<Pair<NodeQueryPojo, NodeQueryPojo>> children = new ArrayList<Pair<NodeQueryPojo, NodeQueryPojo>>();

            while (iterQuery.hasNext()) {

                NestedLoopsJoins nlj;
                Pair<NodeQueryPojo, NodeQueryPojo> nextIterQuery = iterQuery.next();
                nlj = new Join().joinNodeSEdge(name, nextIterQuery.getRight());
                Tuple t = new Tuple();
                iterator.Iterator joinResult = (iterator.Iterator) nlj;

                while ((t = joinResult.get_next()) != null) {
                    NodeQueryPojo tmp = new NodeQueryPojo();
                    tmp.setLabel(t.getStrFld(8)); // has destination Label

                    //                    NID destNid2 = new NID();
                    //                    destNid2.pageNo=new PageId(t.getIntFld(4));
                    //                    destNid2.slotNo=t.getIntFld(5);
                    //                    System.out.println("Dest Page No: "+t.getIntFld(4) +" slot: "+ t.getIntFld(5));
                    //                    Descriptor destDisc2 = new Descriptor();
                    //                    destDisc2 =  SystemDefs.JavabaseDB.nhfile.getNode(destNid2).getDesc();
                    //                    System.out.println(" Descriptor"+ destDisc2.getString());

                    NID destNid = new NID();
                    destNid.pageNo = new PageId(t.getIntFld(4));
                    destNid.slotNo = t.getIntFld(5);

                    if (nodeQuery.get(1).getKey() == 2) {
                        Descriptor destDisc = new Descriptor();
                        destDisc = SystemDefs.JavabaseDB.nhfile.getNode(destNid).getDesc();
                        //System.out.println("Destination: "+ t.getStrFld(8) +" Des: "+destDisc.getString2());
                        if (destDisc.equal(nodeQuery.get(1).getDesc()) == 1) {
                            {
                                if (nextIterQuery.getLeft().getLabel() == null) {
                                    NodeQueryPojo rtemp = new NodeQueryPojo();
                                    rtemp.setLabel(t.getStrFld(7));
                                    children.add(new Pair<NodeQueryPojo, NodeQueryPojo>(rtemp, tmp));
                                } else
                                    children.add(new Pair<NodeQueryPojo, NodeQueryPojo>(nextIterQuery.getLeft(), tmp));

                                if (nodeQuery.size() == 2) {
                                    NodeQueryPojo destNidPojo = new NodeQueryPojo();
                                    destNidPojo.setNd(destNid);
                                    if (nextIterQuery.getLeft().getLabel() == null) {
                                        NodeQueryPojo rtemp = new NodeQueryPojo();
                                        rtemp.setLabel(t.getStrFld(7));
                                        ansNids.add(new Pair<NodeQueryPojo, NodeQueryPojo>(rtemp, destNidPojo));
                                    } else
                                        ansNids.add(new Pair<NodeQueryPojo, NodeQueryPojo>(nextIterQuery.getLeft(), destNidPojo));
                                }
                            }
                        }
                    } else {
                        if (t.getStrFld(8).equals(nodeQuery.get(1).getLabel())) {

                            if (nextIterQuery.getLeft().getLabel() == null) {
                                NodeQueryPojo rtemp = new NodeQueryPojo();
                                rtemp.setLabel(t.getStrFld(7));
                                children.add(new Pair<NodeQueryPojo, NodeQueryPojo>(rtemp, tmp));
                            } else
                                children.add(new Pair<NodeQueryPojo, NodeQueryPojo>(nextIterQuery.getLeft(), tmp));

                            if (nodeQuery.size() == 2) {
                                NodeQueryPojo destNidPojo = new NodeQueryPojo();
                                destNidPojo.setNd(destNid);
                                if (nextIterQuery.getLeft().getLabel() == null) {
                                    NodeQueryPojo rtemp = new NodeQueryPojo();
                                    rtemp.setLabel(t.getStrFld(7));
                                    ansNids.add(new Pair<NodeQueryPojo, NodeQueryPojo>(rtemp, destNidPojo));
                                } else
                                    ansNids.add(new Pair<NodeQueryPojo, NodeQueryPojo>(nextIterQuery.getLeft(), destNidPojo));
                            }
                        }
                    }

                }
                nlj.close();
                System.out.print("\tDisk Read Count: "+ Integer.toString(PCounter.readCounter-reads));        

                System.out.println("\tDisk Write Count: "+ Integer.toString(PCounter.writeCounter-writes));
            }

            nodeQuery.remove(0);
            childQuery = children;
        }


        // CREATE Temp Heap File
        List<Pair<NodeQueryPojo, NodeQueryPojo>> ansNidsSorted = new ArrayList<Pair<NodeQueryPojo, NodeQueryPojo>>();
        Tuple nhp = new Tuple();
        AttrType[] attrType = new AttrType[1];
        attrType[0] = new AttrType(AttrType.attrString);

        short[] attrSize = new short[1];
        attrSize[0] = 20;
        nhp.setHdr((short) 1, attrType, attrSize);

        Heapfile f = new Heapfile(null);
        for (int combinedTuple = 0; combinedTuple < ansNids.size(); combinedTuple++) {
            nhp.setStrFld(1, String.format("%05d", Integer.parseInt(ansNids.get(combinedTuple).getLeft().getLabel()))
                    + "_" + String.format("%05d", Integer.parseInt(ansNids.get(combinedTuple).getRight().getLabel())));
            f.insertRecord(nhp.getTupleByteArray());
        }

        if (Type.equalsIgnoreCase("a")) {
            // clean up
            try {

                f.deleteFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return ansNids;
        }

        // SORT the file just created
        FldSpec[] projlist = new FldSpec[1];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        TupleOrder[] order = new TupleOrder[1];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        int numPages = 30;

        FileScan fscan = null;
        try {
            fscan = new FileScan(f._fileName, attrType, attrSize, (short) 1, 1, projlist, null);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Sort sort = null;
        try {
            sort = new Sort(attrType, (short) 1, attrSize, fscan, 1, order[0], 20, numPages);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (Type.equalsIgnoreCase("b")) {
            int count = 0;
            Tuple t = null;
            String outval = null;

            try {
                t = sort.get_next();
            } catch (Exception e) {
                e.printStackTrace();
            }

            boolean flag = true;
            while (t != null) {
                try {
                    NodeQueryPojo head = new NodeQueryPojo();
                    NodeQueryPojo tail = new NodeQueryPojo();
                    head.setLabel(String.valueOf(Integer.parseInt(t.getStrFld(1).substring(0, 5))));
                    tail.setLabel(String.valueOf(Integer.parseInt(t.getStrFld(1).substring(6, 11))));
                    ansNidsSorted.add(new Pair<NodeQueryPojo, NodeQueryPojo>(head, tail));
                    t = sort.get_next();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            // clean up
            try {
                fscan.close();
                sort.close();
                f.deleteFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return ansNidsSorted;
        }

        // Type = "c"
        DuplElim ed = null;
        try {
            ed = new DuplElim(attrType, (short) 1, attrSize, sort, 10, true);
        } catch (Exception e) {
            System.err.println("" + e);
            Runtime.getRuntime().exit(1);
        }

        int count = 0;
        Tuple t = null;
        String outval = null;

        try {
            t = ed.get_next();
        } catch (Exception e) {
            e.printStackTrace();
        }

        boolean flag = true;
        while (t != null) {
            try {
                NodeQueryPojo head = new NodeQueryPojo();
                NodeQueryPojo tail = new NodeQueryPojo();
                head.setLabel(String.valueOf(Integer.parseInt(t.getStrFld(1).substring(0, 5))));
                tail.setLabel(String.valueOf(Integer.parseInt(t.getStrFld(1).substring(6, 11))));
                ansNidsSorted.add(new Pair<NodeQueryPojo, NodeQueryPojo>(head, tail));
                t = ed.get_next();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // clean up
        try {
            fscan.close();
            sort.close();
            ed.close();
            f.deleteFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return ansNidsSorted;
    }

    public static List<NodeQueryPojo> queryPE1(String name, List<NodeQueryPojo> nodeQuery) throws JoinsException, IndexException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {

        List<NodeQueryPojo> ansNids = new ArrayList<NodeQueryPojo>();
        int i = 0;
        List<NodeQueryPojo> childQuery = new ArrayList<NodeQueryPojo>();

        while (nodeQuery.size() > 1) {
        	int reads = PCounter.readCounter, writes = PCounter.writeCounter;
            if (i == 0) {
                i = 1;
                childQuery.add(nodeQuery.get(0));
            }
            Iterator<NodeQueryPojo> iterQuery = childQuery.iterator();
            List<NodeQueryPojo> children = new ArrayList<NodeQueryPojo>();

            while (iterQuery.hasNext()) {

                NestedLoopsJoins nlj;

                nlj = new Join().joinNodeSEdge(name, iterQuery.next());
                Tuple t = new Tuple();
                iterator.Iterator joinResult = (iterator.Iterator) nlj;

                while ((t = joinResult.get_next()) != null) {
                    NodeQueryPojo tmp = new NodeQueryPojo();
                    tmp.setLabel(t.getStrFld(8)); // has destination Label
                    NID destNid = new NID();
                    destNid.pageNo = new PageId(t.getIntFld(4));
                    destNid.slotNo = t.getIntFld(5);

                    if (nodeQuery.get(1).getKey() == 2) {
                        Descriptor destDisc = new Descriptor();
                        destDisc = SystemDefs.JavabaseDB.nhfile.getNode(destNid).getDesc();
                        //System.out.println("Destination: "+ t.getStrFld(8) +" Des: "+destDisc.getString2());
                        if (destDisc.equal(nodeQuery.get(1).getDesc()) == 1) {
                            {
                                children.add(tmp);

                                if (nodeQuery.size() == 2) {
                                    NodeQueryPojo destNidPojo = new NodeQueryPojo();
                                    destNidPojo.setNd(destNid);
                                    ansNids.add(destNidPojo);

                                }
                            }
                        }
                    } else {
                        if (t.getStrFld(8).equals(nodeQuery.get(1).getLabel())) {
                            children.add(tmp);

                            if (nodeQuery.size() == 2) {
                                NodeQueryPojo destNidPojo = new NodeQueryPojo();
                                destNidPojo.setNd(destNid);
                                ansNids.add(destNidPojo);
                            }
                        }
                    }

                }
                nlj.close();
                System.out.print("\tDisk Read Count: "+ Integer.toString(PCounter.readCounter-reads));        

                System.out.println("\tDisk Write Count: "+ Integer.toString(PCounter.writeCounter-writes));
            }

            nodeQuery.remove(0);
            childQuery = children;
        }
        return ansNids;
    }
//    public static List<EdgeQueryPojo> queryPE2(String name,List<EdgeQueryPojo> edgeQuery) throws JoinsException, IndexException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception
//    {
//        List<EdgeQueryPojo> ansNids= new LinkedList<EdgeQueryPojo>();
//        List<EdgeQueryPojo> childQuery = new LinkedList<EdgeQueryPojo>();
//        List<NodeQueryPojo> nchildQuery = new LinkedList<NodeQueryPojo>();
//        int i=0;
//
//        while(edgeQuery.size()>1)
//        {
//            List<EdgeQueryPojo> children = new LinkedList<EdgeQueryPojo>();
//            if(i==0)
//            {
//
//                NID nid=edgeQuery.get(0).getNd();
//                String node_label=SystemDefs.JavabaseDB.nhfile.getNode(nid).getLabel();
//                NodeQueryPojo npj=new NodeQueryPojo();
//                npj.setLabel(node_label);
//                nchildQuery.add(npj);
//                Iterator<NodeQueryPojo> iterQuery = nchildQuery.iterator();
//                while (iterQuery.hasNext()) {
//                    NestedLoopsJoins nlj;
//
//                    nlj  = new Join().joinNodeSEdge(name, iterQuery.next());
//                    Tuple t = new Tuple();
//                    iterator.Iterator joinResult=(iterator.Iterator)nlj;
//                    while ((t = joinResult.get_next()) != null) {
//                        EdgeQueryPojo tmp=new EdgeQueryPojo();
//                        tmp.setEdgelabel(t.getStrFld(6));
//                        NID destNid = new NID();
//                        destNid.pageNo=new PageId(t.getIntFld(4));
//                        destNid.slotNo=t.getIntFld(5);
//                        String label=t.getStrFld(6);
//                        int wt=t.getIntFld(1);
//                        //System.out.println("__________"+edgeQuery.get(1).getKey());
//                        if(edgeQuery.get(1).getKey()==1)
//                        {
//                            if(edgeQuery.get(1).getEdgelabel().equalsIgnoreCase(label))
//                            {
//                                children.add(tmp);
//                                if(edgeQuery.size()==2)
//                                {
//                                    EdgeQueryPojo destNidPojo = new EdgeQueryPojo();
//                                    destNidPojo.setNd(destNid);
//                                    ansNids.add(destNidPojo);
//                                }
//                            }
//
//                        }
//                        else if(edgeQuery.get(1).getKey()==2)
//                        {
//                            if(edgeQuery.get(1).getWeight()>=wt)
//                            {
//                                children.add(tmp);
//                                if(edgeQuery.size()==2)
//                                {
//                                    EdgeQueryPojo destNidPojo = new EdgeQueryPojo();
//                                    destNidPojo.setNd(destNid);
//                                    ansNids.add(destNidPojo);
//                                }
//                            }
//                        }
//
//                    }
//                    nlj.close();
//
//
//                }
//
//            }
//            else{
//
//                EdgeQueryPojo epj=new EdgeQueryPojo();
//                Iterator<EdgeQueryPojo> iterQuery=childQuery.iterator();
//                while (iterQuery.hasNext()) {
//                    NestedLoopsJoins nlj=new  Join().joinEdgeEdgeNested(name, iterQuery.next());
//                    Tuple t=new Tuple();
//                    while((t=nlj.get_next())!=null)
//                    {
//                        EdgeQueryPojo tmp=new EdgeQueryPojo();
//                        String label=t.getStrFld(6);
//                        tmp.setEdgelabel(label);
//                        NID destNid = new NID();
//                        destNid.pageNo=new PageId(t.getIntFld(4));
//                        destNid.slotNo=t.getIntFld(5);
//                        int wt=t.getIntFld(1);
//                        if(edgeQuery.get(1).getKey()==1)
//                        {
//                            if(edgeQuery.get(1).getEdgelabel().equalsIgnoreCase(label))
//                            {
//                                children.add(tmp);
//                                if(edgeQuery.size()==2)
//                                {
//                                    EdgeQueryPojo destNidPojo = new EdgeQueryPojo();
//                                    destNidPojo.setNd(destNid);
//                                    ansNids.add(destNidPojo);
//                                }
//                            }
//
//
//                        }
//                        else if(edgeQuery.get(1).getKey()==2)
//                        {
//                            if(edgeQuery.get(1).getWeight()>=wt)
//                            {
//                                children.add(tmp);
//                                if(edgeQuery.size()==2)
//                                {
//                                    EdgeQueryPojo destNidPojo = new EdgeQueryPojo();
//                                    destNidPojo.setNd(destNid);
//                                    ansNids.add(destNidPojo);
//                                }
//                            }
//                        }
//
//
//                    }
//                    nlj.close();
//
//                }
//
//            }
//
//            System.out.println("children in iteration"+i);
//
//
//            childQuery=children;
//            for(EdgeQueryPojo epj1:childQuery)
//            {
//                System.out.println(epj1.getEdgelabel());
//            }
//            edgeQuery.remove(0);
//            i++;
//        }
//
//
//        return ansNids;
//    }
//
//
// }
public static List<EdgeQueryPojo> queryPE2(String name, List<EdgeQueryPojo> edgeQuery)
        throws JoinsException, IndexException, PageNotReadException, TupleUtilsException, PredEvalException,
        SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
    List<EdgeQueryPojo> ansNids = new LinkedList<EdgeQueryPojo>();
    List<EdgeQueryPojo> childQuery = new LinkedList<EdgeQueryPojo>();
    List<NodeQueryPojo> nchildQuery = new LinkedList<NodeQueryPojo>();
    int i = 0;

    while (edgeQuery.size() > 1) {
    	int reads = PCounter.readCounter, writes = PCounter.writeCounter;
        List<EdgeQueryPojo> children = new LinkedList<EdgeQueryPojo>();
        if (i == 0) {

            NID nid = edgeQuery.get(0).getNd();
            String node_label = SystemDefs.JavabaseDB.nhfile.getNode(nid).getLabel();
            NodeQueryPojo npj = new NodeQueryPojo();
            npj.setLabel(node_label);
            nchildQuery.add(npj);
            Iterator<NodeQueryPojo> iterQuery = nchildQuery.iterator();
            while (iterQuery.hasNext()) {
                NestedLoopsJoins nlj;

                nlj = new Join().joinNodeSEdge(name, iterQuery.next());
                Tuple t = new Tuple();
                iterator.Iterator joinResult = (iterator.Iterator) nlj;
                while ((t = joinResult.get_next()) != null) {
                    EdgeQueryPojo tmp = new EdgeQueryPojo();
                    tmp.setEdgelabel(t.getStrFld(6));
                    NID destNid = new NID();
                    destNid.pageNo = new PageId(t.getIntFld(4));
                    destNid.slotNo = t.getIntFld(5);
                    String label = t.getStrFld(6);
                    int wt = t.getIntFld(1);
                    // System.out.println("__________"+edgeQuery.get(1).getKey());
                    if (edgeQuery.get(1).getKey() == 1) {
                        if (edgeQuery.get(1).getEdgelabel().equalsIgnoreCase(label)) {
                            children.add(tmp);
                            if (edgeQuery.size() == 2) {
                                EdgeQueryPojo destNidPojo = new EdgeQueryPojo();
                                destNidPojo.setNd(destNid);
                                ansNids.add(destNidPojo);
                            }
                        }

                    } else if (edgeQuery.get(1).getKey() == 2) {
                        if (edgeQuery.get(1).getWeight() >= wt) {
                            children.add(tmp);
                            if (edgeQuery.size() == 2) {
                                EdgeQueryPojo destNidPojo = new EdgeQueryPojo();
                                destNidPojo.setNd(destNid);
                                ansNids.add(destNidPojo);
                            }
                        }
                    }

                }
                nlj.close();

            }

        } else {

            EdgeQueryPojo epj = new EdgeQueryPojo();
            Iterator<EdgeQueryPojo> iterQuery = childQuery.iterator();
            while (iterQuery.hasNext()) {
                NestedLoopsJoins nlj = new Join().joinEdgeEdgeNested(name, iterQuery.next());
                Tuple t = new Tuple();
                while ((t = nlj.get_next()) != null) {
                    EdgeQueryPojo tmp = new EdgeQueryPojo();
                    String label = t.getStrFld(6);
                    tmp.setEdgelabel(label);
                    NID destNid = new NID();
                    destNid.pageNo = new PageId(t.getIntFld(4));
                    destNid.slotNo = t.getIntFld(5);
                    int wt = t.getIntFld(1);
                    if (edgeQuery.get(1).getKey() == 1) {
                        if (edgeQuery.get(1).getEdgelabel().equalsIgnoreCase(label)) {
                            children.add(tmp);
                            if (edgeQuery.size() == 2) {
                                EdgeQueryPojo destNidPojo = new EdgeQueryPojo();
                                destNidPojo.setNd(destNid);
                                ansNids.add(destNidPojo);
                            }
                        }

                    } else if (edgeQuery.get(1).getKey() == 2) {
                        if (edgeQuery.get(1).getWeight() >= wt) {
                            children.add(tmp);
                            if (edgeQuery.size() == 2) {
                                EdgeQueryPojo destNidPojo = new EdgeQueryPojo();
                                destNidPojo.setNd(destNid);
                                ansNids.add(destNidPojo);
                            }
                        }
                    }

                }
                nlj.close();

            }
            System.out.print("\tDisk Read Count: "+ Integer.toString(PCounter.readCounter-reads));        

            System.out.println("\tDisk Write Count: "+ Integer.toString(PCounter.writeCounter-writes));
        }

        // System.out.println("children in iteration"+i);

        childQuery = children;
            /*
             * for(EdgeQueryPojo epj1:childQuery) {
             * System.out.println(epj1.getEdgelabel()); }
             */
        edgeQuery.remove(0);
        i++;
    }

    return ansNids;
}

}

