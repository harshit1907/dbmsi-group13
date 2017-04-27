package tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import diskmgr.PCounter;
import global.Descriptor;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.Tuple;
import queryPojo.EdgeQueryPojo;
import queryPojo.NodeQueryPojo;
import queryPojo.Pair;
import queryPojo.QueryProcessor;
//

// Mihir
// batchnodeinsert /Users/mihir/dev/NodeInsertData.txt db1
// batchnodedelete /Users/mihir/dev/NodeDeleteData.txt db1
// batchedgeinsert /Users/mihir/dev/EdgeInsertData.txt db1
// batchedgedelete /Users/mihir/dev/EdgeDeleteData.txt db1

// COMMON Commands
// nodequery GRAPHDBNAME NUMBUF QTYPE INDEX [QUERYOPTIONS]
// nodequery db1 40 1 0 0
// nodequery db1 40 5 1 10 20 30 40 50 5
// nodequery db1 40 5 1 10 20 30 40 50 5
// edgequery GRAPHDBNAME NUMBUF QTYPE INDEX [QUERYOPTIONS]
// edgequery db1 40 1 0 
// fullnodescan db1 
// fulledgescan db1

// Prakhar
// batchnodeinsert /home/prakhar/Documents/minjava/javaminibase/NodeTestData.txt db1
// batchnodedelete /home/prakhar/Documents/minjava/javaminibase/NodeTestData2.txt db1
// batchedgeinsert /home/prakhar/Documents/minjava/javaminibase/EdgeTestData.txt db1
// batchedgedelete /home/prakhar/Documents/minjava/javaminibase/EdgeTestData2.txt db1

public class phase3Test {
    public static void main(String[] agrs) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, IOException, Exception {
        String name;
        Scanner scanner = new Scanner(System.in);
        name = scanner.nextLine();
        //String inputFile = scanner.nextLine();
        File DBfile = new File(name);
        System.out.println("DB " + DBfile + " exists: " + DBfile.exists());
        
        if(DBfile.exists()) {
            System.out.println("Open!\n");
            openDB(name);
        }
        else {
            System.out.println("Create!\n");
            createDB(name);
//            new BatchNodeInsert().batchNodeInsert("/home/prakhar/Documents/minjava/javaminibase/NodeInsertData.txt", name);
//            new BatchEdgeInsert().batchEdgeInsert("/home/prakhar/Documents/minjava/javaminibase/EdgeInsertData.txt");
//            new BatchNodeInsert().batchNodeInsert("/home/anjoy92/Downloads/dbmsi/src/tests/NodeTestDataI.txt", name);
//            new BatchEdgeInsert().batchEdgeInsert("/home/anjoy92/Downloads/dbmsi/src/tests/EdgeTestData.txt");
            
            new BatchNodeInsert().batchNodeInsert("/home/anjoy92/Documents/dbmsi-group13/src/tests/clean/NodeTestDataOld.txt", name);
            new BatchEdgeInsert().batchEdgeInsert("/home/anjoy92/Documents/dbmsi-group13/src/tests/clean/EdgeTestDataOld.txt");
          
       //     new BatchNodeInsert().batchNodeInsert("/home/hadoop/Desktop/NodeInsertData.txt", name);
      //      new BatchEdgeInsert().batchEdgeInsert("/home/hadoop/Desktop/EdgeInsertData.txt");
          
            new Join().createEdgeIndex();

            new Join().createEdgeIndexEid();
        }
  //      new FullScanNode().fullScanNode(name);

        
        //        PCounter.initialize();
//        NodeQueryPojo nodeQueryPojo = new NodeQueryPojo();
//        Descriptor desc=new Descriptor();
//        
//        desc.set(18, 38, 42, 29, 49);
//        nodeQueryPojo.setDesc(desc);
//   
//        new Join().joinNodeSEdge(name, nodeQueryPojo);
//        SystemDefs.JavabaseBM.flushAllPages();
//        System.out.println("****** Read Counter "+PCounter.readCounter+" ==== Write Counter"+ PCounter.writeCounter);
//        nodeQueryPojo.setLabel("50");
        
//        PCounter.initialize();
//        new Join().joinEdgeEdge(name);
//        SystemDefs.JavabaseBM.flushAllPages();
//        System.out.println("****** Read Counter "+PCounter.readCounter+" ==== Write Counter"+ PCounter.writeCounter);
//     
        //*********************** query PE1 ************************//
        
        // Page First then slot no 85, 1 - 109 ||| 38,19 for 19
       /* List<NodeQueryPojo> li= new QueryProcessor().PathExpression1("19/47,20,29,1,38"); // 19/811
        
//        for( NodeQueryPojo tmpLi : li)
//        {
//            if(tmpLi.getKey()==1)
//            System.out.println(tmpLi.getLabel()+" /");
//            else if(tmpLi.getKey()==2)
//            System.out.println(tmpLi.getDesc().getString()+" /");
//            else if(tmpLi.getKey()==3)
//                System.out.println(tmpLi.getNd().pageNo+" "+tmpLi.getNd().slotNo+" /");
//                
//        }

        List<NodeQueryPojo> ansNids = new ArrayList<NodeQueryPojo>();
        List<Pair<NodeQueryPojo,NodeQueryPojo>> ansNodes= new ArrayList<Pair<NodeQueryPojo,NodeQueryPojo>>();
        
        nQueries.queryPQ1a(name, li);
        
        for( Pair<NodeQueryPojo,NodeQueryPojo> tmpLi : ansNodes)
        {
            System.out.println("(Head: "+tmpLi.getLeft().getLabel()+" | Tail: "+tmpLi.getRight().getLabel()+"),");
        }ew Queries();
        ansNids= Queries.queryPE1(name, li);
        
        for( NodeQueryPojo tmpLi : ansNids)
        {
          //  System.out.println(ansNids.size()+" "+tmpLi.getKey());
            System.out.println("(Page No: "+tmpLi.getNd().pageNo+" | Slot no: "+tmpLi.getNd().slotNo+" | Label: "+tmpLi.getLabel()+" | Desr:"+tmpLi.getDesc().getString2()+"),");
        }
        SystemDefs.JavabaseBM.flushAllPages();
          
        
        //29 2 46 14 20
        //7 1 44 22 12
        li= new QueryProcessor().PathExpression1("7,1,44,22,12/4");
        
        ansNodes= 
        
        
        List<EdgeQueryPojo> ansNids = new ArrayList<EdgeQueryPojo>();
        List<EdgeQueryPojo> edgequery = new ArrayList<EdgeQueryPojo>();
        edgequery=new QueryProcessor().PathExpression2("81,9/638_704/40");
        ansNids=new Queries().queryPE2(name, edgequery);
        for(EdgeQueryPojo epj:ansNids)
        {
        	System.out.println("(Page No: "+epj.getNd().pageNo+" | Slot no: "+epj.getNd().slotNo+")");
        }*/
        //CODE FOR EXECUTING PE2
       /* List<EdgeQueryPojo> ansNids = new ArrayList<EdgeQueryPojo>();
        List<EdgeQueryPojo> ansNodes= new ArrayList<EdgeQueryPojo>();
        List<EdgeQueryPojo> edgeQuery= new QueryProcessor().PathExpression2("87,3/309/100");
        ansNodes=new Query47().queryPE2(name, edgeQuery);
        for( EdgeQueryPojo tmpLi : ansNodes)
        {
            System.out.println("(Head: "+tmpLi.getNd().pageNo+" | Tail: "+tmpLi.getNd().slotNo+"),");
        }*/
        //code for executing 2.7
        
       //  String path_expr="87/309/100";
        
        
//        List<NodeQueryPojo> li= new QueryProcessor().PathExpression1("20,29,1,38"); // 19/811
//        
//        iterator.Iterator ansNodes= new Join().joinNodeSEdgeEid(name, li.get(0));
//       Tuple t= new Tuple();
//        while((t=ansNodes.get_next())!=null )
//        {
//            System.out.println("(Head: "+t.getIntFld(9)+"),");
//        }
        new Join().joinEdgeEdge(name);
        
        
          SystemDefs.JavabaseBM.flushAllPages();
      
        scanner.close();
    }  

    public static void createDB(String graphDbName) {
        SystemDefs sysdef = new SystemDefs(graphDbName,500000,5000,"Clock",0);
    }

    public static void openDB(String graphDbName) throws HashOperationException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, IOException {
        //SystemDefs sysdef = new SystemDefs(graphDbName,0,400,"Clock",0);
        //if(SystemDefs.JavabaseDB!=null) 
        //SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs sysdef = new SystemDefs(graphDbName,0,5000,"Clock",0);
        SystemDefs.JavabaseBM.flushAllPages();
    }
}
