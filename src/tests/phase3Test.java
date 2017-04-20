package tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
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
import queryPojo.EdgeQueryPojo;
import queryPojo.NodeQueryPojo;
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
            
    //        new BatchNodeInsert().batchNodeInsert("/home/anjoy92/Documents/dbmsi-group13/src/tests/clean/NodeTestDataOld.txt", name);
      //      new BatchEdgeInsert().batchEdgeInsert("/home/anjoy92/Documents/dbmsi-group13/src/tests/clean/EdgeTestDataOld.txt");
          
            new BatchNodeInsert().batchNodeInsert("/home/anjoy92/Documents/dbmsi-group13/src/tests/clean/NodeInsertDataNew.txt", name);
            new BatchEdgeInsert().batchEdgeInsert("/home/anjoy92/Documents/dbmsi-group13/src/tests/clean/EdgeInsertDataNew.txt");
          
            
            new Join().createEdgeIndex();
        }
        new FullScanNode().fullScanNode(name);
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
        
        
        // Page First then slot no 85, 1 - 109
        List<NodeQueryPojo> li= new QueryProcessor().PathExpression1("19/214");
        
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

        List<NodeQueryPojo> ansPojo = new LinkedList<NodeQueryPojo>();
        new Queries().queryPE1(name, li, ansPojo);
       
      for( NodeQueryPojo tmpLi : ansPojo)
      {
          if(tmpLi.getKey()==1)
          System.out.println(tmpLi.getLabel()+" /");
          else if(tmpLi.getKey()==2)
          System.out.println(tmpLi.getDesc().getString()+" /");
          else if(tmpLi.getKey()==3)
              System.out.println(tmpLi.getNd().pageNo+" "+tmpLi.getNd().slotNo+" /");
      }
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
