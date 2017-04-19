package tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import diskmgr.PCounter;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
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
            new BatchNodeInsert().batchNodeInsert("/home/prakhar/Documents/minjava/javaminibase/NodeTestData.txt", name);
            new BatchEdgeInsert().batchEdgeInsert("/home/prakhar/Documents/minjava/javaminibase/EdgeTestData.txt");
        }
        new FullScanNode().fullScanNode(name);
//        new NodeQuery().nodeQuery(name,400,1,1,"0");
//        new EdgeQuery().edgeQuery(name,400,1,1,"0");
//        new NodeQuery().nodeQuery(name,400,1,1,"0");
//        new EdgeQuery().edgeQuery(name,400,1,0,"0");
      //  SystemDefs.JavabaseBM.flushAllPages();
        
        new Join().joinEdgeDNode(name);
        new Join().joinEdgeDNode(name);
        new Join().joinEdgeEdge(name);
        new Join().joinEdgeEdge(name);
         
        SystemDefs.JavabaseBM.flushAllPages();
        scanner.close();
    }  

    public static void createDB(String graphDbName) {
        SystemDefs sysdef = new SystemDefs(graphDbName,10000,4000,"Clock",0);
    }

    public static void openDB(String graphDbName) throws HashOperationException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, IOException {
        //SystemDefs sysdef = new SystemDefs(graphDbName,0,400,"Clock",0);
        //if(SystemDefs.JavabaseDB!=null) 
        //SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs sysdef = new SystemDefs(graphDbName,0,4000,"Clock",0);
        SystemDefs.JavabaseBM.flushAllPages();
    }
}
