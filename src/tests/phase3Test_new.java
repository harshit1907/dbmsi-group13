package tests;
import diskmgr.PCounter;
import bufmgr.*;
import global.SystemDefs;
import heap.*;
import queryPojo.NodeQueryPojo;
import queryPojo.Pair;
import queryPojo.QueryProcessor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
//
//Demo queries
//task-2.3 db23 path_exp
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

public class phase3Test_new {
    public static void main(String[] agrs) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, IOException, Exception {
        while (true) {

            PCounter.initialize();
            System.out.print("Group13> ");
            BufferedReader input = new BufferedReader(new InputStreamReader(System.in));

            String line = input.readLine();
            String[] tokens = line.trim().split(" ");
            String name = tokens[1];
            String path_expr=tokens[2];

            String operation = tokens[0];
            File DBfile = new File(tokens[1]);
            if (DBfile.exists()) {
                System.out.println("Open!\n");
                openDB(tokens[1]);
            } else {
                System.out.println("Create!\n");
                createDB(tokens[1]);
//            new BatchEdgeInsert().batchEdgeInsert("/home/anjoy92/Downloads/dbmsi/src/tests/EdgeTestData.txt");

                //        new BatchNodeInsert().batchNodeInsert("/home/anjoy92/Documents/dbmsi-group13/src/tests/clean/NodeTestDataOld.txt", name);
                //      new BatchEdgeInsert().batchEdgeInsert("/home/anjoy92/Documents/dbmsi-group13/src/tests/clean/EdgeTestDataOld.txt");

                new BatchNodeInsert().batchNodeInsert("/home/nishi/dbmsi-group13/insertN.txt", name);
                new BatchEdgeInsert().batchEdgeInsert("/home/nishi/dbmsi-group13/insertE.txt");

                new Join().createEdgeIndex();
            }
            if (operation.equalsIgnoreCase("exit") || operation.equalsIgnoreCase("quit")) {
                System.err.println("Bye! \n");
                System.exit(0);
                break;
            }
            if (operation.equalsIgnoreCase("help")) {
                System.out.println("");
                continue;
            }


            //String inputFile = scanner.nextLine();


            if (operation.equalsIgnoreCase("task-2.3")) {

                List<NodeQueryPojo> li= new QueryProcessor().PathExpression1(path_expr); // 19/811
                List<NodeQueryPojo> ansNids = new ArrayList<NodeQueryPojo>();
                List<Pair<NodeQueryPojo,NodeQueryPojo>> ansNodes= new ArrayList<Pair<NodeQueryPojo,NodeQueryPojo>>();

                new Queries();
                ansNids= Queries.queryPE1(name, li);

                for( NodeQueryPojo tmpLi : ansNids)
                {
                    //  System.out.println(ansNids.size()+" "+tmpLi.getKey());
                    System.out.println("task 2.3");
                    System.out.println("(Page No: "+tmpLi.getNd().pageNo+" | Slot no: "+tmpLi.getNd().slotNo+" | Label: "+tmpLi.getLabel()+" | Desr:"+tmpLi.getDesc().getString2()+"),");
                }
                SystemDefs.JavabaseBM.flushAllPages();

            }
            if (operation.equalsIgnoreCase("task-2.6")) {
                List<NodeQueryPojo> li= new QueryProcessor().PathExpression1(path_expr); // 19/811
                List<NodeQueryPojo> ansNids = new ArrayList<NodeQueryPojo>();
                List<Pair<NodeQueryPojo,NodeQueryPojo>> ansNodes= new ArrayList<Pair<NodeQueryPojo,NodeQueryPojo>>();


                ansNodes= Queries.queryPQ1a(name, li);

                for( Pair<NodeQueryPojo,NodeQueryPojo> tmpLi : ansNodes)
                {
                    System.out.println("task 2.6");
                    System.out.println("(Head: "+tmpLi.getLeft().getLabel()+" | Tail: "+tmpLi.getRight().getLabel()+"),");
                }
                SystemDefs.JavabaseBM.flushAllPages();
            }

        }
    }//29 2 46 14 20


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
