package tests;

import bufmgr.*;
import diskmgr.PCounter;
import global.SystemDefs;
import heap.*;
import queryPojo.EdgeQueryPojo;
import queryPojo.NodeQueryPojo;
import queryPojo.Pair;
import queryPojo.QueryProcessor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
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

public class phase3Test_commands {
    public static void main(String[] agrs) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, IOException, Exception {
    	while(true) {
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

                new BatchNodeInsert().batchNodeInsert("C://NodeInsertData.txt", name);
                new BatchEdgeInsert().batchEdgeInsert("C://EdgeInsertData.txt");

                new Join().createEdgeIndex();
                new Join().createEdgeIndexEid();
            }
            /*
            if (operation.equalsIgnoreCase("exit") || operation.equalsIgnoreCase("quit")) {
                System.err.println("Bye! \n");
                System.exit(0);
                break;
            }*/
            if (operation.equalsIgnoreCase("help")) {
                System.out.println("");
               // continue;
            }


            //String inputFile = scanner.nextLine();

            
            if (operation.equalsIgnoreCase("2.3")) {

                List<NodeQueryPojo> li= new QueryProcessor().PathExpression1(path_expr); // 19/811
                List<NodeQueryPojo> ansNids = new ArrayList<NodeQueryPojo>();
                List<Pair<NodeQueryPojo,NodeQueryPojo>> ansNodes= new ArrayList<Pair<NodeQueryPojo,NodeQueryPojo>>();

                new Queries();
                ansNids= Queries.queryPE1(name, li);

                for( NodeQueryPojo tmpLi : ansNids)
                {
                    //  System.out.println(ansNids.size()+" "+tmpLi.getKey());
                    System.out.println("pe1");
                    System.out.println("(Page No: "+tmpLi.getNd().pageNo+" | Slot no: "+tmpLi.getNd().slotNo+" | Label: "+tmpLi.getLabel()+" | Desr:"+tmpLi.getDesc().getString2()+"),");
                }
                SystemDefs.JavabaseBM.flushAllPages();
                System.out.println("\nTotal Disk Read Count: " + PCounter.readCounter);

                System.out.println("Total Disk Write Count: " + PCounter.writeCounter);



            }
            if (operation.equalsIgnoreCase("2.4")) {
                List<EdgeQueryPojo> ansNids = new ArrayList<EdgeQueryPojo>();
                List<EdgeQueryPojo> ansNodes= new ArrayList<EdgeQueryPojo>();
                List<EdgeQueryPojo> edgeQuery= new QueryProcessor().PathExpression2(path_expr);
                ansNodes=new Query47().queryPE2(name, edgeQuery);
                for( EdgeQueryPojo tmpLi : ansNodes)
                {
                    System.out.println("(Page id: "+tmpLi.getNd().pageNo+" | Slot no: "+tmpLi.getNd().slotNo+"),");
                }
                SystemDefs.JavabaseBM.flushAllPages();
                System.out.println("\nTotal Disk Read Count: " + PCounter.readCounter);

                System.out.println("Total Disk Write Count: " + PCounter.writeCounter);


            }
            if (operation.equalsIgnoreCase("2.6a")) {
                List<NodeQueryPojo> li= new QueryProcessor().PathExpression1(path_expr); // 19/811
                List<NodeQueryPojo> ansNids = new ArrayList<NodeQueryPojo>();
                List<Pair<NodeQueryPojo,NodeQueryPojo>> ansNodes= new ArrayList<Pair<NodeQueryPojo,NodeQueryPojo>>();


                ansNodes= Queries.queryPQ1(name, li,"a");

                for( Pair<NodeQueryPojo,NodeQueryPojo> tmpLi : ansNodes)
                {
                    System.out.println("task 2.6a");
                    System.out.println("(Head: "+tmpLi.getLeft().getLabel()+" | Tail: "+tmpLi.getRight().getLabel()+"),");
                }
                SystemDefs.JavabaseBM.flushAllPages();
                System.out.println("\nTotal Disk Read Count: " + PCounter.readCounter);

                System.out.println("Total Disk Write Count: " + PCounter.writeCounter);


            }
            if (operation.equalsIgnoreCase("2.6b")) {
                List<NodeQueryPojo> li= new QueryProcessor().PathExpression1(path_expr); // 19/811
                List<NodeQueryPojo> ansNids = new ArrayList<NodeQueryPojo>();
                List<Pair<NodeQueryPojo,NodeQueryPojo>> ansNodes= new ArrayList<Pair<NodeQueryPojo,NodeQueryPojo>>();


                ansNodes= Queries.queryPQ1(name, li,"b");

                for( Pair<NodeQueryPojo,NodeQueryPojo> tmpLi : ansNodes)
                {
                    System.out.println("task 2.6b");
                    System.out.println("(Head: "+tmpLi.getLeft().getLabel()+" | Tail: "+tmpLi.getRight().getLabel()+"),");
                }
                SystemDefs.JavabaseBM.flushAllPages();
                System.out.println("\nTotal Disk Read Count: " + PCounter.readCounter);

                System.out.println("Total Disk Write Count: " + PCounter.writeCounter);


            }
            if (operation.equalsIgnoreCase("2.6c")) {
                List<NodeQueryPojo> li= new QueryProcessor().PathExpression1(path_expr); // 19/811
                List<NodeQueryPojo> ansNids = new ArrayList<NodeQueryPojo>();
                List<Pair<NodeQueryPojo,NodeQueryPojo>> ansNodes= new ArrayList<Pair<NodeQueryPojo,NodeQueryPojo>>();


                ansNodes= Queries.queryPQ1(name, li,"c");

                for( Pair<NodeQueryPojo,NodeQueryPojo> tmpLi : ansNodes)
                {
                    System.out.println("task 2.6c");
                    System.out.println("(Head: "+tmpLi.getLeft().getLabel()+" | Tail: "+tmpLi.getRight().getLabel()+"),");
                }
                SystemDefs.JavabaseBM.flushAllPages();
                System.out.println("\nTotal Disk Read Count: " + PCounter.readCounter);

                System.out.println("Total Disk Write Count: " + PCounter.writeCounter);


            }


            if(operation.toLowerCase().startsWith("2.7")){
                //String path_expr="3/26/355_961";
                String paths[]=path_expr.split("/");
                List<NodeQueryPojo> nodeQuery= new QueryProcessor().PathExpression1(paths[0]);
                String edge_path="";
                for(int i=1;i<paths.length;i++)
                {
                    edge_path+=paths[i];
                    if(i<paths.length-1)
                    {
                        edge_path+="/";
                    }
                }

                List<EdgeQueryPojo> edgeQuery= new QueryProcessor().PathExpression2(edge_path);
                if(operation.indexOf("7a")>0) {
                    List<Pair<EdgeQueryPojo, EdgeQueryPojo>> ansNids = new Query47().queryPQ2a(name, edgeQuery, nodeQuery, "a");
                    System.out.println("printing result 2.7a");
                    for (Pair<EdgeQueryPojo, EdgeQueryPojo> tmpLi : ansNids) {
                        System.out.println("(Head: " + tmpLi.getLeft().getDestLabel() + " | Tail: " + tmpLi.getRight().getDestLabel() + "),");
                    }
                }


                if(operation.indexOf("7b")>0) {

                    System.out.println("printing result 2.7b");
                    List<Pair<EdgeQueryPojo, EdgeQueryPojo>> ansNids1 = new Query47().queryPQ2a(name, edgeQuery, nodeQuery, "b");
                    for (Pair<EdgeQueryPojo, EdgeQueryPojo> tmpLi : ansNids1) {
                        System.out.println("(Head: " + tmpLi.getLeft().getDestLabel() + " | Tail: " + tmpLi.getRight().getDestLabel() + "),");
                    }
                }

               if(operation.indexOf("7c")>0) {

                   System.out.println("printing result 2.7c");
                   List<Pair<EdgeQueryPojo, EdgeQueryPojo>> ansNids2 = new Query47().queryPQ2a(name, edgeQuery, nodeQuery, "c");
                   for (Pair<EdgeQueryPojo, EdgeQueryPojo> tmpLi : ansNids2) {
                       System.out.println("(Head: " + tmpLi.getLeft().getDestLabel() + " | Tail: " + tmpLi.getRight().getDestLabel() + "),");
                   }
               }

                SystemDefs.JavabaseBM.flushAllPages();
                System.out.println("\nTotal Disk Read Count: " + PCounter.readCounter);

                System.out.println("Total Disk Write Count: " + PCounter.writeCounter);



            }
            if(operation.toLowerCase().startsWith("2.9")){
                List<EdgeQueryPojo> li2 = new QueryProcessor().PathExpression2(path_expr);
                List<EdgeQueryPojo> edgeQueryPojoList = new ArrayList<EdgeQueryPojo>();
                EdgeQueryPojo edgeQueryPojo = new EdgeQueryPojo();
                edgeQueryPojo.setWeight(50);
                EdgeQueryPojo edgeQueryPojo1 = new EdgeQueryPojo();
                edgeQueryPojo1.setWeight(50);
                EdgeQueryPojo edgeQueryPojo2 = new EdgeQueryPojo();
                edgeQueryPojo2.setWeight(50);
                edgeQueryPojoList.add(edgeQueryPojo);
                edgeQueryPojoList.add(edgeQueryPojo1);
                edgeQueryPojoList.add(edgeQueryPojo2);
                System.out.println("Executing T9");
                List<String> ansNodes2 = new ArrayList<String>();
                if(operation.indexOf("9a")>0){
                    System.out.println("Qa");
                    new Triangle().triangleJoin(name, li2, "a");

                    SystemDefs.JavabaseBM.flushAllPages();
                }
                if(operation.indexOf("9b")>0){
                    System.out.println("Qb");
                    new Triangle().triangleJoin(name, li2, "b");

                    SystemDefs.JavabaseBM.flushAllPages();
                }
                if(operation.indexOf("9c")>0){
                    System.out.println("Qc");
                    new Triangle().triangleJoin(name, li2, "c");

                    SystemDefs.JavabaseBM.flushAllPages();
                    System.out.println("\nTotal Disk Read Count: " + PCounter.readCounter);

                    System.out.println("Total Disk Write Count: " + PCounter.writeCounter);


                }

            }
    	}

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
