/**
 * Created by nishi on 16/4/17.
 */
package tests;

        import java.io.BufferedReader;

        import btree.BTFileScan;
        import btree.BTreeFile;
        import giterator.GraphNodeNestedLoopsJoins;
        import global.IndexType;
        import heap.*;

        import java.io.IOException;
        import java.io.InputStreamReader;

        import diskmgr.PCounter;
        import global.AttrOperator;
        import global.AttrType;
        import global.SystemDefs;
        import heap.HFBufMgrException;
        import heap.HFDiskMgrException;
        import heap.HFException;
        import heap.InvalidSlotNumberException;
        import heap.InvalidTupleSizeException;
        import index.IndexScan;
        import iterator.CondExpr;
        import iterator.FldSpec;
        import iterator.NestedLoopsJoins;
        import iterator.RelSpec;
        import org.w3c.dom.Attr;

// Shobhit
// batchnodeinsert /home/anjoy92/Documents/dbmsi-group13/src/tests/NodeTestDataI.txt db1
// batchnodedelete /home/anjoy92/Documents/dbmsi-group13/src/tests/NodeTestDataD.txt db1

// Harshit
// batchnodeinsert C://NodeTestData.txt db1
// batchnodedelete C://mihir.txt db1
// batchedgeinsert C://EdgeTestData.txt db1
// batchedgedelete C://EdgeTestData2.txt db1

//Nishi
//batchnodeinsert /home/nishi/dbmsi-group13/NodeInsertData1.txt db1
//batchedgeinsert /home/nishi/dbmsi-group13/EdgeInsertData1.txt db1

// Mihir
// batchnodeinsert /Users/mihir/dev/NodeTestData.txt db1
// batchnodedelete /Users/mihir/dev/NodeTestData2.txt db1
// batchedgeinsert /Users/mihir/dev/EdgeTestData.txt db1
// batchedgedelete /Users/mihir/dev/EdgeTestData2.txt db1

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

public class GJoinTest {
    public static void main(String[] agrs) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, IOException, Exception {
        String graphDbName=null, inputFile = null, qOptions=null;
        int numBuf=0, qType=0, Index=0;

        boolean created=false;
        while(true) {
            PCounter.initialize();
            System.out.print("Group13> ");
            BufferedReader input = new BufferedReader (new InputStreamReader (System.in));

            String line = input.readLine();
            String []tokens = line.trim().split(" ");

            String operation = tokens[0];

            if(operation.equalsIgnoreCase("exit") || operation.equalsIgnoreCase("quit")) {
                System.err.println("Bye! \n");
                System.exit(0);
                break;
            }
            if(operation.equalsIgnoreCase("help")) {
                System.out.println("");
                continue;
            }

            if(operation.equalsIgnoreCase("batchnodeinsert")) {

                if(tokens.length<=2) {
                    System.out.println("Not enough arguments!\n");
                    continue;
                }
                inputFile = tokens[1];
                graphDbName = tokens[2];
                if(created==false) {
                    createDB(graphDbName);
                    created=true;
                }
                new BatchNodeInsert().batchNodeInsert(inputFile, graphDbName);

            } else if (operation.equalsIgnoreCase("batchnodedelete")) {
                if(tokens.length<=2) {
                    System.out.println("Not enough arguments!\n");
                    continue;
                }
                inputFile = tokens[1];
                graphDbName = tokens[2];
                if(created==false) {
                    createDB(graphDbName);
                    created=true;
                }
                new BatchNodeDelete().batchNodeDelete(inputFile, graphDbName);

            }
            else if (operation.equalsIgnoreCase("batchedgeinsert")) {
                if(tokens.length<=2) {
                    System.out.println("Not enough arguments!\n");
                    continue;
                }
                inputFile = tokens[1];
                graphDbName = tokens[2];
                if(created==false) {
                    createDB(graphDbName);
                    created=true;
                }
                new BatchEdgeInsert().batchEdgeInsert(inputFile);
            } else if (operation.equalsIgnoreCase("batchedgedelete")) {
                if(tokens.length<=2) {
                    System.out.println("Not enough arguments!\n");
                    continue;
                }
                inputFile = tokens[1];
                graphDbName = tokens[2];
                if(created==false) {
                    createDB(graphDbName);
                    created=true;
                }
                new BatchEdgeDelete().batchEdgeDelete(inputFile);
            } else if (operation.equalsIgnoreCase("fullnodescan")) {
                new FullScanNode().fullScanNode(graphDbName);
            } else if (operation.equalsIgnoreCase("fulledgescan")) {
                new FullScanEdge().fullScanEdge(graphDbName);

            }
            else if(operation.equalsIgnoreCase("join")){
                final boolean OK   = true;
                final boolean FAIL = false;
                boolean status=true;
                if(created==false){
                    System.out.print("Database does not exist\n");
                    continue;
                }
                graphDbName = tokens[1];
                CondExpr [] outfilter=new CondExpr[2];
                outfilter[0]=new CondExpr();
                outfilter[1]=new CondExpr();

                outfilter[0].next=null;
                outfilter[0].op=new AttrOperator(AttrOperator.aopEQ);
                outfilter[0].type1=new AttrType(AttrType.attrSymbol);
                outfilter[0].type2=new AttrType(AttrType.attrSymbol);
                outfilter[0].operand1.symbol=new FldSpec(new RelSpec(RelSpec.outer),1);
                outfilter[0].operand2.symbol=new FldSpec(new RelSpec(RelSpec.innerRel),1);

                outfilter[1]=null;
                Tuple t = new Tuple();
                t=null;
                AttrType [] etypes={
                        new AttrType(AttrType.attrString),
                        new AttrType(AttrType.attrString),
                        new AttrType(AttrType.attrString),
                        new AttrType(AttrType.attrInteger)

                };
                AttrType [] ntypes={
                        new AttrType(AttrType.attrString),
                        new AttrType(AttrType.attrDesc)
                };
                short [] esizes= new short[3];
                esizes[0]=15;
                esizes[1]=15;
                esizes[2]=15;
                short [] nsizes= new short[1];
                nsizes[0]=15;
                AttrType [] Jtypes={
                        new AttrType(AttrType.attrString),
                        new AttrType(AttrType.attrString)
                };
                short [] Jsizes=new short[2];
                Jsizes[0]=15;
                Jsizes[1]=15;

                FldSpec[] proj1 = {
                        new FldSpec(new RelSpec(RelSpec.outer),1),
                        new FldSpec(new RelSpec(RelSpec.innerRel),1)
                };
                CondExpr [] selects= new CondExpr[1];
                selects[0]=null;

                iterator.Iterator am = null;
                /*BTreeFile btf = null;
                try {
                    btf = new BTreeFile(SystemDefs.JavabaseDBName+"_BTreeNodeIndex", AttrType.attrString, 32, 1);
                }
                catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                    Runtime.getRuntime().exit(1);
                }*/
                IndexType b_index = new IndexType (IndexType.B_Index);
                try {
                    am = new IndexScan( b_index, SystemDefs.JavabaseDBName+"_Node",
                            "BTreeIndex", ntypes, nsizes, 4, 2,
                            proj1, null, 1, false);
                }

                catch (Exception e) {
                    System.err.println ("*** Error creating scan for Index scan");
                    System.err.println (""+e);
                    Runtime.getRuntime().exit(1);
                }
                GraphNodeNestedLoopsJoins nlj=null;
                try {
                    nlj = new GraphNodeNestedLoopsJoins(etypes, 4, esizes,
                            ntypes, 2, nsizes,
                            10,
                            SystemDefs.JavabaseDBName+"_Edge", am,
                            outfilter, null, proj1, 2);
                }
                catch (Exception e) {
                    System.err.println ("*** Error preparing for nested_loop_join");
                    System.err.println (""+e);
                    e.printStackTrace();
                    Runtime.getRuntime().exit(1);
                }

            }else if (operation.equalsIgnoreCase("nodequery")) {
                if(created==false) {
                    System.out.println("Database does not exist!\n");
                    continue;
                }
                graphDbName = tokens[1];
                numBuf = Integer.parseInt(tokens[2]);
                qType = Integer.parseInt(tokens[5]);
                Index = Integer.parseInt(tokens[4]);
                if(tokens.length>=5) { // "30 40 50 60 70 5"
                    qOptions="";
                    for(int i=5;i<tokens.length;i++)
                        qOptions+=tokens[i]+" ";
                }
                if(qOptions!=null) qOptions = qOptions.trim();
                System.out.println("Selected option: "+qOptions);
                new NodeQuery().nodeQuery(graphDbName,numBuf,qType,Index,qOptions);

            } else if (operation.equalsIgnoreCase("edgequery")) {
                if(created==false) {
                    System.out.println("Database does not exist!\n");
                    continue;
                }
                graphDbName = tokens[1];
                numBuf = Integer.parseInt(tokens[2]);
                qType = Integer.parseInt(tokens[3]);
                Index = Integer.parseInt(tokens[4]);
                if(tokens.length>=5) { //put space sep lowerbound and upper bound "10 20"
                    qOptions="";
                    for(int i=5;i<tokens.length;i++)
                        qOptions+=tokens[i]+" ";
                }
                if(qOptions!=null) qOptions = qOptions.trim();
                System.out.println("Selected option: "+qOptions);
                new EdgeQuery().edgeQuery(graphDbName,numBuf,qType,Index,qOptions);

            } else {
                System.out.print(line);
                System.out.println(" is not recognized as a command.\nType help for more information.");
            }
        }
    }

    public static void createDB(String graphDbName) {
        SystemDefs sysdef = new SystemDefs(graphDbName,10000,40,"Clock",0);
    }
}
