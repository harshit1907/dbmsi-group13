package tests;

import java.io.BufferedReader;

import java.io.IOException;
import java.io.InputStreamReader;

import diskmgr.PCounter;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;

// Shobhit
// batchnodeinsert /home/anjoy92/Documents/dbmsi-group13/src/tests/NodeTestDataI.txt db1
// batchnodedelete /home/anjoy92/Documents/dbmsi-group13/src/tests/NodeTestDataD.txt db1

// Harshit
// batchnodeinsert C://NodeTestData.txt db1
// batchnodedelete C://mihir.txt db1
// batchedgeinsert C://EdgeTestData.txt db1
// batchedgedelete C://EdgeTestData2.txt db1

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
// batchnodeinsert /home/prakhar/Documents/minjava/javaminibase/NodeInsertData.txt db1
// batchnodedelete /home/prakhar/Documents/minjava/javaminibase/NodeDeleteData.txt db1
// batchedgeinsert /home/prakhar/Documents/minjava/javaminibase/EdgeInsertData.txt db1
// batchedgedelete /home/prakhar/Documents/minjava/javaminibase/EdgeDeleteData.txt db1

public class G13App {
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

            } else if (operation.equalsIgnoreCase("batchedgeinsert")) {
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

            } else if (operation.equalsIgnoreCase("nodequery")) {
                if(created==false) {
                    System.out.println("Database does not exist!\n");
                    continue;
                }
                graphDbName = tokens[1];
                numBuf = Integer.parseInt(tokens[2]);
                qType = Integer.parseInt(tokens[3]);
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
