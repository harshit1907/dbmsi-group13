package tests;

import java.io.BufferedReader;

import java.io.IOException;
import java.io.InputStreamReader;

import bufmgr.BufMgr;
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

// Shobhit
// batchnodeinsert /home/anjoy92/Documents/dbmsi-group13/src/tests/NodeTestDataI.txt db1
// batchnodedelete /home/anjoy92/Documents/dbmsi-group13/src/tests/NodeTestDataD.txt db1
// fullnodescan db1
// Harshit
// batchnodeinsert C://NodeTestData.txt db1
// batchnodedelete C://mihir.txt db1
// batchedgeinsert C://EdgeTestData.txt db1
// batchedgedelete C://EdgeTestData2.txt db1

// fullnodescan db1
// fulledgescan db1
// nodequery C://NodeTestData.txt db1
// edgequery C://EdgeTestData.txt db1



//Mihir
//batchnodeinsert /Users/mihir/dev/NodeTestData.txt db1
//batchnodedelete /Users/mihir/dev/NodeTestData2.txt db1
//batchedgeinsert /Users/mihir/dev/EdgeTestData.txt db1
//batchedgedelete /Users/mihir/dev/EdgeTestData2.txt db1
public class G13App {
	   public static void main(String[] agrs) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, IOException, Exception {
		   	//String graphdbname=args[0];
		   	String graphdbname=null;
		   	
		   	while(true) {
		   		PCounter.initialize();
		   		System.out.print("Group13> ");
		   		BufferedReader input = new BufferedReader (new InputStreamReader (System.in));
		   		String line = input.readLine();
		   		String []tokens = line.trim().split(" ");
		   		String operation = tokens[0];
		   		if(operation.equalsIgnoreCase("exit")) {
		   			System.err.println("Bye! \n");
		   			break;
		   		}
		   		if(operation.equalsIgnoreCase("help")) {
		   			System.out.println("");
		   		}
		   		String inputFile = tokens[1];
		   		String DBName=null;
		   		if(tokens.length>2)
		   			DBName = tokens[2];
			   	
			   	if(operation.equalsIgnoreCase("batchnodeinsert")) {

			   		if(graphdbname==null) {
			   			graphdbname = DBName;
			   			createDB(DBName);
			   		}
			   		//SystemDefs.JavabaseBM.flushAllPages();
			   		//SystemDefs.JavabaseBM = new BufMgr(40, "Clock");
			   		//SystemDefs.JavabaseBM.flushAllPages();
			   		new BatchNodeInsert().batchNodeInsert(inputFile, graphdbname);
			   	} else if (operation.equalsIgnoreCase("batchnodedelete")) {
			   		if(graphdbname==null) {
			   			graphdbname = DBName;
			   			createDB(DBName);
			   		}
			   		/*SystemDefs.JavabaseBM.flushAllPages();
			   		SystemDefs.JavabaseBM = new BufMgr(30, "Clock");
			   		SystemDefs.JavabaseBM.flushAllPages();*/
			   		new BatchNodeDelete().batchNodeDelete(inputFile, graphdbname);
			   	} else if (operation.equalsIgnoreCase("fullnodescan")) {
			   		SystemDefs.JavabaseBM.flushAllPages();
			   		SystemDefs.JavabaseBM = new BufMgr(50, "Clock");
			   		SystemDefs.JavabaseBM.flushAllPages();
			   		new FullScanNode().fullScanNode( graphdbname);
			   	} else if (operation.equalsIgnoreCase("fulledgescan")) {
			   		new FullScanEdge().fullScanEdge( graphdbname);
			   	} else if (operation.equalsIgnoreCase("batchedgeinsert")) {
			   		if(graphdbname==null) {
			   			graphdbname = DBName;
			   			createDB(DBName);
			   		} //else 
			   		//	openDB(DBName);
			   		//SystemDefs.JavabaseBM.flushAllPages();
			   		new BatchEdgeInsert().batchEdgeInsert(inputFile);
			   	} else if (operation.equalsIgnoreCase("nodequery")) {
			   		//int numBuf = Integer.parseInt(tokens[3]);
			   		if(graphdbname==null) {
			   			graphdbname = DBName;
			   		}
			   		new NodeQuery().nodeQuery(graphdbname, 40,3,1,"30 40 50 60 70 5"); //put label
			  	} else if (operation.equalsIgnoreCase("edgequery")) {
				   		//int numBuf = Integer.parseInt(tokens[3]);
				   		if(graphdbname==null) {
				   			graphdbname = DBName;
				   		}
				   		//System.out.println("Whuy!!");
				   	new EdgeQuery().edgeQuery(graphdbname, 40,2,0,"10 20");//put space sep lowerbound and upper bound
				   //	System.out.println("Whuy!!--");
			   	} else if (operation.equalsIgnoreCase("batchedgedelete")) {
			   		if(graphdbname==null) {
			   			graphdbname = DBName;
			   			createDB(DBName);
			   		}// else 
			   		//	openDB(DBName);
			   		new BatchEdgeDelete().batchEdgeDelete(inputFile);
			   	} else {
			   		System.out.println(operation + " is not recognized as a command.\nType help for more information.");
			   	}
		   	}
		}  
	   public static void createDB(String graphDbName) {
		   SystemDefs sysdef = new SystemDefs(graphDbName,10000,20,"Clock",0);
	   }
	   public static void openDB(String graphDbName) throws HashOperationException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, IOException {
		   SystemDefs.JavabaseBM.flushAllPages();
		   System.out.println("hihihihi");
		   SystemDefs sysdef = new SystemDefs(graphDbName,0,20,"Clock",0);
	   }
}
