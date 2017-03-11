package tests;

import java.io.BufferedReader;

import java.io.IOException;
import java.io.InputStreamReader;

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
// Mihir
// batchnodeinsert /Users/mihir/dev/NodeTestData.txt db1
// batchnodedelete /Users/mihir/dev/NodeTestData2.txt db1
// batchedgeinsert /Users/mihir/dev/EdgeTestData.txt db1
// batchedgedelete /Users/mihir/dev/EdgeTestData2.txt db1

// fullnodescan db1
// fulledgescan db1



public class G13App {
	   public static void main(String[] agrs) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, IOException, Exception {
		   	//String graphdbname=args[0];
		   	String graphdbname=null;
		   	
		   	while(true) {
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
		   		String DBName="db1";
		   		if(tokens.length>2)
		   			DBName = tokens[2];
			   	//System.out.println("You entered the command: "+operation+" -- "+nodefile);
			   	if(operation.equalsIgnoreCase("batchnodeinsert")) {
			   		//System.out.println("You entered the command: "+operation+" -- "+inputFile);
			   		if(graphdbname==null) {
			   			graphdbname = DBName;
			   			createDB(DBName);
			   		}
			   		
			   		new BatchNodeInsert().batchNodeInsert(inputFile, graphdbname);
			   	} else if (operation.equalsIgnoreCase("batchnodedelete")) {
			   		if(graphdbname==null) {
			   			graphdbname = DBName;
			   			createDB(DBName);
			   		}
			   		new BatchNodeDelete().batchNodeDelete(inputFile, graphdbname);
			   	} else if (operation.equalsIgnoreCase("fullnodescan")) {
			   		new FullScanNode().fullScanNode( graphdbname);
			   	} else if (operation.equalsIgnoreCase("fulledgescan")) {
			   		new FullScanEdge().fullScanEdge( graphdbname);
			   	} else if (operation.equalsIgnoreCase("batchedgeinsert")) {
			   		if(graphdbname==null) {
			   			graphdbname = DBName;
			   			createDB(DBName);
			   		}
			   		new BatchEdgeInsert().batchEdgeInsert(inputFile);
			   	} else if (operation.equalsIgnoreCase("batchedgedelete")) {
			   		if(graphdbname==null) {
			   			graphdbname = DBName;
			   			createDB(DBName);
			   		}
			   		new BatchEdgeDelete().batchEdgeDelete(inputFile);
			   	} else {
			   		System.out.println(operation + " is not recognized as a command.\nType help for more information.");
			   	}
		   	}
		   	
		}  
	   public static void createDB(String graphDbName) {
		   SystemDefs sysdef = new SystemDefs(graphDbName,10000,10000,"Clock");
	   }

}
