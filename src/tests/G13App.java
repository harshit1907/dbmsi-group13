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

// batchnodeinsert /Users/mihir/dev/NodeTestData.txt db1
// batchnodedelete /Users/mihir/dev/NodeTestData2.txt db1
// 
//


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
		   		String DBName = tokens[2];
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
			   	} else if (operation.equalsIgnoreCase("batchedgeinsert")) {
			   		
			   	} else if (operation.equalsIgnoreCase("batchedgedelete")) {
			   		
			   	} else {
			   		System.out.println(operation + " is not recognized as a command.\nType help for more information.");
			   	}
		   	}
		   	
		}  
	   public static void createDB(String graphDbName) {
		   SystemDefs sysdef = new SystemDefs(graphDbName,10000,1000,"Clock");
	   }
	   
}
