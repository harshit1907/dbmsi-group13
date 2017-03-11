package tests;

import java.io.IOException;

import global.NID;
import global.SystemDefs;
import heap.InvalidTupleSizeException;
import nodeheap.NScan;
import nodeheap.Node;

public class FullScanNode {
	private final static boolean OK = true;
	private final static boolean FAIL = false;

	protected  boolean fullScanNode (String graphDbName) throws IOException, InvalidTupleSizeException {

		NScan scan = null;
		boolean status = OK;

		if ( status == OK ) {	
			System.out.println ("  - Scan the records just inserted\n");

			try {
				scan = SystemDefs.JavabaseDB.nhfile.openScan();
			}
			catch (Exception e) {
				status = FAIL;
				System.err.println ("*** Error opening scan\n");
				e.printStackTrace();
			}

			if ( status == OK &&  SystemDefs.JavabaseBM.getNumUnpinnedBuffers() 
					== SystemDefs.JavabaseBM.getNumBuffers() ) {
				System.err.println ("*** The heap-file scan has not pinned the first page\n");
				status = FAIL;
			}
		}
		NID nidTmp = new NID();

		if ( status == OK ) {
			Node node = null;

			boolean done = false;
			while (!done) { 
				node = scan.getNext(nidTmp);
				if (node == null) {
					done = true;
					break;
				}
				System.out.print("Label: "+node.getLabel());
				System.out.println(" Descriptor: "+node.getDesc().getString());
			}

		}
		scan.closescan();

		return false;

	}
}
