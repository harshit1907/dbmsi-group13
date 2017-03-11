package tests;

import global.EID;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import edgeheap.EScan;
import edgeheap.Edge;

public class FullScanEdge {
	private final static boolean OK = true;
	private final static boolean FAIL = false;

	protected  boolean fullScanEdge (String graphDbName) throws HFException, HFDiskMgrException, HFBufMgrException, Exception {
		EScan scan = null;
		boolean status = OK;
		
		if ( status == OK ) {	
			System.out.println ("  - Scan the records just inserted\n");

			try {
				scan = SystemDefs.JavabaseDB.ehfile.openScan();
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
		EID eidTmp = new EID();

		if ( status == OK ) {
			Edge edge = null;

			boolean done = false;
			while (!done) { 
				edge = scan.getNext(eidTmp);
				if (edge == null) {
					done = true;
					break;
				}
				System.out.print(" Label: "+edge.getLabel());
				System.out.print(" Source: "+SystemDefs.JavabaseDB.nhfile.getNode(edge.getSource()).getLabel());
				System.out.print(" Destination: "+SystemDefs.JavabaseDB.nhfile.getNode(edge.getDestination()).getLabel());
				System.out.println(" Weight: "+edge.getWeight());
			}
		}
		scan.closescan();
		return false;
	}
}
