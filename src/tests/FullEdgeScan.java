package tests;

import java.io.IOException;

import edgeheap.EScan;
import edgeheap.Edge;
import global.EID;
import global.NID;
import global.SystemDefs;
import heap.InvalidTupleSizeException;
import nodeheap.NScan;
import nodeheap.Node;

public class FullEdgeScan {
	 private final static boolean OK = true;
	  private final static boolean FAIL = false;
		
	 protected  boolean fullScanEdge (String graphDbName) throws IOException, InvalidTupleSizeException {
		 
		 EScan scan = null;
		 boolean status = OK;
		    
		    if ( status == OK ) {	
		      System.out.println ("  - Scanning edges \n");
		      
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
		 	System.out.print("Label: "+edge.getLabel());
		 	System.out.println(" Descriptor: "+edge.getWeight());
		 	System.out.println(" Source : ("+edge.getSource().slotNo+","+edge.getSource().pageNo.pid+")");
		 	System.out.println("Destination :("+edge.getDestination().slotNo+","+edge.getSource().pageNo.pid+")");
		        }
		    
		    }

		return false;
		 
	 }
}
