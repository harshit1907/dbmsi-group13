package tests;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import global.NID;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import nodeheap.NScan;
import nodeheap.Node;

public class BatchNodeDelete {
	private final static boolean OK = true;
	private final static boolean FAIL = false;

	

	protected  boolean batchNodeDelete (String nodefilename,String graphDbName) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, IOException, Exception {
		System.out.println ("\n  Test 2: Delete fixed-size records\n");
	    BufferedReader br = null ;
	    
	    
		try {
			br= new BufferedReader(new FileReader(nodefilename));
			String line = br.readLine();
			while (line != null) {
				String nodelabel = line;
				NScan scan = null;
				NID nid = new NID();
				boolean status = OK;
				
				//SystemDefs.JavabaseDB.
				//System.out.println("  - Open the same heap file as test 1\n");
				
				if (status == OK) {
					try {
						scan = SystemDefs.JavabaseDB.nhfile.openScan();
						
					} catch (Exception e) {
						status = FAIL;
						System.err.println("*** Error opening scan\n");
						e.printStackTrace();
						return false;
					}
				}
				if (status == OK) {
					Node node = new Node();
					boolean done = false;

					while (!done) {
						try {
							node = scan.getNext(nid);
							if (node == null) {
								done = true;
							}
							//System.out.println(nid+"   "+SystemDefs.JavabaseDB.nhfile.getNode(nid));
							
						} catch (Exception e) {
							status = FAIL;
							e.printStackTrace();
							
						}

						//System.out.println("Deleting this .."+node.getLabel()+" ^^^ "+nodelabel);
						
						if (!done && status == OK) {
							boolean del = false;
							if (nodelabel.equals(node.getLabel()))
							{
								del = true;
								System.out.print("Deleting this .."+nodelabel);
							}
							if (del) { 
								try {
									status = SystemDefs.JavabaseDB.nhfile.deleteNode(nid);
									done=true;
								} catch (Exception e) {
									status = FAIL;
									System.err.println("*** Error deleting record " + node.getLabel() + "\n");
									e.printStackTrace();
									
								}
							}
						}
					}
				}
				scan.closescan();
				line=br.readLine();
			}

		} finally {
			br.close();
		}
		
		return true;
	}

}