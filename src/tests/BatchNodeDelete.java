package tests;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import diskmgr.PCounter;
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
		System.out.println ("\n  Task 12: Batch Delete Nodes\n");
	    BufferedReader br = null ;
	    
	    
		try {
			br= new BufferedReader(new FileReader(nodefilename));
			String line = br.readLine();
			int countA=0,countD=0;
			while (line != null) {
				String nodelabel = line;
				NID nid = new NID();
				countA++;
				//SystemDefs.JavabaseDB.
				//System.out.println("  - Open the same heap file as test 1\n");

				Node nd=new Node();
				nd.setLabel(nodelabel);
				nid=SystemDefs.JavabaseDB.nhfile.getNID(nd);
				if(nid!=null){
					try {
						SystemDefs.JavabaseDB.nhfile.deleteNode(nid);
						countD++;
					} catch (Exception e) {
						System.err.println("*** Error deleting record " + nd.getLabel() + "\n");
						e.printStackTrace();
					}
				}

//				if (status == OK) {
//					try {
//						scan = SystemDefs.JavabaseDB.nhfile.openScan();
//						
//					} catch (Exception e) {
//						status = FAIL;
//						System.err.println("*** Error opening scan\n");
//						e.printStackTrace();
//						return false;
//					}
//				}
//				if (status == OK) {
//					Node node = new Node();
//					boolean done = false;
//
//					while (!done) {
//						try {
//							node = scan.getNext(nid);
//							if (node == null) {
//								done = true;
//							}
//							//System.out.println(nid+"   "+SystemDefs.JavabaseDB.nhfile.getNode(nid));
//							
//						} catch (Exception e) {
//							status = FAIL;
//							e.printStackTrace();
//							
//						}
//
//						//System.out.println("Deleting this .."+node.getLabel()+" ^^^ "+nodelabel);
//						
//						if (!done && status == OK) {
//							boolean del = false;
//							if (nodelabel.equals(node.getLabel()))
//							{
//								del = true;
//								//System.out.print("Deleting this .."+nodelabel);
//							}
//							if (del) { 
//								try {
//									status = SystemDefs.JavabaseDB.nhfile.deleteNode(nid);
//									scan.getNext(nid);
//									countD++;
//									done=true;
//								} catch (Exception e) {
//									status = FAIL;
//									System.err.println("*** Error deleting record " + node.getLabel() + "\n");
//									e.printStackTrace();
//								}
//							}
//						}
//					}
//				}
//				scan.closescan();





				line=br.readLine();
			}

			System.out.println("Deleted "+countD+" Nodes. Nodes given in file to delete "+countA);

		} finally {
			br.close();
		}


		System.out.println("Node Count: "+SystemDefs.JavabaseDB.getNodeCnt());

		System.out.println("Edge Count: "+SystemDefs.JavabaseDB.getEdgeCnt());

		System.out.println("Disk Read Count: "+PCounter.readCounter);

		System.out.println("Disk Write Count: "+PCounter.writeCounter);
		
		return true;
	}

}