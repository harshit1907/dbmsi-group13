package tests;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import diskmgr.PCounter;
import global.EID;
import global.NID;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import edgeheap.EScan;
import edgeheap.Edge;

public class BatchEdgeDelete {
	private final static boolean OK = true;
	private final static boolean FAIL = false;



	protected  boolean batchEdgeDelete (String nodefilename) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, IOException, Exception {
		System.out.println ("\n  Test 13: Batch Delete Edges\n");
		BufferedReader br = null ;


		try {
			br = new BufferedReader(new FileReader(nodefilename));

			int countA=0, countD=0;
			String line = br.readLine();
			while (line != null) {
				String []tokens = line.trim().split(" ");

				String edgeLabel = tokens[2];

				countA++;

				EID eid = new EID();

				Edge nd=new Edge();
				nd.setLabel(edgeLabel);
				eid=SystemDefs.JavabaseDB.ehfile.getEID(nd, tokens[0], tokens[1]);
				if(eid!=null){
					try {
						SystemDefs.JavabaseDB.ehfile.deleteEdge(eid);
						countD++;
					} catch (Exception e) {
						System.err.println("*** Error deleting record " + nd.getLabel() + "\n");
						e.printStackTrace();
					}
				}
				line=br.readLine();

			}

			
			System.out.println("Deleted "+countD+" Edges. Edges given in file to delete "+countA);

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