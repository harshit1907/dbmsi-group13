package tests;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

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

				NID srcNid = new BatchEdgeInsert().getNodeNID(tokens[0]);
				String srcLabel = SystemDefs.JavabaseDB.nhfile.getNode(srcNid).getLabel();
				
				NID desNid = new BatchEdgeInsert().getNodeNID(tokens[1]);
				String destLabel = SystemDefs.JavabaseDB.nhfile.getNode(desNid).getLabel();

				String edgeLabel = tokens[2];
				String edgeLabel_ = srcLabel+"_"+destLabel;
				countA++;

				if(edgeLabel_.equalsIgnoreCase(edgeLabel)) {
					EID eid = new EID();
					
					Edge nd=new Edge();
					nd.setLabel(edgeLabel);
					eid=SystemDefs.JavabaseDB.ehfile.getEID(nd);
					if(eid!=null){
						try {
							SystemDefs.JavabaseDB.ehfile.deleteEdge(eid);
							countD++;
						} catch (Exception e) {
							System.err.println("*** Error deleting record " + nd.getLabel() + "\n");
							e.printStackTrace();
						}
					}			
				}

				line=br.readLine();
			}
			System.out.println("Deleted "+countD+" Edges. Edges given in file to delete "+countA);

		} finally {
			br.close();
		}
		return true;
	}

}