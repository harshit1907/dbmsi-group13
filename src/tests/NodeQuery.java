package tests;
//originally from : joins.C



public class NodeQuery
{
	
	public NodeQuery(String graphDBName,int numBuf,int qType,int index,int queryOptions)
	{
		switch(qType)
		{
		case 0: 
			System.out.println("  query will print the node data in the order it occurs in the node heap");
			break;
		case 1:
			System.out.println("query will print the node data in increasing alphanumerical order of labels");
            break;
		case 2:
			System.out.println(" query will print the node data in increasing order of distance from a given 5D target descriptor");
		    break;
		case 3:
			 System.out.println("then the query will take a target descriptor and a distance and return the labels of nodes with the given distance from the target descripto");
		     break;
		case 4:
			 System.out.println("query will take a label and return all relevant information (including outgoing and incoming edges) about the node with the matching labe");
		     break;
		case 5:
			System.out.println("query will take a target descriptor and a distance and return all relevant information (including outgoing and incoming edges) about the nodes with the given distance from the target descriptor.");
			break;
		
		default:
			System.out.println("invalid option");
			break;
		}
		
		
	}
	public static void main(String[] args)
	{
		String graphDBName=args[0];
		int numBuf=Integer.parseInt(args[1]);
		int qType=Integer.parseInt(args[2]);
		int index=Integer.parseInt(args[3]);
		int queryOptions=Integer.parseInt(args[4]);
		NodeQuery nodequery=new NodeQuery(graphDBName, numBuf,qType,index,queryOptions);
		
	}
	
}	