package tests;
//originally from : joins.C



public class EdgeQuery
{
	
	public EdgeQuery(String graphDBName,int numBuf,int qType,int index,int queryOptions)
	{
		switch(qType)
		{
		case 0: 
			System.out.println(" query will print the edge data in the order it occurs in the node heap");
			break;
		case 1:
			System.out.println(" query will print the edge data  in increasing alphanumerical order of source labels.");
            break;
		case 2:
			System.out.println(" query will print the edge data in increasing alphanumerical order of destination labels");
		    break;
		case 3:
			 System.out.println("query will print the edge data in increasing alphanumerical order of edge labels.");
		     break;
		case 4:
			 System.out.println("query will print the edge data in increasing order of weights");
		     break;
		case 5:
			System.out.println("query will take a lower and upper bound on edge weights, and will return the matching edge data");
			break;
		case 6:
			System.out.println(" query will return pairs of incident graph edges");
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
		EdgeQuery edgequery=new EdgeQuery( graphDBName, numBuf, qType, index, queryOptions);
		
	}
	
}