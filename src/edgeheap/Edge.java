package edgeheap;

import java.io.IOException;

import global.AttrType;
import global.Convert;
import global.Descriptor;
import global.NID;
import heap.Tuple;




public class Edge extends Tuple{

	 /** 
	  * Maximum size of any tuple
	  */
	  public static final int max_size = 50;

	 /** 
	   * a byte array to hold data
	   */
	  private byte [] data;

	  /**
	   * start position of this tuple in data[]
	   */
	  private int edge_offset;

	  /**
	   * length of this node
	   */
	  private int edge_length;

	  /** 
	   * private field
	   * Number of fields in this tuple
	   */
	  private short fldCnt;

	  /** 
	   * private field
	   * Array of offsets of the fields
	   */
	 
	  private short [] fldOffset;
	  
	  
	public NID getSource() throws IOException {
		return Convert.getNIDValue(4, data);
	}


	public NID getDestination() throws IOException {
		return Convert.getNIDValue(12, data);
	}



	

   /**
    * Class constructor
    * Create a new node with length = max_size,node offset = 0.
    */
    public Edge()
    {
         // Create a new tuple
         data = new byte[max_size];
         edge_offset = 0;
         edge_length = max_size;
    }
     
     /** Constructor
      * @param atuple a byte array which contains the node
      * @param offset the offset of the node in the byte array
      * @param length the length of the node
      */

     public Edge(byte [] anode, int offset, int length)
     {
    	super(anode,  offset,  length);
        data = anode;
        edge_offset = offset;
        edge_length = length; // let's keep this for now
      //  fldCnt = getShortValue(offset, data);
     }
     
     /** Constructor(used as node copy)
      * @param fromNode   a byte array which contains the tuple
      * 
      */
     public Edge(Edge fromNode)
     {
         data = fromNode.getEdgeByteArray();
         edge_length = fromNode.getLength();
         edge_offset = 0;
         fldCnt = 4; // fixed two fields in node
         // fldOffset = 0; 
         // what's this? I think we do not require this, as there are no variable fields in here 
     }
     
     public String getLabel() throws IOException {
         return Convert.getStrValue(20, data, data.length-20);
     }
     
     public int getWeight() throws IOException {
         return Convert.getIntValue(0, data);
     }
     
     public void setSource(NID source) throws IOException {
    	 Convert.setNIDValue(source,4, data);
 	}
     
     public void setLabel(String label) throws IOException {
         Convert.setStrValue(label, 20, data);
     }

     public void setWeight(int weight) throws IOException {
         Convert.setIntValue(weight, 0, data);
     }
     
 	public void setDestination(NID destination) throws IOException {
 		Convert.setNIDValue(destination, 12, data);
	}
     
     /** Copy the node byte array out
      *  @return  byte[], a byte array contains the node
      *		the length of byte[] = length of the node
      */
      
     public byte [] getEdgeByteArray() 
     {
         byte [] nodecopy = new byte [edge_length];
         System.arraycopy(data, edge_offset, nodecopy, 0, edge_length);
         return nodecopy;
     }
     
     public void print(AttrType type[]) //TODO : print() after NID class
             throws IOException
     {
         int i, val;
         float fval;
         String sval;
         Descriptor dval; 

         System.out.print("[");
         for (i=0; i< fldCnt-1; i++)
         {
             switch(type[i].attrType) {

                 case AttrType.attrInteger:
                     val = Convert.getIntValue(fldOffset[i], data);
                     System.out.print(val);
                     break;

                 case AttrType.attrReal:
                     fval = Convert.getFloValue(fldOffset[i], data);
                     System.out.print(fval);
                     break;

                 case AttrType.attrString:
                     sval = Convert.getStrValue(fldOffset[i], data,fldOffset[i+1] - fldOffset[i]);
                     System.out.print(sval);
                     break;
                 
                 case AttrType.attrDesc:
                     dval = Convert.getDescValue(fldOffset[i], data);
                     System.out.print(dval);
                     break;

                 case AttrType.attrNull:
                 case AttrType.attrSymbol:
                     break;
             }
             System.out.print(", ");
         }
         switch(type[fldCnt-1].attrType) {

             case AttrType.attrInteger:
                 val = Convert.getIntValue(fldOffset[i], data);
                 System.out.print(val);
                 break;

             case AttrType.attrReal:
                 fval = Convert.getFloValue(fldOffset[i], data);
                 System.out.print(fval);
                 break;

             case AttrType.attrString:
                 sval = Convert.getStrValue(fldOffset[i], data,fldOffset[i+1] - fldOffset[i]);
                 System.out.print(sval);
                 break;
             
             case AttrType.attrDesc:
                 dval = Convert.getDescValue(fldOffset[i], data);
                 System.out.print(dval);
                 break;

             case AttrType.attrNull:
             case AttrType.attrSymbol:
                 break;
         }
         System.out.println("]");

     }
     
     public void edgeCopy(Edge fromNode)
     {
         byte [] temparray = fromNode.getEdgeByteArray();
         System.arraycopy(temparray, 0, data, edge_offset, temparray.length);//Not sure: edge_length
//        fldCnt = fromTuple.noOfFlds();
//        fldOffset = fromTuple.copyFldOffset();
     }
     public void edgeInit(byte [] anode, int offset)
     {
         data = anode;
         edge_offset = offset;
         edge_length = data.length; //Not sure
     }

     public void edgeSet(byte [] fromnode, int offset)
     {
         System.arraycopy(fromnode, offset, data, 0, fromnode.length);
         edge_offset = 0;
         edge_length = fromnode.length;
     }
     
     public short size()
     {
         return ((short) (fldOffset[fldCnt] - edge_offset));
     }
}