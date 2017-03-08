package nodeheap;

import heap.Tuple; //not sure
import java.io.*;
import java.lang.*;

import edgeheap.Edge;
import global.*;



public class Node extends Tuple{

	 /** 
	  * Maximum size of any tuple
	  */
	  public static final int max_size = MINIBASE_PAGESIZE;

	 /** 
	   * a byte array to hold data
	   */
	  private byte [] data;

	  /**
	   * start position of this tuple in data[]
	   */
	  private int node_offset;

	  /**
	   * length of this node
	   */
	  private int node_length;

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
	  
	  public static int Label=AttrType.attrString;
	  public static int Descriptor=AttrType.attrDesc;

   /**
    * Class constructor
    * Create a new node with length = max_size,node offset = 0.
    */
    public Node()
    {
         // Create a new tuple
         data = new byte[max_size];
         node_offset = 0;
         node_length = max_size;
    }
     
     /** Constructor
      * @param atuple a byte array which contains the node
      * @param offset the offset of the node in the byte array
      * @param length the length of the node
      */

     public Node(byte [] anode, int offset, int length)
     {
        data = anode;
        node_offset = offset;
        node_length = length; // let's keep this for now
      //  fldCnt = getShortValue(offset, data);
     }
     
     /** Constructor(used as node copy)
      * @param fromNode   a byte array which contains the tuple
      * 
      */
     public Node(Node fromNode)
     {
         data = fromNode.getNodeByteArray();
         node_length = fromNode.getLength();
         node_offset = 0;
         fldCnt = 2; // fixed two fields in node
         // fldOffset = 0; 
         // what's this? I think we do not require this, as there are no variable fields in here 
     }
     
     public static int getLabel() {
         return Label;
     }
     
     public static int getDesc() {
         return Descriptor;
     }
     
     public static void setLabel(int label) {
         Label = label;
     }

     public static void setWeight(int Desc) {
         Descriptor = Desc;
     }
     
     /** Copy the node byte array out
      *  @return  byte[], a byte array contains the node
      *		the length of byte[] = length of the node
      */
      
     public byte [] getNodeByteArray() 
     {
         byte [] nodecopy = new byte [node_length];
         System.arraycopy(data, node_offset, nodecopy, 0, node_length);
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
     
     public void nodeCopy(Node fromNode)
     {
         byte [] temparray = fromNode.getNodeByteArray();
         System.arraycopy(temparray, 0, data, node_offset, temparray.length);//Not sure: edge_length
//        fldCnt = fromTuple.noOfFlds();
//        fldOffset = fromTuple.copyFldOffset();
     }
     public void nodeInit(byte [] anode, int offset)
     {
         data = anode;
         node_offset = offset;
         node_length = data.length;//Not sure
     }

     public void nodeSet(byte [] fromnode, int offset)
     {
         System.arraycopy(fromnode, offset, data, 0, fromnode.length);
         node_offset = 0;
         node_length = fromnode.length;
     }
     
     public short size()
     {
         return ((short) (fldOffset[fldCnt] - node_offset));
     }
}