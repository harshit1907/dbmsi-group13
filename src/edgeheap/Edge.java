package edgeheap;

import java.io.IOException;

import global.AttrType;
import global.Convert;
import global.NID;
import heap.Tuple;//not sure




public class Edge extends Tuple{

    private short fldCnt;

    private short [] fldOffset;

    private int edge_offset;
    private int edge_length;

    /**
     * length of this tuple
     */

    public static final int max_size = MINIBASE_PAGESIZE;
    public static NID Source;//TODO : change to NID
    public static NID Destination;//TODO : change to NID
    public static int Label=AttrType.attrString;
    public static int Weight=AttrType.attrInteger;

    public static void setSource(NID source) {
        Source = source;
    }

    public static void setDestination(NID destination) {
        Destination = destination;
    }

    public static void setLabel(int label) {
        Label = label;
    }

    public static void setWeight(int weight) {
        Weight = weight;
    }



    public static NID getSource() {
        return Source;
    }

    public static NID getDestination() {
        return Destination;
    }

    public static int getLabel() {
        return Label;
    }

    public static int getWeight() {
        return Weight;
    }

    /**

     * a byte array to hold data
     */
    private byte [] data;

    public  Edge()
    {
        // Creat a new edge
        data = new byte[max_size];
        edge_offset = 0;

    }
    public Edge(byte [] aedge, int offset)
    {
        data = aedge;
        edge_offset = offset;
        edge_length = aedge.length; //not sure: should keep or not because not in construct argument
        //  fldCnt = getShortValue(offset, data);
    }
    public Edge(Edge fromEdge)
    {
        data = fromEdge.getEdgeByteArray();
        edge_length = fromEdge.getLength();
        edge_offset = 0;
        fldCnt = fromEdge.noOfFlds();
        fldOffset = fromEdge.copyFldOffset();
    }
    public void edgeCopy(Edge fromEdge)
    {
        byte [] temparray = fromEdge.getEdgeByteArray();
        System.arraycopy(temparray, 0, data, edge_offset, temparray.length);//Not sure: edge_length
//       fldCnt = fromTuple.noOfFlds();
//       fldOffset = fromTuple.copyFldOffset();
    }
    public void edgeInit(byte [] aedge, int offset)
    {
        data = aedge;
        edge_offset = offset;
        edge_length = data.length;//Not sure
    }

    public void edgeSet(byte [] fromedge, int offset)
    {
        System.arraycopy(fromedge, offset, data, 0, fromedge.length);
        edge_offset = 0;
        edge_length = fromedge.length;
    }

    public byte [] getEdgeByteArray()
    {
        byte [] edgecopy = new byte [data.length];
        System.arraycopy(data, edge_offset, edgecopy, 0, data.length);//Not Sure
        return edgecopy;
    }

    public int getLength()
    {
        return edge_length;
    }

    public short noOfFlds()
    {
        return fldCnt;
    }

    public short[] copyFldOffset()
    {
        short[] newFldOffset = new short[fldCnt + 1];
        for (int i=0; i<=fldCnt; i++) {
            newFldOffset[i] = fldOffset[i];
        }

        return newFldOffset;
    }

    public byte [] getNodeByteArray()
    {
        byte [] edgecopy = new byte [data.length];
        System.arraycopy(data, edge_offset, edgecopy, 0, data.length);
        return edgecopy;
    }

    public void print(AttrType type[]) //TODO : print() after NID class
            throws IOException
    {
        int i, val;
        float fval;
        String sval;

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

            case AttrType.attrNull:
            case AttrType.attrSymbol:
                break;
        }
        System.out.println("]");

    }
    public short size()
    {
        return ((short) (fldOffset[fldCnt] - edge_offset));
    }
    }