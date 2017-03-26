package ZIndex;

import global.*;
import zbtree.*;
import iterator.CondExpr;
import iterator.UnknownKeyTypeException;
import nodeheap.Node;

import java.io.IOException;
import java.util.*;

import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;


/**
 * IndexUtils class opens an index scan based on selection conditions.
 * Currently only BTree_scan is supported
 */
public class ZIndexUtils {

  /**
   * BTree_scan opens a BTree scan based on selection conditions
   * @param selects conditions to apply
   * @param indFile the index (BTree) file
   * @return an instance of IndexFileScan (BTreeFileScan)
   * @exception IOException from lower layer
   * @exception UnknownKeyTypeException only int and string keys are supported
   * @exception index.InvalidSelectionException selection conditions (selects) not valid
   * @exception KeyNotMatchException Keys do not match
   * @exception UnpinPageException unpin page failed
   * @exception PinPageException pin page failed
   * @exception IteratorException iterator exception
   * @exception ConstructPageException failed to construct a header page
   */
  public static IndexFileScan ZBTree_scan(CondExpr[] selects, ZIndexFile indFile)
		  throws IOException,
		  UnknownKeyTypeException,
		  index.InvalidSelectionException,
		  KeyNotMatchException,
		  UnpinPageException,
		  PinPageException,
		  IteratorException,
		  ConstructPageException, DescriptorNotMatchException, InvalidSelectionException {
      IndexFileScan indScan;

      if (selects == null || selects[0] == null) {
			indScan = ((ZBTreeFile)indFile).new_scan(null, null);
			return indScan;
      }

      if (selects[1] == null) {
		  if (selects[0].type1.attrType != AttrType.attrSymbol && selects[0].type2.attrType != AttrType.attrSymbol) {
	  		throw new index.InvalidSelectionException("IndexUtils.java: Invalid selection condition");
	}

	KeyClass key;

	// symbol = value
	if (selects[0].op.attrOperator == AttrOperator.aopEQ) {
	  if (selects[0].type1.attrType != AttrType.attrSymbol) {
	    key = getValue(selects[0], selects[0].type1, 1);
	    indScan = ((ZBTreeFile)indFile).new_scan(key, key);
	  }
	  else {
	    key = getValue(selects[0], selects[0].type2, 2);
	    indScan = ((ZBTreeFile)indFile).new_scan(key, key);
	  }
	  return indScan;
	}

	// symbol < value or symbol <= value
	if (selects[0].op.attrOperator == AttrOperator.aopLT || selects[0].op.attrOperator == AttrOperator.aopLE) {
	  if (selects[0].type1.attrType != AttrType.attrSymbol) {
	    key = getValue(selects[0], selects[0].type1, 1);
	    indScan = ((ZBTreeFile)indFile).new_scan(null, key);
	  }
	  else {
	    key = getValue(selects[0], selects[0].type2, 2);
	    indScan = ((ZBTreeFile)indFile).new_scan(null, key);
	  }
	  return indScan;
	}

	// symbol > value or symbol >= value
	if (selects[0].op.attrOperator == AttrOperator.aopGT || selects[0].op.attrOperator == AttrOperator.aopGE) {
	  if (selects[0].type1.attrType != AttrType.attrSymbol) {
	    key = getValue(selects[0], selects[0].type1, 1);
	    indScan = ((ZBTreeFile)indFile).new_scan(key, null);
	  }
	  else {
	    key = getValue(selects[0], selects[0].type2, 2);
	    indScan = ((ZBTreeFile)indFile).new_scan(key, null);
	  }
	  return indScan;
	}

	// error if reached here
	System.err.println("Error -- in IndexUtils.ZBTree_scan()");
	return null;
      }
      else {
	// selects[1] != null, must be a range query
	if (selects[0].type1.attrType != AttrType.attrSymbol && selects[0].type2.attrType != AttrType.attrSymbol) {
	  throw new index.InvalidSelectionException("IndexUtils.java: Invalid selection condition");
	}
	if (selects[1].type1.attrType != AttrType.attrSymbol && selects[1].type2.attrType != AttrType.attrSymbol) {
	  throw new InvalidSelectionException("IndexUtils.java: Invalid selection condition");
	}
	
	// which symbol is higher??
	KeyClass key1, key2;
	AttrType type;
	
	if (selects[0].type1.attrType != AttrType.attrSymbol) {
	  key1 = getValue(selects[0], selects[0].type1, 1);
	  type = selects[0].type1;
	}
	else {
	  key1 = getValue(selects[0], selects[0].type2, 2);
	  type = selects[0].type2;
	}
	if (selects[1].type1.attrType != AttrType.attrSymbol) {
	  key2 = getValue(selects[1], selects[1].type1, 1);
	}
	else {
	  key2 = getValue(selects[1], selects[1].type2, 2);
	}
	
	switch (type.attrType) {
	case AttrType.attrDesc:
	  if (((DescriptorKey)key1).getKey().compareTo(((DescriptorKey)key2).getKey()) < 0) {
	    indScan = ((ZBTreeFile)indFile).new_scan(key1, key2);
	  }
	  else {
	    indScan = ((ZBTreeFile)indFile).new_scan(key2, key1);
	  }
	  return indScan;


	default:
	  // error condition
	  throw new UnknownKeyTypeException("IndexUtils.java: Only Integer and String keys are supported so far");	
	}
      } // end of else 
      
    } 

  /**
   * getValue returns the key value extracted from the selection condition.
   * @param cd the selection condition
   * @param type attribute type of the selection field
   * @param choice first (1) or second (2) operand is the value
   * @return an instance of the KeyClass (IntegerKey or StringKey)
   * @exception UnknownKeyTypeException only int and string keys are supported 
   */
  private static KeyClass getValue(CondExpr cd, AttrType type, int choice)
       throws UnknownKeyTypeException {
    // error checking
    if (cd == null) {
      return null;
    }
    if (choice < 1 || choice > 2) {
      return null;
    }
    
    switch (type.attrType) {
    case AttrType.attrDesc:
      if (choice == 1) return new DescriptorKey(cd.operand1.string);

      /*
      // need FloatKey class in bt.java
      if (choice == 1) return new FloatKey(new Float(cd.operand.real));
      else return new FloatKey(new Float(cd.operand.real));
      */
    default:
	throw new UnknownKeyTypeException("IndexUtils.java: Only Integer and String keys are supported so far");
    }
  }

    private static int calculateDiagonalEdgeCeil(int v0, int distance) {
        return (int) Math.ceil(v0 + distance / Math.sqrt(5.0));
    }
    private static int calculateDiagonalEdgeFloor(int v0, int distance) {
        return (int) Math.floor(v0 - distance / Math.sqrt(5.0));
    }

  public static DescriptorRangePair getDiagonalDescFromDistance(KeyClass key, int distance) {
    int value[] = ((DescriptorKey) key).getDesc().value;

    String min1 = "";
    String max1 = "";
    int res1[] = new int[5];
    for (int i = 0; i < 2; i++) {
        res1[0] = (i == 0) ? calculateDiagonalEdgeCeil(value[0], distance):
                calculateDiagonalEdgeFloor(value[0], distance);

        for (int j = 0; j < 2; j++) {
            res1[1] = j == 0 ? calculateDiagonalEdgeCeil(value[1], distance):
                calculateDiagonalEdgeFloor(value[1], distance);
            for (int k = 0; k < 2; k++) {

                res1[2] = k == 0 ? calculateDiagonalEdgeCeil(value[2], distance):
                calculateDiagonalEdgeFloor(value[2], distance);
                for (int l = 0; l < 2; l++) {

                    res1[3] = l == 0 ? calculateDiagonalEdgeCeil(value[3], distance):
                calculateDiagonalEdgeFloor(value[3], distance);
                    for (int m = 0; m < 2; m++) {

                        res1[4] = m == 0 ? calculateDiagonalEdgeCeil(value[4], distance):
                calculateDiagonalEdgeFloor(value[4], distance);
                        Descriptor desc = new Descriptor();
                        desc.set(res1[0], res1[1], res1[2], res1[3], res1[4]);
                        String zvalue = new DescriptorKey(desc).getKey();
                        if (min1.isEmpty()) min1 = zvalue;
                        else
                            min1 = zvalue.compareTo(min1) > 0 ? min1 : zvalue;
                        if (max1.isEmpty()) max1 = zvalue;
                        else
                            max1 = zvalue.compareTo(max1) > 0 ? zvalue : max1;
                    }
                }
            }
        }
    }
    KeyClass lower=new DescriptorKey(min1);

    KeyClass upper=new DescriptorKey(max1);
    return new DescriptorRangePair((DescriptorKey) lower, (DescriptorKey) upper);
    }

  public static List<DescriptorRangePair> getRangesForDescRange(DescriptorRangePair pair,Descriptor target,
                                                                int distance) throws
          GetFileEntryException, ConstructPageException, AddFileEntryException, IOException,
          InvalidFrameNumberException, ReplacerException, PageUnpinnedException, HashEntryNotFoundException {
      List<DescriptorRangePair> result = new LinkedList<>();
      LinkedList<DescriptorRangePair> queue = new LinkedList<>();
      if (findFalsePositives(pair.getStart().getKey(), pair.getEnd().getKey(), target, distance)) {
          result.add(pair);
          return result;
      } else {
          queue.add(pair);
      }

      while (!queue.isEmpty()) {
          DescriptorRangePair range = queue.remove();
          String startKey = range.getStart().getKey();
          String endKey = range.getEnd().getKey();
          DescriptorRangePair lower = null,upper = null;
          for (int i = 0; i < startKey.length(); i++) {
              if (startKey.charAt(i) != endKey.charAt(i)) {
                  int axis = i % 5;
                  int posOnAxis = i / 5;
                  String[] lit = BitShufflingUtils.stringsFromKey(startKey);
                  String[] big = BitShufflingUtils.stringsFromKey(endKey);

                  String litMax = getLitMax(lit[axis], posOnAxis);
                  String bigMin = getBigMin(big[axis], posOnAxis);

                  String[] litStart = lit.clone();
                  String[] litEnd = big.clone();
                  litEnd[axis] = litMax;
                  lower = new DescriptorRangePair(
                          BitShufflingUtils.keyFromStrings(litStart),
                          BitShufflingUtils.keyFromStrings(litEnd));

                  String[] bigStart = lit.clone();
                  bigStart[axis] = bigMin;
                  String[] bigEnd = big.clone();
                  upper = new DescriptorRangePair(
                          BitShufflingUtils.keyFromStrings(bigStart),
                          BitShufflingUtils.keyFromStrings(bigEnd));
                  
                  if (findFalsePositives(lower.getStart().getKey(), lower.getEnd().getKey(),target,distance)) {
                      DescriptorRangePair ansLow= new DescriptorRangePair(lower.getStart(), lower.getEnd());
                      result.add(ansLow);
                  } else {
                      queue.add(lower);
                  }
                  if (findFalsePositives(upper.getStart().getKey(), upper.getEnd().getKey(),target,distance)) {
                      DescriptorRangePair ansHigh= new DescriptorRangePair(upper.getStart(), upper.getEnd());
                      result.add(ansHigh);
                  } else {
                      queue.add(upper);
                  }
                  break;
              }
          }
      }
      result.forEach(x -> {
          System.out.println("Start: " + x.getStart().getKey());
          System.out.println("End: " + x.getEnd().getKey());
          System.out.println();
      });
      return result;
  }

  private static boolean findFalsePositives(String start, String end, Descriptor target, int distance)
          throws GetFileEntryException, ConstructPageException, AddFileEntryException,
          IOException, InvalidFrameNumberException, ReplacerException,
          PageUnpinnedException, HashEntryNotFoundException {

    boolean OK   = true; 
    boolean FAIL = false;
    int falsePts=0;
    boolean status = OK;
    SystemDefs.JavabaseDB.ztNodeDesc = new ZBTreeFile(SystemDefs.JavabaseDBName+"_ZTreeNodeIndex", AttrType.attrString, 180, 1/*delete*/);
    // start index scan
    ZBTFileScan izscan = null;
    try {
        KeyClass low_key = new DescriptorKey(start);
        KeyClass high_key = new DescriptorKey(end);
        izscan = SystemDefs.JavabaseDB.ztNodeDesc.new_scan(low_key,high_key);
    }
    catch (Exception e) {
        status = FAIL;
        e.printStackTrace();
    }

    DescriptorDataEntry tz=null;
    try {
        tz = izscan.get_next();
    }
    catch (Exception e) {
        status = FAIL;
        e.printStackTrace();
    }
    boolean flag = true;
    //System.out.println(t+""+iscan);
    while (tz != null && izscan!=null) {
//System.out.println("hii");
        
        try {
            DescriptorKey k = (DescriptorKey)tz.key;
            
            zbtree.LeafData l = (zbtree.LeafData)tz.data;
            NID nid =  l.getData();

            Node node = SystemDefs.JavabaseDB.nhfile.getNode(nid);

            //System.out.println("Inside: "+node.getDesc().getString()+" Dist: "+((int)target.distance(node.getDesc())));
            if((target.distance(node.getDesc()))>distance)
            {
                //System.out.println("Reason: "+node.getDesc().getString()+" Dist: "+((int)target.distance(node.getDesc())));
                falsePts++;
            }
       //     System.out.println("Key: "+k.getKey()+" Label: "+node.getLabel()+" -- Descripotr: "+node.getDesc().value[0]+" "+node.getDesc().value[1]+" "+node.getDesc().value[2]+" "+node.getDesc().value[3]+" "+node.getDesc().value[4]);

        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        try {
            tz = izscan.get_next();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        if(falsePts>0)
        {
           //izscan.DestroyZBTreeFileScan();
            break;
        }
    }

    // clean up
    try {
        //iscan.close();
        SystemDefs.JavabaseDB.ztNodeDesc.close();
    }
    catch (Exception e) {
        status = FAIL;
        e.printStackTrace();
    }
    if(falsePts>0)
    {
        return false; 
    }
    return true;
  }

private static String getLitMax(String lit, int pos) {
      StringBuilder res = new StringBuilder();
	  for (int i = 0; i < 32; i++) {
		  if (i < pos) res.append(lit.charAt(i));
		  else if (i == pos) res.append(0);
		  else res.append("1");
	  }
	  return res.toString();
  }

  private static String getBigMin(String big, int pos) {
	  StringBuilder res = new StringBuilder();
	  for (int i = 0; i < 32; i++) {
		  if (i < pos) res.append(big.charAt(i));
		  else if (i == pos) res.append(1);
		  else res.append("0");
	  }
	  return res.toString();
  }
}
