package queryPojo;

import java.io.IOException;

import global.Descriptor;
import global.NID;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;

/**
 * Key 1 : Node Label;
 * Key 2 : Desc
 */
public class NodeQueryPojo {

    NID nd;
    String label;
    Descriptor desc;
    int key;
    public int getKey() {
        return key;
    }
    public NID getNd() {
        return nd;
    }
    public void setNd(NID nd) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, IOException, Exception {
        this.key=1;
        this.label= SystemDefs.JavabaseDB.nhfile.getNode(nd).getLabel();
        this.nd = nd;
    }
    public String getLabel() {
        return label;
    }
    public void setLabel(String label) {
        this.key = 1;
        this.label = label;
    }
    public Descriptor getDesc() {
        return desc;
    }
    public void setDesc(Descriptor desc) {
        this.key = 2;
        this.desc = desc;
    }
}
