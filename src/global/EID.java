package global;

/**
 * Created by pushkar on 2/21/17.
 */
public class EID extends RID {
    /**
     * Default Constructor
     */
    public EID() {}

    /**
     *  constructor of class
     */
    public EID(PageId pageno, int slotno) {
        pageNo = pageno;
        slotNo = slotno;
    }

    /**
     * make a copy of the given Eid
     */
    public void copyEid(EID eid)
    {
        pageNo = eid.pageNo;
        slotNo = eid.slotNo;
    }

    /** Compares two EID object, i.e, this to the rid
     * @param EID EID object to be compared to
     * @return true is they are equal
     *         false if not.
     */
    public boolean equals(EID eid) {
        return (this.pageNo.pid == eid.pageNo.pid)
                && (this.slotNo == eid.slotNo);
    }
}
