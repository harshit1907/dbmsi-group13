package zbtree;

import global.Descriptor;
import global.NID;
import global.PageId;

/**
 * Created by prakhar on 3/8/17.
 */
public class DescriptorDataEntry {
    public KeyClass key;
    public DataClass data;

    public DescriptorDataEntry(Descriptor desc, PageId pageNo) {
        this.key = new DescriptorKey(desc);
        this.data = new IndexData(pageNo);
    }

    public DescriptorDataEntry(KeyClass key, PageId pageNo) {
        if (key instanceof DescriptorKey) {
            this.key = key;
        }
        data = new IndexData(pageNo);
    }

    public DescriptorDataEntry(Descriptor desc, NID nid) {
        this.key = new DescriptorKey(desc);
        this.data = new LeafData(nid);
    }

    public DescriptorDataEntry(KeyClass key, NID nid) {
        if (key instanceof DescriptorKey) {
            this.key = key;
        }
        data = new LeafData(nid);
    }

    public DescriptorDataEntry(KeyClass key, DataClass data) {
        if (key instanceof DescriptorKey) {
            this.key = key;
        }

        if (data instanceof IndexData) {
            this.data = data;
        } else if (data instanceof LeafData) {
            this.data = data;
        }
    }

    public boolean equals(DescriptorDataEntry entry) {
        boolean st1 = false, st2;

        if (this.key instanceof DescriptorKey) {
            st1 = ((DescriptorKey) this.key).getKey()
                    .equals(((DescriptorKey) entry.key).getKey());
        }

        if( data instanceof IndexData )
            st2 = ( (IndexData)data).getData().pid==
                    ((IndexData)entry.data).getData().pid ;
        else
            st2 = ((NID)((LeafData)data).getData()).equals
                    (((NID)((LeafData)entry.data).getData()));


        return (st1 && st2);
    }
}
