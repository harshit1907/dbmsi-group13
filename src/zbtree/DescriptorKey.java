package zbtree;

import global.BitShufflingUtils;
import global.Descriptor;

/**
 * Created by prakhar on 3/8/17.
 */
public class DescriptorKey extends KeyClass {
    private String key;

    public DescriptorKey(Descriptor desc) {
        key = BitShufflingUtils.bitShuffle(desc);
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String toString() {
        return key;
    }
}
