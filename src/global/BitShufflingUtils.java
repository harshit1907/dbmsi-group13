package global;

/**
 * Created by prakhar on 3/8/17.
 */
public class BitShufflingUtils {
    public final static String bitShuffle(Descriptor desc) {
        String d0 = Long.toBinaryString( Integer.toUnsignedLong(desc.get(0)) | 0x100000000L ).substring(1);
        String d1 = Long.toBinaryString( Integer.toUnsignedLong(desc.get(1)) | 0x100000000L ).substring(1);
        String d2 = Long.toBinaryString( Integer.toUnsignedLong(desc.get(2)) | 0x100000000L ).substring(1);
        String d3 = Long.toBinaryString( Integer.toUnsignedLong(desc.get(3)) | 0x100000000L ).substring(1);
        String d4 = Long.toBinaryString( Integer.toUnsignedLong(desc.get(4)) | 0x100000000L ).substring(1);

        StringBuilder res = new StringBuilder();
        for (int i = 0; i < 32; i++) {
            res.append(d0.charAt(i)).append(d1.charAt(i)).append(d2.charAt(i)).append(d3.charAt(i))
                    .append(d4.charAt(i));
        }

        return res.toString();
    }
}
