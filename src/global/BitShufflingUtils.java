package global;

import zbtree.DescriptorKey;

/**
 * Created by prakhar on 3/8/17.
 */
public class BitShufflingUtils {
    public static String bitShuffle(Descriptor desc) {
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

    public static Descriptor descFromKey(String key) {
        StringBuilder str1 = new StringBuilder();
        StringBuilder str2 = new StringBuilder();
        StringBuilder str3 = new StringBuilder();
        StringBuilder str4 = new StringBuilder();
        StringBuilder str5 = new StringBuilder();

        for (int i = 0; i < key.length(); i++) {
            char x = key.charAt(i);
            if (i % 5 == 0) str1.append(x);
            else if (i % 5 == 1) str2.append(x);
            else if (i % 5 == 2) str3.append(x);
            else if (i % 5 == 3) str4.append(x);
            else if (i % 5 == 4) str5.append(x);
        }

        Descriptor desc = new Descriptor();
        desc.set(
                Integer.parseInt(str1.toString(), 2),
                Integer.parseInt(str2.toString(), 2),
                Integer.parseInt(str3.toString(), 2),
                Integer.parseInt(str4.toString(), 2),
                Integer.parseInt(str5.toString(), 2)
        );

        return desc;
    }

    public static String[] stringsFromKey(String key) {
        StringBuilder str1 = new StringBuilder();
        StringBuilder str2 = new StringBuilder();
        StringBuilder str3 = new StringBuilder();
        StringBuilder str4 = new StringBuilder();
        StringBuilder str5 = new StringBuilder();

        for (int i = 0; i < key.length(); i++) {
            char x = key.charAt(i);
            if (i % 5 == 0) str1.append(x);
            else if (i % 5 == 1) str2.append(x);
            else if (i % 5 == 2) str3.append(x);
            else if (i % 5 == 3) str4.append(x);
            else if (i % 5 == 4) str5.append(x);
        }

        return new String[] {str1.toString(),
                str2.toString(),
                str3.toString(),
                str4.toString(),
                str5.toString()
        };
    }

    public static DescriptorKey keyFromStrings(String[] strings) {
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < 32; i++) {
            str.append(strings[0].charAt(i))
                    .append(strings[1].charAt(i))
                    .append(strings[2].charAt(i))
                    .append(strings[3].charAt(i))
                    .append(strings[4].charAt(i));
        }
        return new DescriptorKey(str.toString());
    }
}
