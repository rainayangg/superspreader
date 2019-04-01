import java.net.InetAddress;

// convert ip address to binary address which contains 32bits
public class Converter {
    public static long convertAddressToLong(String address) {
        byte[] bytes = null;
        try{
            bytes = InetAddress.getByName(address).getAddress();
        }
        catch (Exception e){
            e.printStackTrace();
            return 0;
        }

        long val = 0;
        for (int i = 0; i < bytes.length; i++) {
            val <<= 8;
            val |= bytes[i] & 0xff;
        }
        return val;
    }

    public static String convertLongToAddress(long ip){
        return String.format("%d.%d.%d.%d", (ip & 0xff), (ip >> 8 & 0xff), (ip >> 16 & 0xff), (ip >> 24 & 0xff));
    }
}
