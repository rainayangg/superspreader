import java.util.*;
import java.math.*;
/* abstraction for Packets in a network that helps retrieve 
   the key fields from the packet*/
public class Packet {
    private long srcip;
    private long dstip;
    int bitmaplen = 128;
    long timestamp;

    //metadata
    public boolean recirculated_min=false; // recirculate to replace the minimum value;
    public int min_stage;// the stage containing the smallest count;
    public int carry_min = Integer.MAX_VALUE;// smallest count experienced;
    public long carry_SrcIp;
    public long carry_time = Long.MAX_VALUE;
    public boolean[] bitmap;

    public boolean recirculated_dup=false; // recirculate to delete duplicated entry;
//    private String srcPort;
//    private String dstPort;
//    private String protocol;

    public Packet(long srcip, long dstip, long timestamp) {
        this.srcip = srcip;
        this.dstip = dstip;
        this.bitmap = new boolean[bitmaplen];
        this.timestamp =timestamp;
//        this.srcPort = srcPort;
//        this.dstPort = dstPort;
//        this.protocol = protocol;
    }

    // get source ip of a packet
    public long getSrcIp(){
        return this.srcip;
    }

    public long getTimestamp(){
        return  this.timestamp;
    }
    // get destination ip of a packet
    public long getDestIp(){
        return this.dstip;
    }
}