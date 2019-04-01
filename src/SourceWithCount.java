import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;

public class SourceWithCount{
    long sourceId;
    int count;

    public SourceWithCount(long sourceId, int count){
        this.sourceId = sourceId;
        this.count = count;
    }

//    public int compareTo(SourceWithCount that){
//        return Integer.compare(this.count, that.count);
//    }


    public static HashMap<Long, HashSet<Long>> getSpreaders(ArrayList<Packet>inputPackets){
        HashMap<Long, HashSet<Long>> spreaders = new HashMap<Long, HashSet<Long>>();
        for (Packet p: inputPackets){
            if (!spreaders.containsKey(p.getSrcIp())){
                spreaders.put(p.getSrcIp(), new HashSet<Long>());
            }
            spreaders.get(p.getSrcIp()).add(p.getDestIp());
        }
        return spreaders;
    }


    public static ArrayList<SourceWithCount> topKSuperspreader(HashMap<Long, HashSet<Long>> spreaders, int K){
        ArrayList<SourceWithCount> topk = new ArrayList<SourceWithCount>();
        Converter convert = new Converter();

        for (Long src_ip : spreaders.keySet()){
            if (spreaders.get(src_ip).size() >= K){
                topk.add(new SourceWithCount(src_ip, spreaders.get(src_ip).size()));
            }
        }
        return topk;
    }

}

