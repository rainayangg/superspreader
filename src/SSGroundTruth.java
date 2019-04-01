import java.util.*;
import java.io.File;
import java.io.PrintWriter;
import java.io.FileNotFoundException;

/* Simulation of Ground Truth for Superspreader Detection */
public class SSGroundTruth {
    // read packet input from source file
    public static ArrayList<Packet> read_csv_file(String filepath){
        ArrayList<Packet> inputPackets = new ArrayList<Packet>();
        Converter convert = new Converter();
        int valid_packet = 0;
        try {
            Scanner scanner = new Scanner(new File(filepath));
            while (scanner.hasNext()){
                String packet_info = scanner.nextLine();
                 packet_info = packet_info.trim();
                String[] field = packet_info.split("\\s+");
//                if (field.length < 6){
//                    continue;
//                }
//                if(field[3].equals("TCP") || field[3].equals("UDP")) {
//                    long srcip = convert.convertAddressToLong(field[0]);
//                    long dstip = convert.convertAddressToLong(field[2]);
//                    inputPackets.add(new Packet(srcip, dstip));
//                    valid_packet += 1;
//                }
            }
        }
        catch (FileNotFoundException e){
            e.printStackTrace();
        }
//        System.out.println(valid_packet);
        return inputPackets;
    }

    // construct spreader hashtable from input array
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

    // get top K superspreader
    public static ArrayList<Long> topKSuperspreader(HashMap<Long, HashSet<Long>> spreaders, int K, String filename){
        ArrayList<Long> topk = new ArrayList<Long>();
        Converter convert = new Converter();
        try{
            PrintWriter writer = new PrintWriter(filename);
            for (Long src_ip : spreaders.keySet()){
                if (spreaders.get(src_ip).size() >= K){
                    topk.add(src_ip);
                    writer.println((src_ip) + "," + spreaders.get(src_ip).size());
                }
            }
        }
        catch (FileNotFoundException fe){
            fe.printStackTrace();
        }
        return topk;
    }

    public static void main(String[] args){
        File dir = new File("/Users/weifenghu/Desktop/MSCS/W19/CSE222A/superspreader/src/data_tmp");
        File[] data = dir.listFiles();
        if (data != null) {
            // loop through all files for data
            for (File f : data) {
//                HashMap<Long, HashSet<Long>> spreaders = new HashMap<Long, HashSet<Long>>();
                ArrayList<Packet> input = read_csv_file(f.getAbsolutePath());
                HashMap<Long, HashSet<Long>> spreaders = getSpreaders(input);
                ArrayList<Long> topk = topKSuperspreader(spreaders, 1, f.getName());
            }
        }
        System.out.print("Done");
    }
}
