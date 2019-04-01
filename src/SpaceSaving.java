import java.util.*;
import java.io.File;
import java.io.FileNotFoundException;

public class SpaceSaving {

    public static void main(String[] args) {



        //parameters used in this experiment.
        int[] Wholetablesize = {120,160,200,240,280,320,360,400,440,480,520,560,600,640,680,720};

        int[] D = {120,160,200,240,280,320,360,400,440,480,520,560,600,640,680,720};
        int[] counter = {20,40,60,80,100};
        int bitmaplen = 2048;
        int[] Recirculate_delay = {1};
        int NumHash_bf = 12;


        int d_index = 0;
        for (int wholetablesize : Wholetablesize) {
                int d = D[d_index];
                d_index++;
                for(int recirculate_delay : Recirculate_delay) {
                    //start_time records the time when main function is called;
                    long start_time = System.currentTimeMillis();

                    //inputPacketStream records the source ip and dest ip addresses of the incoming packets in sequence;
                    ArrayList<Packet> inputPacketStream = new ArrayList<Packet>();

                    //SourceCount contains the number of different destinations for each source ip;
                    ArrayList<SourceWithCount> SourceCount = new ArrayList<SourceWithCount>();

                    String file_path = args[0];
                    File folder = new File(file_path);
                    File[] listOfFiles = folder.listFiles();
                    Arrays.sort(listOfFiles);
                    for (int i = 0; i < listOfFiles.length; i++) {
                        inputPacketStream = DataParser.parsedata_5("/Users/yangrui/data-130000/" + listOfFiles[i].getName(), inputPacketStream);
                    }

                //control number
                ArrayList<Packet> input = new ArrayList<Packet>(inputPacketStream.subList(0,500000));
                inputPacketStream = input;

                    // ground truth
                    // get the list of Source Ip and its number of destinations. K indicates the number of its destinations are larger than K;
                    HashMap<Long, HashSet<Long>> spreaders = SourceWithCount.getSpreaders(inputPacketStream);

                    SourceCount = SourceWithCount.topKSuperspreader(spreaders, 1);
                    Collections.sort(SourceCount, new SourceWithCountCompare());

                    ArrayList<SourceWithCount> SourceCounttemp;
                    SourceCounttemp = new ArrayList<SourceWithCount>(SourceCount.subList(0, wholetablesize));
                    SourceCount = SourceCounttemp;


                    // initialize the whole table in switch (supposed to be divided into d tables in reality)
                    ArrayList<TableEntry> table = new ArrayList<TableEntry>(wholetablesize);
                    for (int i = 0; i < wholetablesize; i++) {
                        table.add(null);
                    }

                    // build hash function for stage table and bloom filter.
                    HashFunction hash = new HashFunction(d, wholetablesize);
                    HashFunction_BloomFilter hash_bloomfilter = new HashFunction_BloomFilter(bitmaplen, NumHash_bf);

                    // loop through all incoming packets
                    while (inputPacketStream.size() != 0) {

//                    // print every 10,000 packets
//                    if (inputPacketStream.size() % 100000 == 0) {
//                        for (int l = 0; l < table.size(); l++) {
//                            if (table.get(l) != null) {
//                                System.out.println("IP: " + table.get(l).getSourceIP() + " counter: " + table.get(l).getCounter());
//                            } else {
//                                System.out.println("null");
//                            }
//                        }
//                        System.out.println();
//
//                    }
                        Packet incoming = inputPacketStream.get(0);
                        // check recirculate bit in packet's metadata

                        // store source ip address, dest ip address and timestamp of the incoming packet.
                        long SrcIp = incoming.getSrcIp();
                        long DestIp = incoming.getDestIp();
                        long timestamp = incoming.getTimestamp();

                        // get hashed position
                        int[] position = hash.index(SrcIp);

                        // recirculated packet to replace the smallest one it has found.
                        if (incoming.recirculated_min) {
                            int stage_number = incoming.min_stage;
                            int position_sub = position[stage_number];
//                TableEntry tmp = table.get(position_sub);
//                tmp.setSourceIP(incoming.getSrcIp());

                            // create new tableentry for recirculated packet.
                            TableEntry tmp = new TableEntry(SrcIp, bitmaplen, timestamp);

//                            //get the bitmap and counter of the older entry
//                            tmp.setBitmap(table.get(position_sub).getBitmap().clone());
                            tmp.setCounter(table.get(position_sub).getCounter());

                            //update the bitmap and counter
                            int[] index = hash_bloomfilter.index(DestIp);
                            tmp.BloomfilterSet(index);
                            table.set(position_sub, tmp);
                            inputPacketStream.remove(0);
                            //test
//                System.out.println("recirculation for substitution");
                            continue;
                        }

//                        // recirculated packet to refresh the old entry.
//                        if (incoming.recirculated_dup) {
//                            int stage_number = incoming.min_stage;
//                            int position_sub = position[stage_number];
//
//                            if (table.get(position_sub) != null && table.get(position_sub).getSourceIP() == SrcIp) {
//                                TableEntry tmp = new TableEntry(incoming.carry_SrcIp, bitmaplen, incoming.getTimestamp());
//                                tmp.setBitmap(incoming.bitmap);
//                                tmp.setCounter(incoming.carry_min);
////                            System.out.println("recirculation for duplication and delete it successfully");
//                            }
////                        boolean[] bitmap_tmp = tmp.getBitmap();
////
////                        for (int k = 0; k < bitmaplen; k++) {
////                            bitmap_tmp[k] = bitmap_tmp[k] || incoming.bitmap[k];
////                        }
////                        tmp.setBitmap(incoming.bitmap);
////                        tmp.setCounter(incoming.carry_min);
////                        table.set(position[i], tmp);
//
////                System.out.println("recirculation for duplication");
//
//
//                            inputPacketStream.remove(0);
//                            //test
//                            continue;
//                        }

                        // flag to indicate whether entry having same source ip has been found;
                        boolean matched = false;
                        for (int j = 0; j < d; j++) {
                            if (!matched) {
                                if (table.get(position[j]) == null) {
                                    TableEntry entry = new TableEntry(SrcIp, bitmaplen, timestamp);
                                    // set bitmap and counter if necessary
                                    int[] index = hash_bloomfilter.index(DestIp);
                                    entry.BloomfilterSet(index);
                                    // insert into the table
                                    table.set(position[j], entry);
                                    matched = true;
                                } else if (table.get(position[j]).getSourceIP() == SrcIp) {
                                    int[] index = hash_bloomfilter.index(DestIp);
                                    table.get(position[j]).BloomfilterSet(index);
                                    table.get(position[j]).setTimestamp(timestamp);
                                    matched = true;
                                } else if (table.get(position[j]).getSourceIP() != SrcIp) {

                                    if (incoming.carry_min > table.get(position[j]).getCounter()) {
                                        incoming.carry_min = table.get(position[j]).getCounter();
                                        incoming.carry_time = table.get(position[j]).getTimestamp();

                                        incoming.min_stage = j;
                                    }
                                        if (incoming.carry_min == table.get(position[j]).getCounter() && incoming.carry_time > table.get(position[j]).getTimestamp()) {
                                            incoming.carry_min = table.get(position[j]).getCounter();
                                            incoming.carry_time = table.get(position[j]).getTimestamp();
                                            incoming.min_stage = j;
                                        }

                                }
                            } else if (matched) {
                                if (table.get(position[j]) == null) {
                                    TableEntry tmp = new TableEntry(incoming.carry_SrcIp, bitmaplen, incoming.getTimestamp());
                                    tmp.setBitmap(incoming.bitmap);
                                    tmp.setCounter(incoming.carry_min);
                                } else if (table.get(position[j]).getSourceIP() == SrcIp) {
                                    incoming.recirculated_dup = true;
                                    int[] index = hash_bloomfilter.index(DestIp);
                                    table.get(position[j]).BloomfilterSet(index);
                                    table.get(position[j]).setTimestamp(timestamp);
//                        incoming.carry_min = table.get(position[j]).getCounter();
//                        incoming.bitmap = table.get(position[j]).getBitmap().clone();
//                        table.set(position[j], null);
                                } else if (table.get(position[j]).getSourceIP() != SrcIp) {
                                    //keep walking;
                                }
                            }
                        }


                        if (!matched) {
                            // generate an integer in [0,carry_min-1];

                            incoming.recirculated_min = true;
                            // add packet to a particular position in the input packet stream
                            // to simulate the recircualte delay.
                            if (inputPacketStream.size() < recirculate_delay) {
                                inputPacketStream.add(inputPacketStream.size() - 1, incoming);
                            } else {
                                inputPacketStream.add(recirculate_delay - 1, incoming);
                            }

                        }


                        inputPacketStream.remove(0);
                    }



                    System.out.println("table size= " + wholetablesize + " d= " + d + " delay= "+recirculate_delay);
                for (int l = 0; l < table.size(); l++) {
                    System.out.println("IP: " + table.get(l).getSourceIP() + " counter: " + table.get(l).getCounter());
                }

                    ArrayList<SourceWithCount> output = new ArrayList<SourceWithCount>(wholetablesize);
                    for (int i = 0; i < wholetablesize; i++) {
                        if (table.get(i) != null) {
                            long ip = table.get(i).getSourceIP();
                            int count = table.get(i).getCounter();
                            SourceWithCount tmp = new SourceWithCount(ip, count);
                            output.add(tmp);
                        }
                    }

                    Evaluate evaluate = new Evaluate(SourceCount, output);
                    System.out.println("accuracy= " + evaluate.accuracy());
                    for(int c=0;c<counter.length;c++){
                        System.out.println("False Negative for count=" + counter[c]+ " : " + evaluate.FalseNegativeWithCount(counter[c]));

                    }
                    System.out.println("false negative = " + (float) evaluate.fn / (float) (evaluate.fn + evaluate.tp));
                    long end_time = System.currentTimeMillis();
                    System.out.println("run time = " + ((float) end_time - (float) start_time));

                }
            }
        }



}





