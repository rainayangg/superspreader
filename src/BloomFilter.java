public class BloomFilter {
    public boolean[] map;
    public int HashFuctionNum;
    private int[] hashBigA;
    private int[] hashBigB;
    private int bigP;

    public  BloomFilter(int mapsize, int hashFuctionNum){
        this.map = new boolean[mapsize];
        for(int i =0; i<mapsize;i++){
            map[i] =false;
        }
        this.HashFuctionNum = hashFuctionNum;
        hashBigA = new int[HashFuctionNum];
        hashBigB = new int[HashFuctionNum];
        bigP = new Integer(Long.toString(4991));
        for (int i = 0; i < HashFuctionNum; i++){
            hashBigA[i] = new Integer(Integer.toString((int) (Math.random()* 3991)));
            hashBigB[i] = new Integer(Integer.toString((int) (Math.random()* 3991)));
        }

    }

    public void add(long DestIp){
        for (int i =0; i<HashFuctionNum; i++){
            long tmp = (long)hashBigA[i]*DestIp+hashBigB[i];
            tmp = tmp % bigP;
            tmp = tmp % map.length;
            this.map[(int)tmp] = true;
        }
    }

    public boolean check(long DestIp){
        boolean exist = true;
        for (int i =0; i<HashFuctionNum; i++){
            long tmp = (long)hashBigA[i]*DestIp+hashBigB[i];
            tmp = tmp % bigP;
            tmp = tmp % map.length;
            if (!this.map[(int)tmp]){
                //this dest ip has not shown before
                exist = false;
                break;
            }
        }
        if(!exist){
            add(DestIp);
            return false; // count ++
        }
        else{
            return true; // count keep same;
        }


    }
}
