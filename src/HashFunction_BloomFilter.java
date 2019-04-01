public class HashFunction_BloomFilter {
    public int mapsize;
    public int HashFuctionNum;
    private int[] hashBigA;
    private int[] hashBigB;
    private int bigP;

    public  HashFunction_BloomFilter(int mapsize, int hashFuctionNum){
        this.mapsize = mapsize;
        this.HashFuctionNum = hashFuctionNum;
        hashBigA = new int[HashFuctionNum];
        hashBigB = new int[HashFuctionNum];
        bigP = new Integer(Long.toString(49991));
        for (int i = 0; i < HashFuctionNum; i++){
            hashBigA[i] = new Integer(Integer.toString((int) (Math.random()* 49991)));
            hashBigB[i] = new Integer(Integer.toString((int) (Math.random()* 49991)));
        }

    }

    public int[] index(long DestIp){
        int[] indexs = new int[mapsize];
        for (int i =0; i<HashFuctionNum; i++){
            long tmp = (long)hashBigA[i]*DestIp+hashBigB[i];
            tmp = tmp % bigP;
            tmp = tmp % mapsize;
            indexs[i] = (int)tmp;
        }
        return indexs;
    }
}


