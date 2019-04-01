// hash function used to map an ip address to a certain index in table or in bitmap
import java.math.BigInteger;
import java.lang.Math.*;

public class HashFunction {
    int NumHashFunction;
    private int WholeTableSize;
    private int StageTableSize;
    private int[] hashBigA;
    private int[] hashBigB;
    private int bigP;

    public HashFunction(int d,int wholetablesize){
        this.NumHashFunction = d;
        this.WholeTableSize = wholetablesize;
        this.StageTableSize = wholetablesize/d;
        hashBigA = new int[NumHashFunction];
        hashBigB = new int[NumHashFunction];
        bigP = new Integer(Long.toString(3991));
        for (int i = 0; i < NumHashFunction; i++){
            hashBigA[i] = new Integer(Integer.toString((int) (Math.random()* 3991)));
            hashBigB[i] = new Integer(Integer.toString((int) (Math.random()* 3991)));
        }

    }

    public int[] index(long IpAddress){
        int[] indexs = new int[NumHashFunction];
        for (int i =0; i<NumHashFunction; i++){
            long tmp = (long)hashBigA[i]*IpAddress+hashBigB[i];
            tmp = tmp % bigP;
            tmp = tmp % StageTableSize;
            tmp = tmp + StageTableSize*i;
            indexs[i] = (int)tmp;
        }
        return indexs;

    }

    // multiplication hash function
    public static int Hash_Bitmap(long ipaddress, int tablesize){

        final double A = 0.6180339887;
        double kA = A*ipaddress - Math.floor(A*ipaddress);
        int index = (int)Math.floor(kA*tablesize);
        return index;

    }


}
