public class TableEntry {
    private long sourceIP;
    private boolean[] bitmap;
    private int counter;
    private int bitmaplen;
    private long timestamp;


    // constructor
    public TableEntry(long sourceIP,int bitmaplen,long timestamp) {
        this.sourceIP = sourceIP;
        this.bitmap = new boolean[bitmaplen];
        this.bitmaplen = bitmaplen;
        this.counter = 0;
        this.timestamp = timestamp;
    }

    /* Getters for the internal variables */
    public long getSourceIP() {
        return sourceIP;
    }

    public long getTimestamp(){
        return timestamp;
    }
    public boolean[] getBitmap() {
        return bitmap;
    }

    public int getCounter() {
        return counter;
    }

    /* Setters for the bitmap and counter */
    public void setSourceIP(long sourceIP){
        this.sourceIP = sourceIP;
    }
    public void setBitmap(boolean[] bitmap) {
        this.bitmap = bitmap.clone();
    }

    public void setTimestamp(long timestamp){
        this.timestamp=timestamp;
    }

    public void setCounter(int counter) {
        this.counter = counter;
    }

    private void increCounter(){
        this.counter += 1;
    }

//    public void bitmapTocounter(){
//        this.counter = 0;
//        for(int k =0;k<bitmaplen;k++){
//            if(this.bitmap[k]){
//                this.counter ++;
//            }
//        }
//    }

    public void BloomfilterSet(int[] index){
        boolean flag = false;
        for(int i = 0; i<index.length; i++){
            if(!bitmap[index[i]]){
                bitmap[index[i]] = true;
                flag = true;
            }
        }
        if(flag){
            increCounter();
        }
    }
}
