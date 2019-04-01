import java.util.Comparator;


public class SourceWithCountCompare implements Comparator<SourceWithCount> {
    @Override
    public int compare(SourceWithCount s1,SourceWithCount s2){
        return -s1.count+s2.count;
    }
}