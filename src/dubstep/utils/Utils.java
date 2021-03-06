package dubstep.utils;

import java.io.File;
import java.util.Map;

public class Utils {

    public static int getRandomNumber(int min, int max) {
        return (int) (Math.random() * ((max - min) + 1)) + min;
    }

    public static void mapPutAll(Map<String, Integer> source, Map<String, Integer> target) {
        int diff = target.size();
        for (Map.Entry<String, Integer> e : source.entrySet()) {
            target.put(e.getKey(), e.getValue() + diff);
        }
//            if(!target.containsKey(e.getKey())

    }

    public static void deleteDir(File file) {
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                deleteDir(f);
            }
        }
        file.delete();
    }
}
