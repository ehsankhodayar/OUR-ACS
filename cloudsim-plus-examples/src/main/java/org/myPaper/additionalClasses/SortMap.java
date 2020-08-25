package org.myPaper.additionalClasses;

import java.util.*;

public class SortMap {
    public static <k, v extends Comparable<? super v>> Map<k, v> sortByValue(Map<k, v> unsortedMap, boolean ascendingOrder){
        //this sort method support generics

        // 1. Convert Map to List of Map
        List<Map.Entry<k, v>> list = new ArrayList<>(unsortedMap.entrySet());

        Collections.shuffle(list);

        // 2. Sort list with Collections.sort(), provide a custom Comparator
        //    Try switch the o1 o2 position for a different order
        list.sort(Map.Entry.comparingByValue());

        // 3. Loop the sorted list and put it into a new insertion order Map LinkedHashMap
        Map<k, v> sortedMap= new LinkedHashMap<>();

        if (ascendingOrder) {
            list.forEach(entry->{
                sortedMap.put(entry.getKey(), entry.getValue());
            });
        }else {
            Collections.reverse(list);
            list.forEach(entry->{
                sortedMap.put(entry.getKey(), entry.getValue());
            });
        }

        return sortedMap;
    }
}
