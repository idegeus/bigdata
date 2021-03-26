package nl.uva.bigdata.hadoop.assignment3;


import java.util.*;
import java.util.stream.Collectors;

interface MapFunction<K1 extends Comparable<K1>, V1, K2 extends Comparable<K2>, V2> {
    Collection<Record<K2, V2>> map(Record<K1, V1> inputRecord);
}

interface ReduceFunction<K2 extends Comparable<K2>, V2, V3> {
    Collection<Record<K2, V3>> reduce(K2 key, Collection<V2> valueGroup);
}

public class MapReduceEngine<K1 extends Comparable<K1>, V1, K2 extends Comparable<K2>, V2, V3> {

    private Collection<Record<K2, V2>> runMapPhase(
            Collection<Record<K1, V1>> inputRecords,
            MapFunction<K1, V1, K2, V2> map
    ) {
        Collection<Record<K2, V2>> mapOutputs = new ArrayList<>();

        for (Record<K1, V1> record : inputRecords) {
            for (Record<K2, V2> item : map.map(record)) {
                mapOutputs.add(item);
            }     
        }
        return mapOutputs;
    }

    private Collection<Collection<Record<K2, V2>>> partitionMapOutputs(
            Collection<Record<K2, V2>> mapOutputs,
            int numPartitions) {
        
        List<Record<K2, V2>> mapList = new ArrayList<>(mapOutputs);
        Collection<Collection<Record<K2, V2>>> partitionedMapOutput = new ArrayList<>();
        int batchSize = (int) Math.ceil(mapOutputs.size() / (double) numPartitions);

        Collections.sort(mapList, new Comparator<Record<K2, V2>>() {
            public int compare(Record<K2, V2> o1, Record<K2, V2> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        });
        
        int start = 0;
        int end = batchSize;
        String lastWord = mapList.get(end-1).getKey().toString();
        String nextWord = mapList.get(end).getKey().toString();

        for (int i=0; i<numPartitions; i++) {
            while (lastWord.charAt(0) == nextWord.charAt(0) && end < mapList.size() - 1) {
                end += 1;
                lastWord = mapList.get(end-1).getKey().toString();
                nextWord = mapList.get(end).getKey().toString();
            }
            partitionedMapOutput.add(mapList.subList(start, Math.min(mapList.size(), end)));
            start = end;
            end += batchSize;
        }
        return partitionedMapOutput;
    }

    private Map<K2, Collection<V2>> groupReducerInputPartition(Collection<Record<K2, V2>> reducerInputPartition) {

        Map<K2, Collection<V2>> map = new HashMap<>();

        for (Record<K2, V2> output : reducerInputPartition) {
            if (map.containsKey(output.getKey())) {
                map.get(output.getKey()).add(output.getValue());
            } else {
                map.put(output.getKey(), new ArrayList<V2>());
                map.get(output.getKey()).add(output.getValue());
            }
        }
        return map;
    }

    private Collection<Record<K2, V3>> runReducePhaseOnPartition(
            Map<K2, Collection<V2>> reducerInputs,
            ReduceFunction<K2, V2, V3> reduce
    ) {
        Collection<Record<K2, V3>> reducerOutputs = new ArrayList<>();

        for (K2 key : reducerInputs.keySet()) {
            for (Record<K2, V3> item :reduce.reduce(key, reducerInputs.get(key))) {
                reducerOutputs.add(item);
            }
        }
        return reducerOutputs;
    }

    public Collection<Record<K2, V3>> compute(
        Collection<Record<K1, V1>> inputRecords,
        MapFunction<K1, V1, K2, V2> map,
        ReduceFunction<K2, V2, V3> reduce,
        int numPartitionsDuringShuffle
    ) {

        Collection<Record<K2, V2>> mapOutputs = runMapPhase(inputRecords, map);
        Collection<Collection<Record<K2, V2>>> partitionedMapOutput =
                partitionMapOutputs(mapOutputs, numPartitionsDuringShuffle);

        assert numPartitionsDuringShuffle == partitionedMapOutput.size();

        List<Record<K2, V3>> outputs = new ArrayList<>();

        for (Collection<Record<K2, V2>> partition : partitionedMapOutput) {
            Map<K2, Collection<V2>> reducerInputs = groupReducerInputPartition(partition);

            outputs.addAll(runReducePhaseOnPartition(reducerInputs, reduce));
        }


        return outputs;
    }

}
