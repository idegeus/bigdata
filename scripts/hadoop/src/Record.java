package nl.uva.bigdata.hadoop.assignment3;

public class Record<K  extends Comparable<K>, V> {
    K key;
    V value;

    public Record(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }
    
    public int compareTo(Record<K, V> other) {
        return this.getKey().compareTo(other.getKey());
    }
}
