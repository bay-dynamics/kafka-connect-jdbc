package com.baydynamics.riskfabric.connect.jdbc.util;

import org.apache.commons.collections4.queue.CircularFifoQueue;

import java.util.HashMap;

public class CircularFifoHashSet<E> {
    private CircularFifoQueue<E> queue;
    private HashMap<E, Integer> hashMap;
    private int lastElementPosition = -1;

    public CircularFifoHashSet(int size) {
        if (size <= 0 )
            throw new IllegalArgumentException("size must be > 0");
        queue = new CircularFifoQueue<E>(size);
        hashMap = new HashMap<E, Integer>(); // Element, Position in queue
    }

    public boolean contains(E element) {
        return hashMap.containsKey(element);
    }

    public void appendWithNoCheck(E element) {
        queue.add(element);
        lastElementPosition++;
        hashMap.put(element, lastElementPosition);
    }

    public boolean addIfAbsent(E element) {
        // present?
        if (hashMap.containsKey(element)) {
            Integer position = hashMap.get(element);
            if (position < lastElementPosition) {
                //move last seen element to top of queue
                E lastSeenElement = queue.get(position);
                queue.remove(lastSeenElement);
                queue.add(lastSeenElement);
                // update position
                hashMap.replace(element, lastElementPosition);
            }
            //else nothing to move
            return false;
        }
        else if (queue.isAtFullCapacity()) {
            // remove eldest
            E eldest = queue.get(0);
            queue.remove(eldest);
            lastElementPosition--;
            hashMap.remove(eldest);
        }
        // add absent
        queue.add(element);
        lastElementPosition++;
        hashMap.put(element, lastElementPosition);
        return true;
    }

    public boolean isAtFullCapacity() {
        return lastElementPosition >= queue.size()-1;
    }
}