package iterative;

import java.util.ArrayDeque;
import java.util.Deque;

public class SimpleQueue<T>  {
    private final Deque<T> items = new ArrayDeque<>();
    private boolean closed = false;


    public synchronized void put(T item) {
        items.addLast(item);
        notifyAll();
    }

    public synchronized T take() throws InterruptedException {
        while (items.isEmpty() && !closed) {
            wait();
        }

        return items.pollFirst();
    }

    public synchronized void  close() {
        closed = true;
        notifyAll();
    }
}
