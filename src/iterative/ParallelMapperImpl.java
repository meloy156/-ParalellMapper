package iterative;

import info.kgeorgiy.java.advanced.mapper.ParallelMapper;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;


public class ParallelMapperImpl implements ParallelMapper {

    private final Thread[] workerThread;
    private final int countThread;
    private volatile boolean flagClose = false;
    private final SimpleQueue<Runnable> tasks;
    private Counter counter;


    public ParallelMapperImpl(int threads) {
        if (threads <= 0) {
            throw new IllegalArgumentException("threads must be > 0");
        }


        countThread = threads;
        workerThread = new Thread[threads];
        tasks = new SimpleQueue<>();

        for (int k = 0; k < threads; k++) {
            workerThread[k] = new Thread(this::workerLoop, "worker- " + k);
            workerThread[k].start();
        }
    }


    private void workerLoop() {
        while (true) {
            Runnable oneTask;
            try {
                oneTask = tasks.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            if (oneTask == null) return;

            try {
                oneTask.run();
            } catch (Throwable t) {
                return;
            }
        }
    }

    @Override
    public <T, R> List<R> map(Function<? super T, ? extends R> function, List<? extends T> list) throws InterruptedException {
        if (list.isEmpty()) {
            throw new IllegalArgumentException("list must be >0 elements");
        }

        int size = list.size();
        Object[] resultList = new Object[size];
        Throwable[] errors = new Throwable[size];


        this.counter = new Counter(size);

        for (int i = 0; i < size; i++) {
            final int idx = i;
            final T element = list.get(i);

            Runnable task = () -> {
                try {
                    resultList[idx] = function.apply(element);
                } catch (Throwable t) {
                    errors[idx] = t;
                } finally {
                    this.counter.decrement();
                }
            };

            tasks.put(task);
        }



        counter.wait();




        for (Throwable t : errors) {
            switch (t) {
                case null -> {
                    continue;
                }
                case RuntimeException re -> throw re;
                case Error er -> throw er;
                default -> {
                }
            }
            throw new RuntimeException(t);
        }

        @SuppressWarnings("unchecked")
        List<R> list1 = Arrays.stream(resultList).map(o -> (R) o).toList();
        return list1;

    }

    @Override
    public void close() {
        if (flagClose) return;
        flagClose = true;

        tasks.close();

        for (Thread t : workerThread) {
            try {
                t.join();
            } catch (InterruptedException i) {
                Thread.currentThread().interrupt();
            }
        }
    }
}