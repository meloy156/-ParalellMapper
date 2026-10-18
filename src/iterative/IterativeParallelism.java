package iterative;

import info.kgeorgiy.java.advanced.iterative.NewListIP;
import info.kgeorgiy.java.advanced.mapper.ParallelMapper;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

public class IterativeParallelism implements NewListIP {

    private final ParallelMapper mapper;

    public IterativeParallelism(ParallelMapper mapper) {
        this.mapper = mapper;
    }

    public IterativeParallelism(int threads) {
        this.mapper = new ParallelMapperImpl(threads);
    }

    /**
     * Вспомогательный метод для нарезки списка и вызова маппера.
     * Вместо самих элементов мы передаем границы [start, end) каждого куска.
     */
    private <R> List<R> parallelIndexed(int threads, int size, Function<int[], R> blockTask) throws InterruptedException {
        int actualThreads = Math.min(threads, size);
        if (actualThreads <= 0) return Collections.emptyList();

        int chunkSize = (size + actualThreads - 1) / actualThreads;
        List<int[]> bounds = new ArrayList<>(actualThreads);

        for (int i = 0; i < actualThreads; i++) {
            int start = i * chunkSize;
            int end = Math.min(start + chunkSize, size);
            if (start >= end) break;
            bounds.add(new int[]{start, end});
        }

        // Маппер сам выполнит blockTask для каждого куска параллельно
        return mapper.map(t -> blockTask.apply(t), bounds);
    }

    @Override
    public <T> int argMax(int threads, List<T> values, Comparator<? super T> comparator) throws InterruptedException {
        Objects.requireNonNull(values, "values");
        Objects.requireNonNull(comparator, "comparator");

        int sizeList = values.size();
        if (sizeList == 0) throw new NoSuchElementException("List<T> is Empty");

        List<Integer> localMaxIndices = parallelIndexed(threads, sizeList, bounds -> {
            int start = bounds[0], end = bounds[1];
            int maxIndex = start;
            T maxVal = values.get(start);
            for (int j = start + 1; j < end; j++) {
                if (comparator.compare(values.get(j), maxVal) > 0) {
                    maxVal = values.get(j);
                    maxIndex = j;
                }
            }
            return maxIndex;
        });

        int globalMaxIndex = localMaxIndices.get(0);
        T globalMaxVal = values.get(globalMaxIndex);
        for (int idx : localMaxIndices) {
            if (comparator.compare(values.get(idx), globalMaxVal) > 0) {
                globalMaxVal = values.get(idx);
                globalMaxIndex = idx;
            }
        }
        return globalMaxIndex;
    }

    @Override
    public <T> int argMax(int threads, List<T> values, Comparator<? super T> comparator, int step) throws InterruptedException {
        Objects.requireNonNull(values, "values");
        Objects.requireNonNull(comparator, "comparator");

        int sizeList = values.size();
        if (sizeList == 0) throw new NoSuchElementException("List<T> is Empty");

        List<Integer> localMaxIndices = parallelIndexed(threads, sizeList, bounds -> {
            int start = ((bounds[0] + step - 1) / step) * step;
            int end = bounds[1];
            if (start >= end) return null;

            int maxIndex = start;
            T maxVal = values.get(start);
            for (int j = start + step; j < end; j += step) {
                if (comparator.compare(values.get(j), maxVal) > 0) {
                    maxVal = values.get(j);
                    maxIndex = j;
                }
            }
            return maxIndex;
        });

        int globalMaxIndex = -1;
        T globalMaxVal = null;
        for (Integer idx : localMaxIndices) {
            if (idx == null) continue;
            if (globalMaxIndex == -1 || comparator.compare(values.get(idx), globalMaxVal) > 0) {
                globalMaxVal = values.get(idx);
                globalMaxIndex = idx;
            }
        }
        if (globalMaxIndex == -1) throw new NoSuchElementException("No valid elements");
        return globalMaxIndex;
    }

    @Override
    public <T> int argMin(int threads, List<T> values, Comparator<? super T> comparator) throws InterruptedException {
        Objects.requireNonNull(values, "values");
        Objects.requireNonNull(comparator, "comparator");

        int sizeList = values.size();
        if (sizeList == 0) throw new NoSuchElementException("List<T> is Empty");

        List<Integer> localMinIndices = parallelIndexed(threads, sizeList, bounds -> {
            int start = bounds[0], end = bounds[1];
            int minIndex = start;
            T minVal = values.get(start);
            for (int j = start + 1; j < end; j++) {
                if (comparator.compare(values.get(j), minVal) < 0) {
                    minVal = values.get(j);
                    minIndex = j;
                }
            }
            return minIndex;
        });

        int globalMinIndex = localMinIndices.get(0);
        T globalMinVal = values.get(globalMinIndex);
        for (int idx : localMinIndices) {
            if (comparator.compare(values.get(idx), globalMinVal) < 0) {
                globalMinVal = values.get(idx);
                globalMinIndex = idx;
            }
        }
        return globalMinIndex;
    }

    @Override
    public <T> int argMin(int threads, List<T> values, Comparator<? super T> comparator, int step) throws InterruptedException {
        Objects.requireNonNull(values, "values");
        Objects.requireNonNull(comparator, "comparator");

        int sizeList = values.size();
        if (sizeList == 0) throw new NoSuchElementException("List<T> is Empty");

        List<Integer> localMinIndices = parallelIndexed(threads, sizeList, bounds -> {
            int start = ((bounds[0] + step - 1) / step) * step;
            int end = bounds[1];
            if (start >= end) return null;

            int minIndex = start;
            T minVal = values.get(start);
            for (int j = start + step; j < end; j += step) {
                if (comparator.compare(values.get(j), minVal) < 0) {
                    minVal = values.get(j);
                    minIndex = j;
                }
            }
            return minIndex;
        });

        int globalMinIndex = -1;
        T globalMinVal = null;
        for (Integer idx : localMinIndices) {
            if (idx == null) continue;
            if (globalMinIndex == -1 || comparator.compare(values.get(idx), globalMinVal) < 0) {
                globalMinVal = values.get(idx);
                globalMinIndex = idx;
            }
        }
        if (globalMinIndex == -1) throw new NoSuchElementException("No valid elements");
        return globalMinIndex;
    }

    @Override
    public <T> int indexOf(int threads, List<T> values, Predicate<? super T> predicate) throws InterruptedException {
        Objects.requireNonNull(values, "values");
        Objects.requireNonNull(predicate, "predicate");

        int sizeList = values.size();
        if (sizeList == 0) return -1;

        List<Integer> localIndices = parallelIndexed(threads, sizeList, bounds -> {
            for (int j = bounds[0]; j < bounds[1]; j++) {
                if (predicate.test(values.get(j))) return j;
            }
            return -1;
        });

        int globalIndex = Integer.MAX_VALUE;
        for (int idx : localIndices) {
            if (idx != -1 && idx < globalIndex) globalIndex = idx;
        }
        return globalIndex == Integer.MAX_VALUE ? -1 : globalIndex;
    }

    @Override
    public <T> int indexOf(int threads, List<T> values, Predicate<? super T> predicate, int step) throws InterruptedException {
        Objects.requireNonNull(values, "values");
        Objects.requireNonNull(predicate, "predicate");

        int sizeList = values.size();
        if (sizeList == 0) return -1;

        List<Integer> localIndices = parallelIndexed(threads, sizeList, bounds -> {
            int start = ((bounds[0] + step - 1) / step) * step;
            for (int j = start; j < bounds[1]; j += step) {
                if (predicate.test(values.get(j))) return j;
            }
            return -1;
        });

        int globalIndex = Integer.MAX_VALUE;
        for (int idx : localIndices) {
            if (idx != -1 && idx < globalIndex) globalIndex = idx;
        }
        return globalIndex == Integer.MAX_VALUE ? -1 : globalIndex;
    }

    @Override
    public <T> int lastIndexOf(int threads, List<T> values, Predicate<? super T> predicate) throws InterruptedException {
        Objects.requireNonNull(values, "values");
        Objects.requireNonNull(predicate, "predicate");

        int sizeList = values.size();
        if (sizeList == 0) return -1;

        List<Integer> localIndices = parallelIndexed(threads, sizeList, bounds -> {
            for (int j = bounds[1] - 1; j >= bounds[0]; j--) {
                if (predicate.test(values.get(j))) return j;
            }
            return -1;
        });

        int globalIndex = -1;
        for (int idx : localIndices) {
            if (idx > globalIndex) globalIndex = idx;
        }
        return globalIndex;
    }

    @Override
    public <T> int lastIndexOf(int threads, List<T> values, Predicate<? super T> predicate, int step) throws InterruptedException {
        Objects.requireNonNull(values, "values");
        Objects.requireNonNull(predicate, "predicate");

        int sizeList = values.size();
        if (sizeList == 0) return -1;

        List<Integer> localIndices = parallelIndexed(threads, sizeList, bounds -> {
            int start = ((bounds[0] + step - 1) / step) * step;
            int end = bounds[1];
            if (start >= end) return -1;

            int maxIdx = start + ((end - 1 - start) / step) * step;
            for (int j = maxIdx; j >= start; j -= step) {
                if (predicate.test(values.get(j))) return j;
            }
            return -1;
        });

        int globalIndex = -1;
        for (int idx : localIndices) {
            if (idx > globalIndex) globalIndex = idx;
        }
        return globalIndex;
    }

    @Override
    public <T> long sumIndices(int threads, List<? extends T> values, Predicate<? super T> predicate) throws InterruptedException {
        Objects.requireNonNull(values, "values");
        Objects.requireNonNull(predicate, "predicate");

        int sizeList = values.size();
        if (sizeList == 0) return -1;

        List<Long> localSums = parallelIndexed(threads, sizeList, bounds -> {
            long sum = 0;
            for (int j = bounds[0]; j < bounds[1]; j++) {
                if (predicate.test(values.get(j))) sum += j;
            }
            return sum;
        });

        long globalSum = 0;
        for (long s : localSums) globalSum += s;
        return globalSum;
    }

    @Override
    public <T> long sumIndices(int threads, List<? extends T> values, Predicate<? super T> predicate, int step) throws InterruptedException {
        Objects.requireNonNull(values, "values");
        Objects.requireNonNull(predicate, "predicate");

        int sizeList = values.size();
        if (sizeList == 0) return -1;

        List<Long> localSums = parallelIndexed(threads, sizeList, bounds -> {
            int start = ((bounds[0] + step - 1) / step) * step;
            long sum = 0;
            for (int j = start; j < bounds[1]; j += step) {
                if (predicate.test(values.get(j))) sum += j;
            }
            return sum;
        });

        long globalSum = 0;
        for (long s : localSums) globalSum += s;
        return globalSum;
    }

    @Override
    public <T> int[] indices(int threads, List<? extends T> list, Predicate<? super T> predicate) throws InterruptedException {
        Objects.requireNonNull(list, "list");
        Objects.requireNonNull(predicate, "predicate");

        int sizeList = list.size();
        if (sizeList == 0) return new int[0];

        List<int[]> localIndices = parallelIndexed(threads, sizeList, bounds -> {
            int[] matches = new int[bounds[1] - bounds[0]];
            int count = 0;
            for (int j = bounds[0]; j < bounds[1]; j++) {
                if (predicate.test(list.get(j))) matches[count++] = j;
            }
            return Arrays.copyOf(matches, count);
        });

        int total = 0;
        for (int[] arr : localIndices) total += arr.length;
        int[] globalResult = new int[total];
        int offset = 0;
        for (int[] arr : localIndices) {
            System.arraycopy(arr, 0, globalResult, offset, arr.length);
            offset += arr.length;
        }
        return globalResult;
    }

    @Override
    public <T> int[] indices(int threads, List<? extends T> list, Predicate<? super T> predicate, int step) throws InterruptedException {
        Objects.requireNonNull(list, "list");
        Objects.requireNonNull(predicate, "predicate");

        int sizeList = list.size();
        if (sizeList == 0) return new int[0];

        List<int[]> localIndices = parallelIndexed(threads, sizeList, bounds -> {
            int start = ((bounds[0] + step - 1) / step) * step;
            int[] matches = new int[bounds[1] - bounds[0]];
            int count = 0;
            for (int j = start; j < bounds[1]; j += step) {
                if (predicate.test(list.get(j))) matches[count++] = j;
            }
            return Arrays.copyOf(matches, count);
        });

        int total = 0;
        for (int[] arr : localIndices) total += arr.length;
        int[] globalResult = new int[total];
        int offset = 0;
        for (int[] arr : localIndices) {
            System.arraycopy(arr, 0, globalResult, offset, arr.length);
            offset += arr.length;
        }
        return globalResult;
    }

    @Override
    public <T> List<T> filter(int threads, List<? extends T> list, Predicate<? super T> predicate) throws InterruptedException {
        Objects.requireNonNull(list, "list");
        Objects.requireNonNull(predicate, "predicate");

        int sizeList = list.size();
        if (sizeList == 0) return new ArrayList<>();

        List<List<T>> localLists = parallelIndexed(threads, sizeList, bounds -> {
            List<T> matches = new ArrayList<>();
            for (int j = bounds[0]; j < bounds[1]; j++) {
                T val = list.get(j);
                if (predicate.test(val)) matches.add(val);
            }
            return matches;
        });

        List<T> globalResult = new ArrayList<>();
        for (List<T> sub : localLists) globalResult.addAll(sub);
        return globalResult;
    }

    @Override
    public <T> List<T> filter(int threads, List<? extends T> list, Predicate<? super T> predicate, int step) throws InterruptedException {
        Objects.requireNonNull(list, "list");
        Objects.requireNonNull(predicate, "predicate");

        int sizeList = list.size();
        if (sizeList == 0) return new ArrayList<>();

        List<List<T>> localLists = parallelIndexed(threads, sizeList, bounds -> {
            int start = ((bounds[0] + step - 1) / step) * step;
            List<T> matches = new ArrayList<>();
            for (int j = start; j < bounds[1]; j += step) {
                T val = list.get(j);
                if (predicate.test(val)) matches.add(val);
            }
            return matches;
        });

        List<T> globalResult = new ArrayList<>();
        for (List<T> sub : localLists) globalResult.addAll(sub);
        return globalResult;
    }

    @Override
    public <T, U> List<U> map(int threads, List<? extends T> list, Function<? super T, ? extends U> function) throws InterruptedException {
        Objects.requireNonNull(list, "list");
        Objects.requireNonNull(function, "function");

        int sizeList = list.size();
        if (sizeList == 0) return new ArrayList<>();

        List<List<U>> localLists = parallelIndexed(threads, sizeList, bounds -> {
            List<U> matches = new ArrayList<>(bounds[1] - bounds[0]);
            for (int j = bounds[0]; j < bounds[1]; j++) {
                matches.add(function.apply(list.get(j)));
            }
            return matches;
        });

        List<U> globalResult = new ArrayList<>(sizeList);
        for (List<U> sub : localLists) globalResult.addAll(sub);
        return globalResult;
    }

    @Override
    public <T, R> List<R> map(int threads, List<? extends T> list, Function<? super T, ? extends R> function, int step) throws InterruptedException {
        Objects.requireNonNull(list, "list");
        Objects.requireNonNull(function, "function");

        int sizeList = list.size();
        if (sizeList == 0) return new ArrayList<>();

        List<List<R>> localLists = parallelIndexed(threads, sizeList, bounds -> {
            int start = ((bounds[0] + step - 1) / step) * step;
            List<R> matches = new ArrayList<>();
            for (int j = start; j < bounds[1]; j += step) {
                matches.add(function.apply(list.get(j)));
            }
            return matches;
        });

        List<R> globalResult = new ArrayList<>();
        for (List<R> sub : localLists) globalResult.addAll(sub);
        return globalResult;
    }
}