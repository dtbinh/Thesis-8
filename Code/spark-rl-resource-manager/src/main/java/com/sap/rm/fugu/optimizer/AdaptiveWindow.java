package com.sap.rm.fugu.optimizer;

import static java.lang.Math.*;

class AdaptiveWindow {
    static final int MAXIMUM_BUCKETS = 5;

    private static final int MINIMUM_WINDOW_WIDTH = 8;
    private static final int MINIMUM_SUBWINDOW_WIDTH = 3;

    private final double delta;
    private final int clock;

    private final BucketList bucketList = new BucketList();

    private int clockTime = 0;
    private int lastBucketIndex = 0;

    private int width = 0;
    private double elementSum = 0.0;
    private double elementVariance = 0.0;

    AdaptiveWindow(double delta, int clock) {
        this.delta = delta;
        this.clock = clock;
    }

    int getWidth() {
        return width;
    }

    void add(double value) {
        insertElement(value);
        if (++clockTime % clock == 0 && getWidth() >= MINIMUM_WINDOW_WIDTH) {
            adapt();
        }
    }

    private void insertElement(double element) {
        width++;

        bucketList.head().insertBucket(element, 0.0);

        if (width > 1) {
            elementVariance += (width - 1) * pow(element - elementSum / (width - 1), 2) / width;
        }
        elementSum += element;

        compressBuckets();
    }

    private void adapt() {
        boolean blnReduceWidth = true;

        while (blnReduceWidth) {
            blnReduceWidth = false;

            int n0 = 0;
            int n1 = width;

            double u0 = 0.0;
            double u1 = elementSum;

            int i = lastBucketIndex;
            blnExit:
            for (Bucket item = bucketList.tail(); item != null; item = item.getPrevious()) {
                for (int k = 0; k <= (item.bucketSizeRow - 1); k++) {
                    n0 += bucketSize(i);
                    n1 -= bucketSize(i);
                    u0 += item.getTotal(k);
                    u1 -= item.getTotal(k);

                    if (i == 0 && k == item.bucketSizeRow - 1) {
                        break blnExit;
                    }

                    if (shouldAdapt(n0, n1, u0 / n0 - u1 / n1)) {
                        blnReduceWidth = true;
                        if (getWidth() > 0) {
                            deleteElement();
                            break blnExit;
                        }
                    }
                }
                i--;
            }
        }
    }

    private int bucketSize(int index) {
        return 1 << index;
    }

    private void compressBuckets() {
        int i = 0;
        for (Bucket item = bucketList.head(); item != null; item = item.next()) {
            if (item.bucketSizeRow == MAXIMUM_BUCKETS + 1) {
                Bucket nextNode = item.next();

                if (nextNode == null) {
                    bucketList.addToTail();
                    nextNode = item.next();
                    lastBucketIndex++;
                }

                int n1 = bucketSize(i);
                int n2 = bucketSize(i);
                double u1 = item.getTotal(0) / n1;
                double u2 = item.getTotal(1) / n2;
                double incVariance = n1 * n2 * (u1 - u2) * (u1 - u2) / (n1 + n2);

                double value = item.getTotal(0) + item.getTotal(1);
                double variance = item.getVariance(0) + item.getVariance(1) + incVariance;

                nextNode.insertBucket(value, variance);
                item.compressBucketsRow(2);

                if (nextNode.bucketSizeRow <= MAXIMUM_BUCKETS) {
                    break;
                }
            } else {
                break;
            }

            i++;
        }
    }

    private boolean shouldAdapt(int n0, int n1, double value) {
        if (n1 <= MINIMUM_SUBWINDOW_WIDTH + 1 || n0 <= MINIMUM_SUBWINDOW_WIDTH + 1) {
            return false;
        }

        double coeff = log(2.0 * log(getWidth()) / delta);
        double var = elementVariance / width;

        double h1 = (n0 - MINIMUM_SUBWINDOW_WIDTH + 1);
        double h2 = (n1 - MINIMUM_SUBWINDOW_WIDTH + 1);

        double hMean = (2.0 * h1 * h2) / (h1 + h2);

        double epsilon = sqrt((4.0 / hMean) * var * coeff) + coeff * (4.0 / (3.0 * hMean));
        return abs(value) > epsilon;
    }

    private void deleteElement() {
        int n = bucketSize(lastBucketIndex);
        Bucket node = bucketList.tail();
        double nodeMean = node.getTotal(0) / n;

        width -= n;
        elementSum -= node.getTotal(0);
        elementVariance -= node.getVariance(0) +
                n * width * pow(nodeMean - elementSum / width, 2) / (n + width);

        // Delete Bucket
        node.removeBucket();
        if (node.bucketSizeRow == 0) {
            bucketList.removeFromTail();
            lastBucketIndex--;
        }
    }
}
