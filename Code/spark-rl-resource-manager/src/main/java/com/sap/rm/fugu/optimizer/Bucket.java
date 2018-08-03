package com.sap.rm.fugu.optimizer;

import static com.sap.rm.fugu.optimizer.AdaptiveWindow.MAXIMUM_BUCKETS;

class Bucket {
    int bucketSizeRow = 0;

    private Bucket next;
    private Bucket previous;

    private double bucketTotal[] = new double[MAXIMUM_BUCKETS + 1];
    private double bucketVariance[] = new double[MAXIMUM_BUCKETS + 1];

    Bucket(Bucket nextNode, Bucket previousNode) {
        next = nextNode;
        previous = previousNode;
        if (nextNode != null) nextNode.previous = this;
        if (previousNode != null) previousNode.next = this;
        clear();
    }

    void insertBucket(double value, double variance) {
        setTotal(value, bucketSizeRow);
        setVariance(variance, bucketSizeRow);
        bucketSizeRow++;
    }

    void removeBucket() {
        compressBucketsRow(1);
    }

    void compressBucketsRow(int deletedItems) {
        for (int i = deletedItems; i <= MAXIMUM_BUCKETS; i++) {
            bucketTotal[i - deletedItems] = bucketTotal[i];
            bucketVariance[i - deletedItems] = bucketVariance[i];
        }

        for (int i = 1; i <= deletedItems; i++) {
            clearBucket(MAXIMUM_BUCKETS - i + 1);
        }

        bucketSizeRow -= deletedItems;
    }

    Bucket getPrevious() {
        return this.previous;
    }

    Bucket next() {
        return this.next;
    }

    void setNext(Bucket next) {
        this.next = next;
    }

    double getTotal(int n) {
        return bucketTotal[n];
    }

    double getVariance(int n) {
        return bucketVariance[n];
    }

    private void clear() {
        bucketSizeRow = 0;
        for (int i = 0; i <= MAXIMUM_BUCKETS; i++) {
            clearBucket(i);
        }
    }

    private void clearBucket(int n) {
        setTotal(0, n);
        setVariance(0, n);
    }

    private void setTotal(double value, int n) {
        bucketTotal[n] = value;
    }

    private void setVariance(double value, int n) {
        bucketVariance[n] = value;
    }
}
