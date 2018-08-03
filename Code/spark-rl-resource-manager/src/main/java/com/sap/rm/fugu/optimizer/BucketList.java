package com.sap.rm.fugu.optimizer;

class BucketList {
    private Bucket head;
    private Bucket tail;

    BucketList() {
        head = new Bucket(null, null);
        tail = head;
    }

    Bucket head() {
        return this.head;
    }

    Bucket tail() {
        return this.tail;
    }

    void addToTail() {
        this.tail = new Bucket(null, this.tail);
        if (this.head == null) this.head = this.tail;
    }

    void removeFromTail() {
        this.tail = this.tail.getPrevious();
        if (this.tail == null) {
            this.head = null;
        } else {
            this.tail.setNext(null);
        }
    }
}
