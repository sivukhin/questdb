/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.std.*;

public class BinaryHeap implements Mutable, Reopenable {
    private final HeapCursor cursor = new HeapCursor();
    private final MemoryPages rowIds;
    private long lastIndex;
    private long sortedIndex;

    public BinaryHeap(long keyPageSize, int keyMaxPages) {
        this.rowIds = new MemoryPages(keyPageSize, keyMaxPages);
        this.lastIndex = 0;
        this.sortedIndex = 0;
    }

    public void put(
            Record leftRecord,
            RecordCursor sourceCursor,
            Record rightRecord,
            RecordComparator recordComparator
    ) {
        long insertedRowId = leftRecord.getRowId();

        long currentRef = rowIds.allocate(Long.BYTES);
        long currentIndex = lastIndex++;

        Unsafe.getUnsafe().putLong(currentRef, insertedRowId);
        recordComparator.setLeft(leftRecord);

        while (currentIndex > 0) {
            long parentIndex = (currentIndex - 1) / 2;
            long parentRef = rowIds.addressOf(Long.BYTES * parentIndex);
            long parentRowId = Unsafe.getUnsafe().getLong(parentRef);
            sourceCursor.recordAt(rightRecord, parentRowId);
            int cmp = recordComparator.compare(rightRecord);
            if (cmp < 0 || (cmp == 0 && insertedRowId < parentRowId)) {
                swap(currentRef, insertedRowId, parentRef, parentRowId);
            } else {
                break;
            }
            currentIndex = parentIndex;
            currentRef = parentRef;
        }
    }

    @Override
    public void clear() {
    }

    @Override
    public void close() {
        Misc.free(rowIds);
    }

    @Override
    public void reopen() {
        rowIds.reopen();
    }

    public void sort(Record leftRecord, RecordCursor sourceCursor, Record rightRecord, RecordComparator recordComparator) {
        long firstRef = rowIds.addressOf(0);
        for (sortedIndex = 0; sortedIndex + 1 < lastIndex; sortedIndex++) {
            long index = lastIndex - sortedIndex - 1;
            long lastRef = rowIds.addressOf(Long.BYTES * index);
            long firstRowId = Unsafe.getUnsafe().getLong(firstRef);
            long currentRowId = Unsafe.getUnsafe().getLong(lastRef);
            swap(firstRef, firstRowId, lastRef, currentRowId);

            long currentIndex = 0;
            long currentRef = firstRef;
            sourceCursor.recordAt(leftRecord, currentRowId);
            recordComparator.setLeft(leftRecord);
            while (currentIndex < index) {
                long leftIndex = 2 * currentIndex + 1;
                long rightIndex = 2 * currentIndex + 2;
                int leftCmp = -1, rightCmp = -1;
                long leftRef = -1, leftRowId = -1, rightRef = -1, rightRowId = -1;
                if (leftIndex < index) {
                    leftRef = rowIds.addressOf(Long.BYTES * leftIndex);
                    leftRowId = Unsafe.getUnsafe().getLong(leftRef);
                    sourceCursor.recordAt(rightRecord, leftRowId);
                    leftCmp = recordComparator.compare(rightRecord);
                }
                if (rightIndex < index) {
                    rightRef = rowIds.addressOf(Long.BYTES * rightIndex);
                    rightRowId = Unsafe.getUnsafe().getLong(rightRef);
                    sourceCursor.recordAt(rightRecord, rightRowId);
                    rightCmp = recordComparator.compare(rightRecord);
                }
                // first, consider cases where direction for sift-down is clear from two comparison between currentNode & left/right
                if (leftCmp > rightCmp || (leftCmp == 0 && rightCmp == 0 && leftRowId < rightRowId)) { // left node - clear winner
                    if (leftCmp > 0 || leftCmp == 0 && leftRowId < currentRowId) {
                        swap(leftRef, leftRowId, currentRef, currentRowId);
                        currentIndex = leftIndex;
                        currentRef = leftRef;
                    } else {
                        break;
                    }
                } else if (rightCmp > leftCmp || (leftCmp == 0 && rightCmp == 0 && rightRowId < leftRowId)) { // right node - clear winner
                    if (rightCmp > 0 || rightCmp == 0 && rightRowId < currentRowId) {
                        swap(rightRef, rightRowId, currentRef, currentRowId);
                        currentIndex = rightIndex;
                        currentRef = rightRef;
                    } else {
                        break;
                    }
                } else {
                    if (leftCmp <= 0) {
                        break;
                    }
                    // not enough information - we will need to explicitly compare left & right
                    sourceCursor.recordAt(leftRecord, leftRowId);
                    recordComparator.setLeft(leftRecord);
                    int cmp = recordComparator.compare(rightRecord);
                    if (cmp < 0 || cmp == 0 && leftRowId < rightRowId) {
                        swap(leftRef, leftRowId, currentRef, currentRowId);
                        currentIndex = leftIndex;
                        currentRef = leftRef;
                    } else {
                        swap(rightRef, rightRowId, currentRef, currentRowId);
                        currentIndex = rightIndex;
                        currentRef = rightRef;
                    }
                    sourceCursor.recordAt(leftRecord, currentRowId);
                    recordComparator.setLeft(leftRecord);
                }
            }
//            validate(lastIndex - sortedIndex - 1, leftRecord, sourceCursor, rightRecord, recordComparator);
        }
    }

    private void validate(long length, Record leftRecord, RecordCursor sourceCursor, Record rightRecord, RecordComparator recordComparator) {
        for (long index = 0; index < length; index++) {
            long currentRef = rowIds.addressOf(Long.BYTES * index);
            long currentRowId = Unsafe.getUnsafe().getLong(currentRef);
            sourceCursor.recordAt(leftRecord, currentRowId);
            recordComparator.setLeft(leftRecord);

            if (2 * index + 1 < length) {
                long leftRef = rowIds.addressOf(Long.BYTES * (2 * index + 1));
                long leftRowId = Unsafe.getUnsafe().getLong(leftRef);
                sourceCursor.recordAt(rightRecord, leftRowId);
                int cmpLeft = recordComparator.compare(rightRecord);
                if (cmpLeft > 0 || cmpLeft == 0 && currentRowId > leftRowId) {
                    throw new RuntimeException(String.format("left invariant broken: %d (rows %d and %d, cmp %d)", index, currentRowId, leftRowId, cmpLeft));
                }
            }
            if (2 * index + 2 < length) {
                long rightRef = rowIds.addressOf(Long.BYTES * (2 * index + 2));
                long rightRowId = Unsafe.getUnsafe().getLong(rightRef);
                sourceCursor.recordAt(rightRecord, rightRowId);
                int cmpRight = recordComparator.compare(rightRecord);
                if (cmpRight > 0 || cmpRight == 0 && currentRowId > rightRowId) {
                    throw new RuntimeException(String.format("right invariant broken: %d (rows %d and %d, cmp %d)", index, currentRowId, rightRowId, cmpRight));
                }
            }
        }
    }

    private void swap(long refA, long valueA, long refB, long valueB) {
        Unsafe.getUnsafe().putLong(refA, valueB);
        Unsafe.getUnsafe().putLong(refB, valueA);
    }


    public HeapCursor getCursor() {
        cursor.toTop();
        return cursor;
    }

    public class HeapCursor {
        private long current;

        public void clear() {
            current = lastIndex;
        }

        public boolean hasNext() {
            return current > 0;
        }

        public long next() {
            long rowId = Unsafe.getUnsafe().getLong(rowIds.addressOf(Long.BYTES * (current - 1)));
            current--;
            return rowId;
        }

        public void toTop() {
            setup();
        }

        private void setup() {
            current = lastIndex;
        }
    }
}
