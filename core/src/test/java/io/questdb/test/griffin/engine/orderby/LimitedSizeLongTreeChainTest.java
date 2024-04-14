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

package io.questdb.test.griffin.engine.orderby;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.orderby.BinaryHeap;
import io.questdb.griffin.engine.orderby.LimitedSizeLongTreeChain;
import io.questdb.griffin.engine.orderby.LongTreeChain;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.std.*;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Test RBTree removal cases asserting final tree structure .
 */
public class LimitedSizeLongTreeChainTest extends AbstractCairoTest {
    static class SimpleRecord implements Record {
        private long rowValue;
        private long rowId;

        public SimpleRecord(long rowValue, long rowId) {
            this.rowValue = rowValue;
            this.rowId = rowId;
        }

        @Override
        public long getRowId() {
            return rowId;
        }

        @Override
        public long getLong(int col) {
            return rowValue;
        }
    }

    static class SimpleRecordCursor implements RecordCursor {
        public int recordAtCalls = 0;
        private final long[] values;

        public SimpleRecordCursor(long[] values) {
            this.values = values;
        }

        @Override
        public void close() {
            throw new RuntimeException("unsupported");
        }

        @Override
        public Record getRecord() {
            throw new RuntimeException("unsupported");
        }

        @Override
        public Record getRecordB() {
            throw new RuntimeException("unsupported");
        }

        @Override
        public boolean hasNext() throws DataUnavailableException {
            throw new RuntimeException("unsupported");
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            recordAtCalls++;
            ((SimpleRecord) record).rowId = atRowId;
            ((SimpleRecord) record).rowValue = values[(int) atRowId];
        }

        @Override
        public long size() throws DataUnavailableException {
            throw new RuntimeException("unsupported");
        }

        @Override
        public void toTop() {
            throw new RuntimeException("unsupported");
        }
    }

    static class SimpleRecordComparator implements RecordComparator {
        private Record left;
        public int compareCalls = 0;
        public int setLeftCalls = 0;

        @Override
        public int compare(Record record) {
            compareCalls++;
            return Long.compare(left.getLong(0), record.getLong(0));
        }

        @Override
        public void setLeft(Record record) {
            setLeftCalls++;
            left = record;
        }
    }

    @Test
    public void testLongTreeChainMemoryLimit() {
        // 40 MB is enough, but 39 MB is not
        // compareCalls: 18919688, setLeftCalls: 999999, recordAtCalls: 19919688
        {
            SimpleRecordComparator recordComparator = new SimpleRecordComparator();
            int recordAtCalls;
            try (LongTreeChain longTreeChain = new LongTreeChain(Numbers.SIZE_1MB, 39, 8 * Numbers.SIZE_1MB, 1024)) {
                recordAtCalls = runLongTreeChain(
                        new Rnd(),
                        1_000_000,
                        longTreeChain,
                        recordComparator
                );
            }
            System.out.printf("compareCalls: %d, setLeftCalls: %d, recordAtCalls: %d%n", recordComparator.compareCalls, recordComparator.setLeftCalls, recordAtCalls);
        }
        Assert.assertThrows(LimitOverflowException.class, () -> {
            try (LongTreeChain longTreeChain = new LongTreeChain(Numbers.SIZE_1MB, 38, 8 * Numbers.SIZE_1MB, 1024)) {
                runLongTreeChain(
                        new Rnd(),
                        1_000_000,
                        longTreeChain,
                        new SimpleRecordComparator()
                );
            }
        });

    }

    @Test
    public void testBinaryHeapMemoryLimit() {
        // 8 MB is enough, but 7 MB is not
        // compareCalls: 54095651, setLeftCalls: 35810229, recordAtCalls: 73000765
        {
            SimpleRecordComparator recordComparator = new SimpleRecordComparator();
            int recordAtCalls;
            try (BinaryHeap binaryHeap = new BinaryHeap(Numbers.SIZE_1MB, 7)) {
                recordAtCalls = runBinaryHeap(
                        new Rnd(),
                        1_000_000,
                        binaryHeap,
                        recordComparator
                );
            }
            System.out.printf("compareCalls: %d, setLeftCalls: %d, recordAtCalls: %d%n", recordComparator.compareCalls, recordComparator.setLeftCalls, recordAtCalls);
        }
        Assert.assertThrows(LimitOverflowException.class, () -> {
            try (BinaryHeap binaryHeap = new BinaryHeap(Numbers.SIZE_1MB, 6)) {
                runBinaryHeap(
                        new Rnd(),
                        1_000_000,
                        binaryHeap,
                        new SimpleRecordComparator()
                );
            }
        });

    }

    private int runBinaryHeap(Rnd rnd, int size, BinaryHeap binaryHeap, RecordComparator recordComparator) {
        long[] values = new long[size];
        long[] sortedValues = new long[size];
        for (int i = 0; i < size; i++) {
            values[i] = rnd.nextLong();
            sortedValues[i] = values[i];
        }
        Arrays.sort(sortedValues);

        SimpleRecordCursor recordCursor = new SimpleRecordCursor(values);
        SimpleRecord leftRecord = new SimpleRecord(0, 0);
        SimpleRecord rightRecord = new SimpleRecord(0, 0);
        for (int i = 0; i < size; i++) {
            recordCursor.recordAt(leftRecord, i);
            binaryHeap.put(leftRecord, recordCursor, rightRecord, recordComparator);
        }
        binaryHeap.sort(leftRecord, recordCursor, rightRecord, recordComparator);

        BinaryHeap.HeapCursor cursor = binaryHeap.getCursor();
        int position = 0;
        while (cursor.hasNext()) {
            if (values[(int) cursor.next()] != sortedValues[position++]) {
                Assert.fail("invalid cursor enumeration");
            }
        }
        return recordCursor.recordAtCalls;
    }

    private int runLongTreeChain(Rnd rnd, int size, LongTreeChain longTreeChain, RecordComparator recordComparator) {
        long[] values = new long[size];
        long[] sortedValues = new long[size];
        for (int i = 0; i < size; i++) {
            values[i] = rnd.nextLong();
            sortedValues[i] = values[i];
        }
        Arrays.sort(sortedValues);

        SimpleRecordCursor recordCursor = new SimpleRecordCursor(values);
        SimpleRecord leftRecord = new SimpleRecord(0, 0);
        SimpleRecord rightRecord = new SimpleRecord(0, 0);
        for (int i = 0; i < size; i++) {
            recordCursor.recordAt(leftRecord, i);
            longTreeChain.put(leftRecord, recordCursor, rightRecord, recordComparator);
        }
        LongTreeChain.TreeCursor cursor = longTreeChain.getCursor();
        int position = 0;
        while (cursor.hasNext()) {
            if (values[(int) cursor.next()] != sortedValues[position++]) {
                Assert.fail("invalid cursor enumeration");
            }
        }
        return recordCursor.recordAtCalls;
    }

    //used in all tests to hide api complexity
    LimitedSizeLongTreeChain chain;
    RecordComparator comparator;
    TestRecordCursor cursor;
    TestRecord left;
    TestRecord placeholder;

    @After
    public void after() {
        chain.close();
    }

    @Before
    public void before() {
        chain = new LimitedSizeLongTreeChain(
                configuration.getSqlSortKeyPageSize(),
                configuration.getSqlSortKeyMaxPages(),
                configuration.getSqlSortLightValuePageSize(),
                configuration.getSqlSortLightValueMaxPages(),
                true,
                20
        );
    }

    @Test
    public void test_create_ordered_tree() {
        assertTree("[Black,2]\n" +
                        " L-[Black,1]\n" +
                        " R-[Black,4]\n" +
                        "   L-[Red,3]\n" +
                        "   R-[Red,5]\n",
                1, 2, 3, 4, 5
        );
    }

    @Test
    public void test_create_ordered_tree_with_duplicates() {
        assertTree(
                "[Black,2]\n" +
                        " L-[Black,1(2)]\n" +
                        " R-[Black,4(2)]\n" +
                        "   L-[Red,3(2)]\n" +
                        "   R-[Red,5]\n",
                1, 2, 3, 4, 5, 1, 4, 3
        );
    }

    @Test
    public void test_create_ordered_tree_with_input_in_descending_order() {
        assertTree(
                "[Black,4]\n" +
                        " L-[Black,2]\n" +
                        "   L-[Red,1]\n" +
                        "   R-[Red,3]\n" +
                        " R-[Black,5]\n",
                5, 4, 3, 2, 1
        );
    }

    @Test
    public void test_create_ordered_tree_with_input_in_no_order() {
        assertTree(
                "[Black,3]\n" +
                        " L-[Black,2]\n" +
                        "   L-[Red,1]\n" +
                        " R-[Black,5]\n" +
                        "   L-[Red,4]\n"
                , 3, 2, 5, 1, 4
        );
    }

    //sibling is black and its both children are black (?)
    @Test
    public void test_remove_black_node_with_black_sibling_with_both_children_black() {
        assertTree(
                "[Black,30]\n" +
                        " L-[Black,20]\n" +
                        " R-[Black,40]\n" +
                        "   L-[Red,35]\n",
                30, 20, 40, 35
        );
        removeRowWithValue(20L);
        assertTree(
                "[Black,35]\n" +
                        " L-[Black,30]\n" +
                        " R-[Black,40]\n"
        );
    }

    //right left case
    @Test
    public void test_remove_black_node_with_black_sibling_with_red_left_child() {
        assertTree(
                "[Black,30]\n" +
                        " L-[Black,20]\n" +
                        " R-[Black,40]\n" +
                        "   L-[Red,35]\n",
                30, 20, 40, 35
        );
        removeRowWithValue(20L);
        assertTree(
                "[Black,35]\n" +
                        " L-[Black,30]\n" +
                        " R-[Black,40]\n"
        );
    }

    //new test cases
    //current node is double black and not the root; sibling is black and at least one of its children is red
    //right right case
    @Test
    public void test_remove_black_node_with_black_sibling_with_red_right_child() {
        assertTree(
                "[Black,30]\n" +
                        " L-[Black,20]\n" +
                        " R-[Black,40]\n" +
                        "   L-[Red,35]\n" +
                        "   R-[Red,50]\n",
                30, 20, 40, 35, 50
        );
        removeRowWithValue(20L);
        assertTree(
                "[Black,40]\n" +
                        " L-[Black,30]\n" +
                        "   R-[Red,35]\n" +
                        " R-[Black,50]\n"
        );
    }

    @Test
    public void test_remove_black_node_with_both_children_and_right_is_not_successor() {
        assertTree(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Red,3]\n" +
                        "   L-[Black,2]\n" +
                        "   R-[Black,5]\n" +
                        "     L-[Red,4]\n",
                0, 1, 2, 3, 5, 4
        );

        removeRowWithValue(3);
        assertTree(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Red,4]\n" +
                        "   L-[Black,2]\n" +
                        "   R-[Black,5]\n"
        );
    }

    @Test
    public void test_remove_black_node_with_both_children_and_right_is_not_successor_but_doesnt_require_rotation() {
        assertTree(
                "[Black,3]\n" +
                        " L-[Red,1]\n" +
                        "   L-[Black,0]\n" +
                        "   R-[Black,2]\n" +
                        " R-[Red,5]\n" +
                        "   L-[Black,4]\n" +
                        "   R-[Black,6]\n" +
                        "     R-[Red,7]\n",
                0, 1, 2, 3, 4, 5, 6, 7
        );

        removeRowWithValue(3);

        assertTree(
                "[Black,4]\n" +
                        " L-[Red,1]\n" +
                        "   L-[Black,0]\n" +
                        "   R-[Black,2]\n" +
                        " R-[Red,6]\n" +
                        "   L-[Black,5]\n" +
                        "   R-[Black,7]\n"
        );
    }

    @Test
    public void test_remove_black_node_with_both_children_and_right_is_not_successor_requires_rotation_todo() {
        assertTree(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Red,3]\n" +
                        "   L-[Black,2]\n" +
                        "   R-[Black,5]\n" +
                        "     L-[Red,4]\n",
                0, 1, 2, 3, 5, 4
        );

        removeRowWithValue(3);

        assertTree(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Red,4]\n" +
                        "   L-[Black,2]\n" +
                        "   R-[Black,5]\n"
        );
    }

    @Test
    public void test_remove_black_node_with_both_children_and_right_is_successor() {
        assertTree(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Black,3]\n" +
                        "   L-[Red,2]\n" +
                        "   R-[Red,4]\n",
                0, 1, 2, 3, 4
        );

        removeRowWithValue(3);

        assertTree(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Black,4]\n" +
                        "   L-[Red,2]\n"
        );
    }

    @Test
    public void test_remove_black_node_with_left_child_only() {
        assertTree(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        "   L-[Red,-1]\n" +
                        " R-[Black,2]\n" +
                        "   R-[Red,3]\n",
                0, 1, 2, 3, -1
        );
        removeRowWithValue(0);
        assertTree(
                "[Black,1]\n" +
                        " L-[Black,-1]\n" +
                        " R-[Black,2]\n" +
                        "   R-[Red,3]\n"
        );
    }

    @Test
    public void test_remove_black_node_with_no_children() {
        assertTree(
                "[Black,1]\n" +
                        " L-[Red,0]\n" +
                        " R-[Red,2]\n",
                0, 1, 2
        );
        removeRowWithValue(2);
        assertTree(
                "[Black,1]\n" +
                        " L-[Red,0]\n"
        );
    }

    @Test
    public void test_remove_black_node_with_right_child_only() {
        assertTree(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Black,2]\n" +
                        "   R-[Red,3]\n",
                0, 1, 2, 3
        );
        removeRowWithValue(2);
        assertTree(
                "[Black,1]\n" +
                        " L-[Black,0]\n" +
                        " R-[Black,3]\n"
        );
    }

    @Test
    public void test_remove_red_node_with_both_children_and_right_is_black_successor() {
        assertTree(
                "[Black,3]\n" +
                        " L-[Red,1]\n" +
                        "   L-[Black,0]\n" +
                        "   R-[Black,2]\n" +
                        " R-[Red,5]\n" +
                        "   L-[Black,4]\n" +
                        "   R-[Black,6]\n" +
                        "     R-[Red,7]\n",
                0, 1, 2, 3, 4, 5, 6, 7
        );
        removeRowWithValue(5);
        assertTree(
                "[Black,3]\n" +
                        " L-[Red,1]\n" +
                        "   L-[Black,0]\n" +
                        "   R-[Black,2]\n" +
                        " R-[Red,6]\n" +
                        "   L-[Black,4]\n" +
                        "   R-[Black,7]\n"
        );
    }

    private void assertTree(String expected, long... values) {
        createTree(values);
        assertTree(expected);
    }

    private void assertTree(String expected) {
        TestUtils.assertEquals(expected, toString(cursor, placeholder));
    }

    private void createTree(long... values) {
        cursor = new TestRecordCursor(values);
        left = (TestRecord) cursor.getRecord();
        placeholder = (TestRecord) cursor.getRecordB();
        comparator = new TestRecordComparator();
        comparator.setLeft(left);
        while (cursor.hasNext()) {
            chain.put(left, cursor, placeholder, comparator);
        }
    }

    private void removeRowWithValue(long value) {
        cursor.recordAtValue(left, value);
        long node = chain.find(left, cursor, placeholder, comparator);
        chain.removeAndCache(node);
    }

    @NotNull
    private String toString(TestRecordCursor cursor, TestRecord right) {
        StringSink sink = new StringSink();
        chain.print(sink, rowid -> {
            cursor.recordAt(right, rowid);
            return String.valueOf(right.getLong(0));
        });
        return sink.toString();
    }

    static class TestRecord implements Record {
        long position;
        long value;

        @Override
        public long getLong(int col) {
            return value;
        }

        @Override
        public long getRowId() {
            return position;
        }
    }

    static class TestRecordComparator implements RecordComparator {
        Record left;

        @Override
        public int compare(Record record) {
            return (int) (left.getLong(0) - record.getLong(0));
        }

        @Override
        public void setLeft(Record record) {
            left = record;
        }
    }

    static class TestRecordCursor implements RecordCursor {
        final Record left = new TestRecord();
        int position = -1;
        final Record right = new TestRecord();
        final LongList values = new LongList();

        TestRecordCursor(long... newValues) {
            for (int i = 0; i < newValues.length; i++) {
                this.values.add(newValues[i]);
            }
        }

        @Override
        public void close() {
            //nothing to do here
        }

        @Override
        public Record getRecord() {
            return left;
        }

        @Override
        public Record getRecordB() {
            return right;
        }

        @Override
        public boolean hasNext() {
            if (position < values.size() - 1) {
                position++;
                recordAt(left, position);
                return true;
            }

            return false;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            ((TestRecord) record).value = values.get((int) atRowId);
            ((TestRecord) record).position = atRowId;
        }

        public void recordAtValue(Record record, long value) {
            for (int i = 0; i < values.size(); i++) {
                if (values.get(i) == value) {
                    recordAt(record, i);
                    return;
                }
            }

            throw new RuntimeException("Can't find value " + value + " in " + values);
        }

        @Override
        public long size() {
            return values.size();
        }

        @Override
        public void toTop() {
            position = 0;
        }
    }
}
