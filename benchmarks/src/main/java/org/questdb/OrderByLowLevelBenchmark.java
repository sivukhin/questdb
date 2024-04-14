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

package org.questdb;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.orderby.BinaryHeap;
import io.questdb.griffin.engine.orderby.LongTreeChain;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.log.LogFactory;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 1)
@Timeout(time = 60)
@Fork(1)
public class OrderByLowLevelBenchmark {

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

    private static final int NUM_ROWS = 10_000_000;

    private long[] sortedValues;
    private SimpleRecordCursor recordCursor;
    private SimpleRecord longTreeChainLeft, longTreeChainRight;
    private RecordComparator recordComparator;

    public static void main(String[] args) throws RunnerException, SqlException {
        Options opt = new OptionsBuilder()
                .include(OrderByLowLevelBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Setup(Level.Trial)
    public void globalSetup() throws Exception {
        long[] values = new long[NUM_ROWS];
        sortedValues = new long[NUM_ROWS];

        Rnd rnd = new Rnd();
        for (int i = 0; i < NUM_ROWS; i++) {
            values[i] = rnd.nextLong();
            sortedValues[i] = values[i];
        }
        Arrays.sort(sortedValues);

        recordCursor = new SimpleRecordCursor(values);

        longTreeChainLeft = new SimpleRecord(0, 0);
        longTreeChainRight = new SimpleRecord(0, 0);
        BytecodeAssembler asm = new BytecodeAssembler();
        RecordComparatorCompiler recordComparatorCompiler = new RecordComparatorCompiler(asm);

        IntList keyColumnIndices = new IntList();
        keyColumnIndices.add(1);

        GenericRecordMetadata columnTypes = new GenericRecordMetadata();
        columnTypes.add(0, new TableColumnMetadata("rowValue", ColumnType.LONG));

        recordComparator = recordComparatorCompiler.compile(columnTypes, keyColumnIndices);
    }

    @Benchmark
    public void longTreeChain() {
        try (LongTreeChain longTreeChain = new LongTreeChain(4 * Numbers.SIZE_1MB, 128, 8 * Numbers.SIZE_1MB, 1024)) {
            for (int i = 0; i < NUM_ROWS; i++) {
                recordCursor.recordAt(longTreeChainLeft, i);
                longTreeChain.put(longTreeChainLeft, recordCursor, longTreeChainRight, recordComparator);
            }
            LongTreeChain.TreeCursor cursor = longTreeChain.getCursor();
            int position = 0;
            while (cursor.hasNext()) {
                long next = recordCursor.values[(int) cursor.next()];
                if (sortedValues[position] != next) {
                    throw new RuntimeException(String.format("%d number: expected %d, actual %d", position, sortedValues[position], next));
                }
                position++;
            }
        }
    }

    @Benchmark
    public void binaryHeap() {
        try (BinaryHeap binaryHeap = new BinaryHeap(4 * Numbers.SIZE_1MB, 128)) {
            for (int i = 0; i < NUM_ROWS; i++) {
                recordCursor.recordAt(longTreeChainLeft, i);
                binaryHeap.put(longTreeChainLeft, recordCursor, longTreeChainRight, recordComparator);
            }
            binaryHeap.sort(longTreeChainLeft, recordCursor, longTreeChainRight, recordComparator);
            BinaryHeap.HeapCursor cursor = binaryHeap.getCursor();
            int position = 0;
            while (cursor.hasNext()) {
                long next = recordCursor.values[(int) cursor.next()];
                if (sortedValues[position] != next) {
                    throw new RuntimeException(String.format("%d number: expected %d, actual %d", position, sortedValues[position], next));
                }
                position++;
            }
        }
    }
}
