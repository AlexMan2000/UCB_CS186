package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.HashFunc;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.disk.Partition;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.*;

public class GHJOperator extends JoinOperator {
    private int numBuffers;
    private Run joinedRecords;

    public GHJOperator(QueryOperator leftSource,
                       QueryOperator rightSource,
                       String leftColumnName,
                       String rightColumnName,
                       TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.GHJ);
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
        this.joinedRecords = null;
    }

    @Override
    public int estimateIOCost() {
        // Since this has a chance of failing on certain inputs we give it the
        // maximum possible cost to encourage the optimizer to avoid it
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean materialized() { return true; }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (joinedRecords == null) {
            // Executing GHJ on-the-fly is arduous without coroutines, so
            // instead we'll accumulate all of our joined records in this run
            // and return an iterator over it once the algorithm completes
            this.joinedRecords = new Run(getTransaction(), getSchema());
            this.run(getLeftSource(), getRightSource(), 1);
        };
        return joinedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * For every record in the given iterator, hashes the value
     * at the column we're joining on and adds it to the correct partition in
     * partitions.
     *
     * @param partitions an array of partitions to split the records into
     * @param records iterable of records we want to partition
     * @param left true if records are from the left relation, otherwise false
     * @param pass the current pass (used to pick a hash function)
     */
    private void partition(Partition[] partitions, Iterable<Record> records, boolean left, int pass) {
        // TODO(proj3_part1): implement the partitioning logic
        // You may find the implementation in SHJOperator.java to be a good
        // starting point. You can use the static method HashFunc.hashDataBox
        // to get a hash value.
        for (Record record: records) {
            DataBox columnValue;
            if (left) {
                columnValue = record.getValue(getLeftColumnIndex());
            } else {
                columnValue = record.getValue(getRightColumnIndex());
            }

            // Hash the column-value(key)
            int hashValue = HashFunc.hashDataBox(columnValue, pass);
            int partitionNum = hashValue % partitions.length;
            if (partitionNum < 0) {
                partitionNum += partitions.length;
            }

            partitions[partitionNum].add(record);
        }
    }

    /**
     * Runs the buildAndProbe stage on a given partition. Should add any
     * matching records found during the probing stage to this.joinedRecords.
     */
    private void buildAndProbe(Partition leftPartition, Partition rightPartition) {
        // true if the probe records come from the left partition, false otherwise
        boolean probeFirst;
        // We'll build our in memory hash table with these records
        Iterable<Record> buildRecords;
        // We'll probe the table with these records
        Iterable<Record> probeRecords;
        // The index of the join column for the build records
        int buildColumnIndex;
        // The index of the join column for the probe records
        int probeColumnIndex;

        if (leftPartition.getNumPages() <= this.numBuffers - 2) {
            buildRecords = leftPartition;
            buildColumnIndex = getLeftColumnIndex();
            probeRecords = rightPartition;
            probeColumnIndex = getRightColumnIndex();
            probeFirst = false;
        } else if (rightPartition.getNumPages() <= this.numBuffers - 2) {
            buildRecords = rightPartition;
            buildColumnIndex = getRightColumnIndex();
            probeRecords = leftPartition;
            probeColumnIndex = getLeftColumnIndex();
            probeFirst = true;
        } else {
            throw new IllegalArgumentException(
                "Neither the left nor the right records in this partition " +
                "fit in B-2 pages of memory."
            );
        }
        // TODO(proj3_part1): implement the building and probing stage
        // You shouldn't refer to any variable starting with "left" or "right"
        // here, use the "build" and "probe" variables we set up for you.
        // Check out how SHJOperator implements this function if you feel stuck.


        Map<DataBox, List<Record>> hashTable = new HashMap<>();

        // Stage 1: Building
        for (Record record: buildRecords) {
            DataBox key = record.getValue(buildColumnIndex);
            // No such key, create a bucket
            if (!hashTable.containsKey(key)) {
                hashTable.put(key, new ArrayList<>());
            }
            // Has such key, add to the existing bucket
            hashTable.get(key).add(record);
        }

        // Stage 2: Staging
        for (Record probe: probeRecords) {
            DataBox key = probe.getValue(probeColumnIndex);
            if (hashTable.containsKey(key)) {
                List<Record> records = hashTable.get(key);
                for (Record build: records) {
                    Record grace;
                    // Because we join left and right, the left relation
                    // should appear first in the final record

                    // If probe relation is the left relation
                    if (probeFirst) {
                        grace = probe.concat(build);
                    } else {
                        grace = build.concat(probe);
                    }
                    this.joinedRecords.add(grace);
                }
            }
        }
    }

    /**
     * Runs the grace hash join algorithm. Each pass starts by partitioning
     * leftRecords and rightRecords. If we can run build and probe on a
     * partition we should immediately do so, otherwise we should apply the
     * grace hash join algorithm recursively to break up the partitions further.
     */
    private void run(Iterable<Record> leftRecords, Iterable<Record> rightRecords, int pass) {
        assert pass >= 1;
        if (pass > 5) throw new IllegalStateException("Reached the max number of passes");

        // Create empty partitions
        Partition[] leftPartitions = createPartitions(true);
        Partition[] rightPartitions = createPartitions(false);

        // Partition records into left and right
        this.partition(leftPartitions, leftRecords, true, pass);
        this.partition(rightPartitions, rightRecords, false, pass);

        for (int i = 0; i < leftPartitions.length; i++) {
            // TODO(proj3_part1): implement the rest of grace hash join
            // If you meet the conditions to run the build and probe you should
            // do so immediately. Otherwise you should make a recursive call.

            /*
              Since leftPartitions and rightparitions are assumed to be of the same length
              We can do the following without OUT OF BOUND exception
            */
            Partition leftPartition = leftPartitions[i];
            Partition rightPartition = rightPartitions[i];
            if (leftPartition.getNumPages() <= numBuffers - 2 || rightPartition.getNumPages() <= numBuffers - 2) {
                buildAndProbe(leftPartition, rightPartition);
            } else {
                run(leftPartition, rightPartition, pass + 1);
            }

        }
    }

    // Provided Helpers ////////////////////////////////////////////////////////

    /**
     * Create an appropriate number of partitions relative to the number of
     * available buffers we have.
     *
     * @return an array of partitions
     */
    private Partition[] createPartitions(boolean left) {
        int usableBuffers = this.numBuffers - 1;
        Partition partitions[] = new Partition[usableBuffers];
        for (int i = 0; i < usableBuffers; i++) {
            partitions[i] = createPartition(left);
        }
        return partitions;
    }

    /**
     * Creates either a regular partition or a smart partition depending on the
     * value of this.useSmartPartition.
     * @param left true if this partition will store records from the left
     *             relation, false otherwise
     * @return a partition to store records from the specified partition
     */
    private Partition createPartition(boolean left) {
        Schema schema = getRightSource().getSchema();
        if (left) schema = getLeftSource().getSchema();
        return new Partition(getTransaction(), schema);
    }

    // Student Input Methods ///////////////////////////////////////////////////

    /**
     * Creates a record using val as the value for a single column of type int.
     * An extra column containing a 500 byte string is appended so that each
     * page will hold exactly 8 records.
     *
     * @param val value the field will take
     * @return a record
     */
    private static Record createRecord(int val) {
        String s = new String(new char[500]);
        return new Record(val, s);
    }

    /**
     * This method is called in testBreakSHJButPassGHJ.
     *
     * Come up with two lists of records for leftRecords and rightRecords such
     * that SHJ will error when given those relations, but GHJ will successfully
     * run. createRecord(int val) takes in an integer value and returns a record
     * with that value in the column being joined on.
     *
     * Hints: Both joins will have access to B=6 buffers and each page can fit
     * exactly 8 records.
     *
     * @return Pair of leftRecords and rightRecords
     */
    public static Pair<List<Record>, List<Record>> getBreakSHJInputs() {
        ArrayList<Record> leftRecords = new ArrayList<>();
        ArrayList<Record> rightRecords = new ArrayList<>();

        // TODO(proj3_part1): populate leftRecords and rightRecords such that
        // SHJ breaks when trying to join them but not GHJ

        // When the left records cannot fit in B - 2 buffers, SHJ will error but SHJ will not,

        // Any big enough number >= 136 is ok for SHJ to error and GHJ will succeed
        for (int i = 0; i < 136; i++) {
            leftRecords.add(createRecord(i));
            rightRecords.add(createRecord(i));
        }

        return new Pair<>(leftRecords, rightRecords);
    }

    /**
     * This method is called in testGHJBreak.
     *
     * Come up with two lists of records for leftRecords and rightRecords such
     * that GHJ will error (in our case hit the maximum number of passes).
     * createRecord(int val) takes in an integer value and returns a record
     * with that value in the column being joined on.
     *
     * Hints: Both joins will have access to B=6 buffers and each page can fit
     * exactly 8 records.
     *
     * @return Pair of leftRecords and rightRecords
     */
    public static Pair<List<Record>, List<Record>> getBreakGHJInputs() {
        ArrayList<Record> leftRecords = new ArrayList<>();
        ArrayList<Record> rightRecords = new ArrayList<>();
        // TODO(proj3_part1): populate leftRecords and rightRecords such that GHJ breaks

        /*
        *  Just consider the extreme case where all records hashed to the same partition
        *  in different pass
        *  But after 5 pass the partition size is still bigger than B - 2
        *
        *  So we create the records all with the same key in both left and rightRecords
        *  But how many records do we need?
        *
        *  Minimally 33, (B - 2) * 8 + 1 = 33;
        * */

        int randomValue = 100;
        for (int i = 0; i < 33; i++) {
            leftRecords.add(createRecord(randomValue));
            rightRecords.add(createRecord(randomValue));
        }

        return new Pair<>(leftRecords, rightRecords);
    }
}

