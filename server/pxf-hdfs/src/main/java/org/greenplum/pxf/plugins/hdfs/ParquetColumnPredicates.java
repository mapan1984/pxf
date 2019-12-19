package org.greenplum.pxf.plugins.hdfs;

import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.filter.ColumnPredicates;

public class ParquetColumnPredicates {

    public static ColumnPredicates.Predicate equalToIgnoreTrailingSpaces(final String target) {
        Preconditions.checkNotNull(target, "target");
        return input -> equalsIgnoreTrailingSpaces(target, input.getBinary().toStringUsingUTF8());
    }

    public static ColumnPredicates.Predicate lessThan(final int target) {
        return input -> input.getInteger() < target;
    }

    public static ColumnPredicates.Predicate lessThan(final long target) {
        return input -> input.getLong() < target;
    }

    public static ColumnPredicates.Predicate lessThan(final boolean target) {
        return input -> Boolean.compare(input.getBoolean(), target) < 0;
    }

    public static ColumnPredicates.Predicate lessThan(final float target) {
        return input -> Float.compare(input.getFloat(), target) < 0;
    }

    public static ColumnPredicates.Predicate lessThan(final double target) {
        return input -> Double.compare(input.getDouble(), target) < 0;
    }

    public static ColumnPredicates.Predicate greaterThan(final int target) {
        return input -> input.getInteger() > target;
    }

    public static ColumnPredicates.Predicate greaterThan(final long target) {
        return input -> input.getLong() > target;
    }

    public static ColumnPredicates.Predicate greaterThan(final boolean target) {
        return input -> Boolean.compare(input.getBoolean(), target) > 0;
    }

    public static ColumnPredicates.Predicate greaterThan(final float target) {
        return input -> Float.compare(input.getFloat(), target) > 0;
    }

    public static ColumnPredicates.Predicate greaterThan(final double target) {
        return input -> Double.compare(input.getDouble(), target) > 0;
    }

    public static ColumnPredicates.Predicate lessThanOrEqual(final int target) {
        return input -> input.getInteger() <= target;
    }

    public static ColumnPredicates.Predicate lessThanOrEqual(final long target) {
        return input -> input.getLong() <= target;
    }

    public static ColumnPredicates.Predicate lessThanOrEqual(final boolean target) {
        return input -> Boolean.compare(input.getBoolean(), target) <= 0;
    }

    public static ColumnPredicates.Predicate lessThanOrEqual(final float target) {
        return input -> Float.compare(input.getFloat(), target) <= 0;
    }

    public static ColumnPredicates.Predicate lessThanOrEqual(final double target) {
        return input -> Double.compare(input.getDouble(), target) <= 0;
    }

    public static ColumnPredicates.Predicate greaterThanOrEqual(final int target) {
        return input -> input.getInteger() >= target;
    }

    public static ColumnPredicates.Predicate greaterThanOrEqual(final long target) {
        return input -> input.getLong() >= target;
    }

    public static ColumnPredicates.Predicate greaterThanOrEqual(final boolean target) {
        return input -> Boolean.compare(input.getBoolean(), target) >= 0;
    }

    public static ColumnPredicates.Predicate greaterThanOrEqual(final float target) {
        return input -> Float.compare(input.getFloat(), target) >= 0;
    }

    public static ColumnPredicates.Predicate greaterThanOrEqual(final double target) {
        return input -> Double.compare(input.getDouble(), target) >= 0;
    }

    public static ColumnPredicates.Predicate notEqualTo(final int target) {
        return input -> input.getInteger() != target;
    }

    public static ColumnPredicates.Predicate notEqualTo(final long target) {
        return input -> input.getLong() != target;
    }

    public static ColumnPredicates.Predicate notEqualTo(final boolean target) {
        return input -> Boolean.compare(input.getBoolean(), target) != 0;
    }

    public static ColumnPredicates.Predicate notEqualToIgnoreTrailingSpaces(final String target) {
        Preconditions.checkNotNull(target, "target");
        return input -> !equalsIgnoreTrailingSpaces(target, input.getBinary().toStringUsingUTF8());
    }

    public static ColumnPredicates.Predicate notEqualTo(final float target) {
        return input -> Float.compare(input.getFloat(), target) != 0;
    }

    public static ColumnPredicates.Predicate notEqualTo(final double target) {
        return input -> Double.compare(input.getDouble(), target) != 0;
    }

    static boolean equalsIgnoreTrailingSpaces(String value1, String value2) {
        String shorter = value1.length() > value2.length() ? value2 : value1;
        String longer = value1.length() > value2.length() ? value1 : value2;

        int n = shorter.length();

        // Check for non whitespace characters at the end of the longer
        // string, and return false if non-whitespace characters are
        // encountered
        int i;
        for (i = n; i < longer.length(); i++) {
            if (longer.charAt(i) != ' ')
                return false;
        }

        i = 0;
        while (n-- != 0) {
            if (shorter.charAt(i) != longer.charAt(i))
                return false;
            i++;
        }
        return true;
    }
}
