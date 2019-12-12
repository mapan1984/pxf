package org.greenplum.pxf.plugins.hdfs;

import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.filter.ColumnPredicates;

public class ParquetColumnPredicates {

    public static ColumnPredicates.Predicate equalToIgnoreTrailingSpaces(final String target) {
        Preconditions.checkNotNull(target, "target");
        return new ColumnPredicates.Predicate() {
            @Override
            public boolean apply(ColumnReader input) {
                return equalsIgnoreTrailingSpaces(target, input.getBinary().toStringUsingUTF8());
            }

            private boolean equalsIgnoreTrailingSpaces(String value1, String value2) {
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
        };
    }
}
