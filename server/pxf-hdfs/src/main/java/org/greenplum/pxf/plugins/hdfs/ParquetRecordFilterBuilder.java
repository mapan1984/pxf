package org.greenplum.pxf.plugins.hdfs;

import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.filter2.compat.FilterCompat;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.filter.ColumnIndexOperand;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.Operand;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.OperatorNode;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.parquet.filter.ColumnPredicates.equalTo;
import static org.apache.parquet.filter.ColumnRecordFilter.column;
import static org.apache.parquet.filter.AndRecordFilter.and;
import static org.apache.parquet.filter.NotRecordFilter.not;
import static org.apache.parquet.filter.OrRecordFilter.or;
import static org.greenplum.pxf.plugins.hdfs.ParquetColumnPredicates.*;


public class ParquetRecordFilterBuilder implements TreeVisitor {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final List<ColumnDescriptor> columnDescriptors;
    private Deque<UnboundRecordFilter> filterQueue;
    private Map<String, Type> fields;

    public ParquetRecordFilterBuilder(List<ColumnDescriptor> columnDescriptors, MessageType schema) {
        this.columnDescriptors = columnDescriptors;
        this.filterQueue = new LinkedList<>();
        populateFields(schema);
    }

    @Override
    public Node before(Node node) {
        return node;
    }

    @Override
    public Node visit(Node node) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();

            if (!operator.isLogical()) {
                processSimpleColumnOperator(operatorNode);
            }
        }
        return node;
    }

    @Override
    public Node after(Node node) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();
            if (operator.isLogical()) {
                processLogicalOperator(operator);
            }
        }
        return node;
    }

    public FilterCompat.Filter getRecordFilter() {
        UnboundRecordFilter predicate = filterQueue.poll();

        if (!filterQueue.isEmpty()) {
            throw new IllegalStateException("Filter queue is not empty after visiting all nodes");
        }

        return predicate != null ? FilterCompat.get(predicate) : FilterCompat.NOOP;
    }

    private void processLogicalOperator(Operator operator) {
        UnboundRecordFilter right = filterQueue.poll();
        UnboundRecordFilter left = null;

        if (operator == Operator.AND || operator == Operator.OR) {
            left = filterQueue.poll();
        }

        switch (operator) {
            case AND:
                filterQueue.push(and(left, right));
                break;
            case OR:
                filterQueue.push(or(left, right));
                break;
            case NOT:
                filterQueue.push(not(right));
                break;
        }
    }

    private void processSimpleColumnOperator(OperatorNode operatorNode) {

        Operator operator = operatorNode.getOperator();
        ColumnIndexOperand columnIndexOperand = operatorNode.getColumnIndexOperand();
        Optional<Operand> valueOperand = operatorNode.getOperand();

        if (!valueOperand.isPresent()) {
            throw new IllegalArgumentException(
                    String.format("Operator %s does not contain an operand", operator));
        }

        int columnIndex = columnIndexOperand.index();
        ColumnDescriptor columnDescriptor = columnDescriptors.get(columnIndex);
        String filterColumnName = columnDescriptor.columnName();
        Operand operand = valueOperand.get();
        Type type = fields.get(filterColumnName);

        UnboundRecordFilter simpleFilter;

        switch (operator) {
            case EQUALS:
                simpleFilter = getEquals(type.getName(),
                        DataType.get(columnDescriptor.columnTypeCode()),
                        type.asPrimitiveType().getPrimitiveTypeName(),
                        operand);
                break;
            case LESS_THAN:
                simpleFilter = getLessThan(type.getName(),
                        DataType.get(columnDescriptor.columnTypeCode()),
                        type.asPrimitiveType().getPrimitiveTypeName(),
                        operand);
                break;
            case GREATER_THAN:
                simpleFilter = getGreaterThan(type.getName(),
                        DataType.get(columnDescriptor.columnTypeCode()),
                        type.asPrimitiveType().getPrimitiveTypeName(),
                        operand);
                break;
            case LESS_THAN_OR_EQUAL:
                simpleFilter = getLessThanOrEqual(type.getName(),
                        DataType.get(columnDescriptor.columnTypeCode()),
                        type.asPrimitiveType().getPrimitiveTypeName(),
                        operand);
                break;
            case GREATER_THAN_OR_EQUAL:
                simpleFilter = getGreaterThanOrEqual(type.getName(),
                        DataType.get(columnDescriptor.columnTypeCode()),
                        type.asPrimitiveType().getPrimitiveTypeName(),
                        operand);
                break;
            case NOT_EQUALS:
                simpleFilter = getNotEquals(type.getName(),
                        DataType.get(columnDescriptor.columnTypeCode()),
                        type.asPrimitiveType().getPrimitiveTypeName(),
                        operand);
                break;
//            case IN:
//                simpleFilter = FilterCompat.get(column(filterColumnName, equalTo(true)));
//                break;
            default:
                throw new UnsupportedOperationException("not supported " + operator);
        }

        filterQueue.push(simpleFilter);
    }

    private UnboundRecordFilter getLessThan(String columnName,
                                            DataType columnType,
                                            PrimitiveType.PrimitiveTypeName parquetType,
                                            Operand operand) {
        String value = operand.toString();

        switch (parquetType) {
            case INT32:
                return column(columnName, lessThan(Integer.parseInt(value)));

            case INT64:
                return column(columnName, lessThan(Long.parseLong(value)));

            case BOOLEAN:
                return column(columnName, lessThan(Boolean.parseBoolean(value)));

            case FLOAT:
                return column(columnName, lessThan(Float.parseFloat(value)));

            case DOUBLE:
                return column(columnName, lessThan(Double.parseDouble(value)));

            default:
                throw new UnsupportedOperationException(
                        String.format("Column %s of type %s is not supported",
                                columnName, operand.getDataType()));
        }
    }

    private UnboundRecordFilter getLessThanOrEqual(String columnName,
                                                   DataType columnType,
                                                   PrimitiveType.PrimitiveTypeName parquetType,
                                                   Operand operand) {
        String value = operand.toString();

        switch (parquetType) {
            case INT32:
                return column(columnName, lessThanOrEqual(Integer.parseInt(value)));

            case INT64:
                return column(columnName, lessThanOrEqual(Long.parseLong(value)));

            case BOOLEAN:
                return column(columnName, lessThanOrEqual(Boolean.parseBoolean(value)));

            case FLOAT:
                return column(columnName, lessThanOrEqual(Float.parseFloat(value)));

            case DOUBLE:
                return column(columnName, lessThanOrEqual(Double.parseDouble(value)));

            default:
                throw new UnsupportedOperationException(
                        String.format("Column %s of type %s is not supported",
                                columnName, operand.getDataType()));
        }
    }

    private UnboundRecordFilter getGreaterThan(String columnName,
                                               DataType columnType,
                                               PrimitiveType.PrimitiveTypeName parquetType,
                                               Operand operand) {
        String value = operand.toString();

        switch (parquetType) {
            case INT32:
                return column(columnName, greaterThan(Integer.parseInt(value)));

            case INT64:
                return column(columnName, greaterThan(Long.parseLong(value)));

            case BOOLEAN:
                return column(columnName, greaterThan(Boolean.parseBoolean(value)));

            case FLOAT:
                return column(columnName, greaterThan(Float.parseFloat(value)));

            case DOUBLE:
                return column(columnName, greaterThan(Double.parseDouble(value)));

            default:
                throw new UnsupportedOperationException(
                        String.format("Column %s of type %s is not supported",
                                columnName, operand.getDataType()));
        }
    }

    private UnboundRecordFilter getGreaterThanOrEqual(String columnName,
                                                      DataType columnType,
                                                      PrimitiveType.PrimitiveTypeName parquetType,
                                                      Operand operand) {
        String value = operand.toString();

        switch (parquetType) {
            case INT32:
                return column(columnName, greaterThanOrEqual(Integer.parseInt(value)));

            case INT64:
                return column(columnName, greaterThanOrEqual(Long.parseLong(value)));

            case BOOLEAN:
                return column(columnName, greaterThanOrEqual(Boolean.parseBoolean(value)));

            case FLOAT:
                return column(columnName, greaterThanOrEqual(Float.parseFloat(value)));

            case DOUBLE:
                return column(columnName, greaterThanOrEqual(Double.parseDouble(value)));

            default:
                throw new UnsupportedOperationException(
                        String.format("Column %s of type %s is not supported",
                                columnName, operand.getDataType()));
        }
    }

    private UnboundRecordFilter getEquals(String columnName,
                                          DataType columnType,
                                          PrimitiveType.PrimitiveTypeName parquetType,
                                          Operand operand) {
        String value = operand.toString();
        switch (parquetType) {
            case INT32:
                return column(columnName, equalTo(Integer.parseInt(value)));

            case INT64:
                return column(columnName, equalTo(Long.parseLong(value)));

            case BOOLEAN:
                return column(columnName, equalTo(Boolean.parseBoolean(value)));

            case BINARY:
                return column(columnName, equalToIgnoreTrailingSpaces(value));

            case FLOAT:
                return column(columnName, equalTo(Float.parseFloat(value)));

            case DOUBLE:
                return column(columnName, equalTo(Double.parseDouble(value)));

//            case INT96:
//                return column(columnName, equalTo(Double.parseDouble(value)));

//            case FIXED_LEN_BYTE_ARRAY:
//                BigDecimal bigDecimal = new BigDecimal(value);
//                byte fillByte = (byte) (bigDecimal.signum() < 0 ? 0xFF : 0x00);
//                byte[] unscaled = bigDecimal.unscaledValue().toByteArray();
//                byte[] bytes = new byte[16];
//                int offset = bytes.length - unscaled.length;
//                for (int i = 0; i < bytes.length; i += 1) {
//                    bytes[i] = (i < offset) ? fillByte : unscaled[i - offset];
//                }
//                return column(columnName, equalTo(Binary.fromReusedByteArray(bytes)));

            default:
                throw new UnsupportedOperationException(
                        String.format("Column %s of type %s is not supported",
                                columnName, operand.getDataType()));
        }
    }

    private UnboundRecordFilter getNotEquals(String columnName,
                                             DataType columnType,
                                             PrimitiveType.PrimitiveTypeName parquetType,
                                             Operand operand) {
        String value = operand.toString();
        switch (parquetType) {
            case INT32:
                return column(columnName, notEqualTo(Integer.parseInt(value)));

            case INT64:
                return column(columnName, notEqualTo(Long.parseLong(value)));

            case BOOLEAN:
                return column(columnName, notEqualTo(Boolean.parseBoolean(value)));

            case BINARY:
                return column(columnName, notEqualToIgnoreTrailingSpaces(value));

            case FLOAT:
                return column(columnName, notEqualTo(Float.parseFloat(value)));

            case DOUBLE:
                return column(columnName, notEqualTo(Double.parseDouble(value)));

//            case INT96:
//                return column(columnName, equalTo(Double.parseDouble(value)));

//            case FIXED_LEN_BYTE_ARRAY:
//                BigDecimal bigDecimal = new BigDecimal(value);
//                byte fillByte = (byte) (bigDecimal.signum() < 0 ? 0xFF : 0x00);
//                byte[] unscaled = bigDecimal.unscaledValue().toByteArray();
//                byte[] bytes = new byte[16];
//                int offset = bytes.length - unscaled.length;
//                for (int i = 0; i < bytes.length; i += 1) {
//                    bytes[i] = (i < offset) ? fillByte : unscaled[i - offset];
//                }
//                return column(columnName, equalTo(Binary.fromReusedByteArray(bytes)));

            default:
                throw new UnsupportedOperationException(
                        String.format("Column %s of type %s is not supported",
                                columnName, operand.getDataType()));
        }
    }

    private void populateFields(MessageType schema) {
        fields = new HashMap<>(schema.getFieldCount() * 2);
        // We need to add the original name and lower cased name to
        // the map to support mixed case where in GPDB the column name
        // was created with quotes i.e "mIxEd CaSe". When quotes are not
        // used to create a table in GPDB, the name of the column will
        // always come in lower-case
        schema.getFields().forEach(t -> {
            String columnName = t.getName();
            fields.put(columnName, t);
            fields.put(columnName.toLowerCase(), t);
        });
    }
}
