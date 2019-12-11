package org.greenplum.pxf.plugins.hdfs;

import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.filter2.compat.FilterCompat;

import org.apache.parquet.schema.MessageType;
import org.greenplum.pxf.api.filter.*;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static org.apache.parquet.filter.ColumnPredicates.equalTo;
import static org.apache.parquet.filter.ColumnRecordFilter.column;
import static org.apache.parquet.filter.AndRecordFilter.and;
import static org.apache.parquet.filter.NotRecordFilter.not;
import static org.apache.parquet.filter.OrRecordFilter.or;


public class ParquetRecordTreeVisitor implements TreeVisitor {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final List<ColumnDescriptor> columnDescriptors;
    private final MessageType schema;
    private Deque<UnboundRecordFilter> filterQueue;

    public ParquetRecordTreeVisitor(List<ColumnDescriptor> columnDescriptors, MessageType schema) {
        this.columnDescriptors = columnDescriptors;
        this.filterQueue = new LinkedList<>();
        this.schema = schema;
    }

    @Override
    public void before(Node node) {
    }

    @Override
    public void visit(Node node) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();

            if (operator.isLogical()) {
                processLogicalOperator(operator);
            } else {
                processSimpleColumnOperator(operatorNode);
            }
        }
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

    @Override
    public void after(Node node) {
    }

    public FilterCompat.Filter getRecordFilter() {
        UnboundRecordFilter predicate = filterQueue.poll();

        if (!filterQueue.isEmpty()) {
            throw new IllegalStateException("Filter queue is not empty after visiting all nodes");
        }

        return predicate != null ? FilterCompat.get(predicate) : FilterCompat.NOOP;
    }

    private void processSimpleColumnOperator(OperatorNode operatorNode) {

        Operator operator = operatorNode.getOperator();
        ColumnIndexOperand columnIndexOperand = operatorNode.getColumnIndexOperator();
        Optional<Operand> valueOperand = operatorNode.getOperand();

        if (!valueOperand.isPresent()) {
            throw new IllegalArgumentException(
                    String.format("Operator %s does not contain an operand", operator));
        }


        int columnIndex = columnIndexOperand.index();
        ColumnDescriptor columnDescriptor = columnDescriptors.get(columnIndex);
        String filterColumnName = columnDescriptor.columnName();
        Operand operand = valueOperand.get();


//        Operators.Column operatorColumn = getColumn(filterColumnName, operand);

//        FilterCompat.Filter simpleFilter;
        switch (operator) {
            case EQUALS:
                filterQueue.push(getEqual(filterColumnName, DataType.get(columnDescriptor.columnTypeCode()), operand));
                break;
//            case LESS_THAN:
//                simpleFilter = lt(operatorColumn, 10);
//                break;
//            case GREATER_THAN:
//                simpleFilter = FilterCompat.get(helper(lt(), operatorColumn, 10));
//                break;
//            case LESS_THAN_OR_EQUAL:
//                simpleFilter = FilterCompat.get(column(filterColumnName, ltEq(true)));
//                break;
//            case GREATER_THAN_OR_EQUAL:
//                simpleFilter = FilterCompat.get(column(filterColumnName, gtEq(true)));
//                break;
//            case NOT_EQUALS:
//                simpleFilter = FilterCompat.get(column(filterColumnName, notEq(true)));
//                break;
//            case IN:
//                simpleFilter = FilterCompat.get(column(filterColumnName, equalTo(true)));
//                break;
            default:
                throw new UnsupportedOperationException("not supported");
        }
    }

    private UnboundRecordFilter getEqual(String columnName, DataType type, Operand operand) {
        String value = operand.toString();
        switch (type) {
            case INTEGER:
                return column(columnName, equalTo(Integer.parseInt(value)));

            case BIGINT:
                return column(columnName, equalTo(Long.parseLong(value)));

            case BOOLEAN:
                return column(columnName, equalTo(Boolean.parseBoolean(value)));

            default:
                throw new UnsupportedOperationException(
                        String.format("Column %s of type %s is not supported",
                                columnName, operand.getDataType()));
        }
    }
}
