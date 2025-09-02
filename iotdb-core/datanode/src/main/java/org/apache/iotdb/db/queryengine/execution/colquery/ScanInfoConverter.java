/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.colquery;

import org.apache.iotdb.db.queryengine.execution.colquery.colservice.Column;
import org.apache.iotdb.db.queryengine.execution.colquery.colservice.ColumnData;
import org.apache.iotdb.db.queryengine.execution.colquery.colservice.ScanInfo;
import org.apache.iotdb.db.queryengine.execution.colquery.colservice.TimeColumn;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;

import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.List;

/**
 * Converter utility class for transforming between various column-oriented data structures.
 *
 * <p>This class provides bidirectional conversion methods between:
 * <ul>
 *   <li>{@link ScanInfo} - Thrift-generated class used for network communication</li>
 *   <li>{@link QueryStateManager.ScanStates} - Internal state management class</li>
 *   <li>{@link TimeColumn} - Thrift-generated time column representation</li>
 *   <li>{@link ColumnData} - Thrift-generated column data representation</li>
 *   <li>{@link TsBlock} - TSFile block data structure</li>
 * </ul>
 *
 * <p>Note: The timestamp field in ScanStates is not processed during conversion,
 * as specified in the requirements.
 */
public class ScanInfoConverter {

    /**
     * Convert ScanInfo to ScanStates.
     *
     * <p>This method transforms a Thrift ScanInfo object to the internal ScanStates representation.
     * The scanTimestamp in ScanStates is set to 0 (default value) since timestamp processing
     * is not required.
     *
     * @param scanInfo the ScanInfo object to convert
     * @return a new ScanStates object with converted values, or null if input is null
     */
    public static QueryStateManager.ScanStates convertToScanStates(ScanInfo scanInfo) {
        if (scanInfo == null) {
            return null;
        }

        return new QueryStateManager.ScanStates(
                0L,                              // scanTimestamp - not processed as per requirement
                scanInfo.getOffset(),            // offset
                scanInfo.isIsCloudEqual(),       // isCouldEqual (note the field name mapping)
                scanInfo.isIsInnerJoin(),        // isInnerJoin
                scanInfo.isIsFullOuterJoin()     // isFullOuterJoin
        );
    }

    /**
     * Convert ScanStates to ScanInfo.
     *
     * <p>This method transforms an internal ScanStates object to a Thrift ScanInfo representation
     * for network communication. The seriesPath field in ScanInfo is set to null since it's not
     * available in ScanStates and must be provided separately if needed.
     *
     * @param scanStates the ScanStates object to convert
     * @return a new ScanInfo object with converted values, or null if input is null
     */
    public static ScanInfo convertToScanInfo(QueryStateManager.ScanStates scanStates) {
        if (scanStates == null) {
            return null;
        }

        ScanInfo scanInfo = new ScanInfo();
        scanInfo.setOffset(scanStates.getOffset());
        scanInfo.setIsCloudEqual(scanStates.isCouldEqual());  // Note the field name mapping
        scanInfo.setIsInnerJoin(scanStates.isInnerJoin());
        scanInfo.setIsFullOuterJoin(scanStates.isFullOuterJoin());
        // seriesPath is set to null - must be provided separately if needed
        scanInfo.setSeriesPath(null);

        return scanInfo;
    }

    /**
     * Convert ScanStates to ScanInfo with specified series path.
     *
     * <p>This method is similar to {@link #convertToScanInfo(QueryStateManager.ScanStates)}
     * but allows setting the seriesPath field during conversion.
     *
     * @param scanStates the ScanStates object to convert
     * @param seriesPath the series path to set in the ScanInfo object
     * @return a new ScanInfo object with converted values and specified series path,
     *         or null if scanStates is null
     */
    public static ScanInfo convertToScanInfo(QueryStateManager.ScanStates scanStates, String seriesPath) {
        if (scanStates == null) {
            return null;
        }

        ScanInfo scanInfo = convertToScanInfo(scanStates);
        if (scanInfo != null) {
            scanInfo.setSeriesPath(seriesPath);
        }

        return scanInfo;
    }

    /**
     * Update existing ScanStates with values from ScanInfo.
     *
     * <p>This method updates an existing ScanStates object with values from a ScanInfo object,
     * preserving the original scanTimestamp value.
     *
     * @param scanStates the ScanStates object to update (must not be null)
     * @param scanInfo the ScanInfo object containing new values (must not be null)
     * @throws IllegalArgumentException if either parameter is null
     */
    public static void updateScanStatesFromScanInfo(QueryStateManager.ScanStates scanStates, ScanInfo scanInfo) {
        if (scanStates == null) {
            throw new IllegalArgumentException("scanStates cannot be null");
        }
        if (scanInfo == null) {
            throw new IllegalArgumentException("scanInfo cannot be null");
        }

        scanStates.setOffset(scanInfo.getOffset());
        scanStates.setCouldEqual(scanInfo.isIsCloudEqual());
        scanStates.setInnerJoin(scanInfo.isIsInnerJoin());
        scanStates.setFullOuterJoin(scanInfo.isIsFullOuterJoin());
        // scanTimestamp is preserved (not updated from ScanInfo)
    }

    /**
     * Update existing ScanInfo with values from ScanStates.
     *
     * <p>This method updates an existing ScanInfo object with values from a ScanStates object,
     * preserving the original seriesPath value.
     *
     * @param scanInfo the ScanInfo object to update (must not be null)
     * @param scanStates the ScanStates object containing new values (must not be null)
     * @throws IllegalArgumentException if either parameter is null
     */
    public static void updateScanInfoFromScanStates(ScanInfo scanInfo, QueryStateManager.ScanStates scanStates) {
        if (scanInfo == null) {
            throw new IllegalArgumentException("scanInfo cannot be null");
        }
        if (scanStates == null) {
            throw new IllegalArgumentException("scanStates cannot be null");
        }

        scanInfo.setOffset(scanStates.getOffset());
        scanInfo.setIsCloudEqual(scanStates.isCouldEqual());
        scanInfo.setIsInnerJoin(scanStates.isInnerJoin());
        scanInfo.setIsFullOuterJoin(scanStates.isFullOuterJoin());
        // seriesPath is preserved (not updated from ScanStates)
    }

    /**
     * Create a deep copy of ScanInfo.
     *
     * @param source the ScanInfo to copy
     * @return a new ScanInfo object with the same values, or null if source is null
     */
    public static ScanInfo copyScanInfo(ScanInfo source) {
        if (source == null) {
            return null;
        }

        return new ScanInfo(
                source.getOffset(),
                source.getSeriesPath(),
                source.isIsCloudEqual(),
                source.isIsInnerJoin(),
                source.isIsFullOuterJoin()
        );
    }

    /**
     * Check if two ScanInfo objects have equivalent scan state values.
     *
     * <p>This method compares the scan-related fields (offset, isCloudEqual, isInnerJoin,
     * isFullOuterJoin) but ignores the seriesPath field.
     *
     * @param info1 first ScanInfo object
     * @param info2 second ScanInfo object
     * @return true if both objects have equivalent scan state values, false otherwise
     */
    public static boolean hasSameScanState(ScanInfo info1, ScanInfo info2) {
        if (info1 == null && info2 == null) {
            return true;
        }
        if (info1 == null || info2 == null) {
            return false;
        }

        return info1.getOffset() == info2.getOffset()
                && info1.isIsCloudEqual() == info2.isIsCloudEqual()
                && info1.isIsInnerJoin() == info2.isIsInnerJoin()
                && info1.isIsFullOuterJoin() == info2.isIsFullOuterJoin();
    }

    /**
     * Check if ScanInfo and ScanStates have equivalent scan state values.
     *
     * <p>This method compares the scan-related fields between ScanInfo and ScanStates objects.
     * The timestamp field in ScanStates is ignored during comparison.
     *
     * @param scanInfo the ScanInfo object
     * @param scanStates the ScanStates object
     * @return true if both objects have equivalent scan state values, false otherwise
     */
    public static boolean hasSameScanState(ScanInfo scanInfo, QueryStateManager.ScanStates scanStates) {
        if (scanInfo == null && scanStates == null) {
            return true;
        }
        if (scanInfo == null || scanStates == null) {
            return false;
        }

        return scanInfo.getOffset() == scanStates.getOffset()
                && scanInfo.isIsCloudEqual() == scanStates.isCouldEqual()
                && scanInfo.isIsInnerJoin() == scanStates.isInnerJoin()
                && scanInfo.isIsFullOuterJoin() == scanStates.isFullOuterJoin();
    }

    // ===================================================================================
    //                       TimeColumn and ColumnData Conversion Methods
    // ===================================================================================

    /**
     * Create a simple TimeColumn from time values list.
     *
     * @param timeValues the list of time values
     * @return Thrift TimeColumn, or null if input is null
     */
    public static org.apache.iotdb.db.queryengine.execution.colquery.colservice.TimeColumn createTimeColumn(List<Long> timeValues) {
        if (timeValues == null || timeValues.isEmpty()) {
            return null;
        }

        return new org.apache.iotdb.db.queryengine.execution.colquery.colservice.TimeColumn(timeValues, timeValues.size());
    }

    /**
     * Extract time values from TimeColumn.
     *
     * @param timeColumn the TimeColumn to extract from
     * @return list of time values, or empty list if input is null
     */
    public static List<Long> extractTimeValues(
            org.apache.iotdb.db.queryengine.execution.colquery.colservice.TimeColumn timeColumn) {
        if (timeColumn == null || timeColumn.getValues() == null) {
            return new ArrayList<>();
        }

        return new ArrayList<>(timeColumn.getValues());
    }

    /**
     * Create ColumnData with integer values.
     *
     * @param intValues the list of integer values
     * @return ColumnData with integer values
     */
    public static ColumnData createIntColumnData(List<Integer> intValues) {
        return ColumnData.intValues(intValues);
    }

    /**
     * Create ColumnData with long values.
     *
     * @param longValues the list of long values
     * @return ColumnData with long values
     */
    public static ColumnData createLongColumnData(List<Long> longValues) {
        return ColumnData.longValues(longValues);
    }

    /**
     * Create ColumnData with double values.
     *
     * @param doubleValues the list of double values
     * @return ColumnData with double values
     */
    public static ColumnData createDoubleColumnData(List<Double> doubleValues) {
        return ColumnData.doubleValues(doubleValues);
    }

    /**
     * Create ColumnData with string values.
     *
     * @param stringValues the list of string values
     * @return ColumnData with string values
     */
    public static ColumnData createStringColumnData(List<String> stringValues) {
        return ColumnData.stringValues(stringValues);
    }

    /**
     * Create ColumnData with boolean values.
     *
     * @param boolValues the list of boolean values
     * @return ColumnData with boolean values
     */
    public static ColumnData createBooleanColumnData(List<Boolean> boolValues) {
        return ColumnData.boolValues(boolValues);
    }

    /**
     * Extract values from ColumnData based on its type.
     *
     * @param columnData the ColumnData to extract from
     * @param expectedType the expected data type
     * @return list of values as Objects, or empty list if no matching type
     */
    public static List<Object> extractColumnValues(ColumnData columnData, TSDataType expectedType) {
        List<Object> values = new ArrayList<>();

        if (columnData == null || expectedType == null) {
            return values;
        }

        switch (expectedType) {
            case INT32:
                if (columnData.isSetIntValues()) {
                    values.addAll(columnData.getIntValues());
                }
                break;
            case INT64:
                if (columnData.isSetLongValues()) {
                    values.addAll(columnData.getLongValues());
                }
                break;
            case DOUBLE:
            case FLOAT:
                if (columnData.isSetDoubleValues()) {
                    values.addAll(columnData.getDoubleValues());
                }
                break;
            case TEXT:
                if (columnData.isSetStringValues()) {
                    values.addAll(columnData.getStringValues());
                }
                break;
            case BOOLEAN:
                if (columnData.isSetBoolValues()) {
                    values.addAll(columnData.getBoolValues());
                }
                break;
            default:
                break;
        }

        return values;
    }

    /**
     * Check if TimeColumn contains valid time data.
     *
     * @param timeColumn the TimeColumn to validate
     * @return true if valid, false otherwise
     */
    public static boolean isValidTimeColumn(
            org.apache.iotdb.db.queryengine.execution.colquery.colservice.TimeColumn timeColumn) {
        return timeColumn != null
                && timeColumn.getValues() != null
                && !timeColumn.getValues().isEmpty()
                && timeColumn.getPositionCount() > 0
                && timeColumn.getPositionCount() == timeColumn.getValues().size();
    }

    /**
     * Check if ColumnData contains valid data for the specified type.
     *
     * @param columnData the ColumnData to validate
     * @param expectedType the expected data type
     * @return true if valid, false otherwise
     */
    public static boolean isValidColumnData(ColumnData columnData, TSDataType expectedType) {
        if (columnData == null || expectedType == null) {
            return false;
        }

        switch (expectedType) {
            case INT32:
                return columnData.isSetIntValues();
            case INT64:
                return columnData.isSetLongValues();
            case DOUBLE:
            case FLOAT:
                return columnData.isSetDoubleValues();
            case TEXT:
                return columnData.isSetStringValues();
            case BOOLEAN:
                return columnData.isSetBoolValues();
            default:
                return false;
        }
    }

    /**
     * Get the data type of the ColumnData based on which field is set.
     *
     * @param columnData the ColumnData to check
     * @return TSDataType of the active field, or null if no field is set
     */
    public static TSDataType getColumnDataType(ColumnData columnData) {
        if (columnData == null) {
            return null;
        }

        if (columnData.isSetIntValues()) {
            return TSDataType.INT32;
        } else if (columnData.isSetLongValues()) {
            return TSDataType.INT64;
        } else if (columnData.isSetDoubleValues()) {
            return TSDataType.DOUBLE;
        } else if (columnData.isSetStringValues()) {
            return TSDataType.TEXT;
        } else if (columnData.isSetBoolValues()) {
            return TSDataType.BOOLEAN;
        }

        return null;
    }

    // ===================================================================================
    //                         TsBlock Conversion Methods
    // ===================================================================================

    /**
     * Convert TsBlock to TimeColumn and List of Column.
     *
     * @param tsBlock the TsBlock to convert
     * @return a TsBlockColumns object containing TimeColumn and List of Column, or null if input is null
     */
    public static TsBlockColumns convertTsBlockToColumns(TsBlock tsBlock) {
        if (tsBlock == null) {
            return null;
        }

        // Extract TimeColumn from TsBlock
        org.apache.tsfile.read.common.block.column.TimeColumn tsFileTimeColumn = tsBlock.getTimeColumn();
        TimeColumn timeColumn = convertTsFileTimeColumnToColServiceTimeColumn(tsFileTimeColumn);

        // Extract value columns from TsBlock
        List<Column> valueColumns = new ArrayList<>();
        int columnCount = tsBlock.getValueColumnCount();

        for (int i = 0; i < columnCount; i++) {
            org.apache.tsfile.block.column.Column tsFileColumn = tsBlock.getColumn(i);
            Column colServiceColumn = convertTsFileColumnToColServiceColumn(tsFileColumn);
            if (colServiceColumn != null) {
                valueColumns.add(colServiceColumn);
            }
        }

        return new TsBlockColumns(timeColumn, valueColumns);
    }

    /**
     * Convert TimeColumn and List of Column back to TsBlock.
     *
     * @param timeColumn the TimeColumn containing time values
     * @param valueColumns the List of Column containing value data
     * @param valueColumnTypes the TSDataType list for the value columns
     * @return TsBlock created from the input data, or null if timeColumn is null
     */
    public static TsBlock convertColumnsToTsBlock(TimeColumn timeColumn, List<Column> valueColumns, List<TSDataType> valueColumnTypes) {
        if (timeColumn == null || timeColumn.getValues() == null) {
            return null;
        }

        if (valueColumnTypes == null) {
            valueColumnTypes = new ArrayList<>();
        }

        // Create TsBlockBuilder with column types
        TsBlockBuilder builder = new TsBlockBuilder(valueColumnTypes);

        int positionCount = timeColumn.getPositionCount();
        List<Long> timeValues = timeColumn.getValues();

        // Add data row by row
        for (int rowIndex = 0; rowIndex < positionCount; rowIndex++) {
            // Add time value
            builder.getTimeColumnBuilder().writeLong(timeValues.get(rowIndex));

            // Add value columns
            if (valueColumns != null) {
                for (int colIndex = 0; colIndex < valueColumns.size() && colIndex < valueColumnTypes.size(); colIndex++) {
                    Column column = valueColumns.get(colIndex);
                    TSDataType dataType = valueColumnTypes.get(colIndex);

                    if (column != null && column.getData() != null) {
                        writeColumnDataToBuilder(builder.getColumnBuilder(colIndex), column.getData(), dataType, rowIndex);
                    } else {
                        builder.getColumnBuilder(colIndex).appendNull();
                    }
                }
            }

            builder.declarePosition();
        }

        return builder.build();
    }

    /**
     * Helper method to convert TsFile TimeColumn to colservice TimeColumn.
     */
    private static TimeColumn convertTsFileTimeColumnToColServiceTimeColumn(org.apache.tsfile.read.common.block.column.TimeColumn tsFileTimeColumn) {
        if (tsFileTimeColumn == null) {
            return null;
        }

        List<Long> timeValues = new ArrayList<>();
        int positionCount = tsFileTimeColumn.getPositionCount();

        for (int i = 0; i < positionCount; i++) {
            timeValues.add(tsFileTimeColumn.getLong(i));
        }

        return new TimeColumn(timeValues, positionCount);
    }

    /**
     * Helper method to convert TsFile Column to colservice Column.
     */
    private static Column convertTsFileColumnToColServiceColumn(org.apache.tsfile.block.column.Column tsFileColumn) {
        if (tsFileColumn == null) {
            return null;
        }

        TSDataType dataType = tsFileColumn.getDataType();
        int positionCount = tsFileColumn.getPositionCount();
        ColumnData columnData = createColumnDataFromTsFileColumn(tsFileColumn, dataType, positionCount);

        if (columnData == null) {
            return null;
        }

        Column column = new Column();
        column.setData(columnData);
        return column;
    }

    /**
     * Helper method to create ColumnData from TsFile Column.
     */
    private static ColumnData createColumnDataFromTsFileColumn(org.apache.tsfile.block.column.Column tsFileColumn, TSDataType dataType, int positionCount) {
        switch (dataType) {
            case INT32:
                List<Integer> intValues = new ArrayList<>();
                for (int i = 0; i < positionCount; i++) {
                    if (tsFileColumn.isNull(i)) {
                        intValues.add(null);
                    } else {
                        intValues.add(tsFileColumn.getInt(i));
                    }
                }
                return ColumnData.intValues(intValues);

            case INT64:
                List<Long> longValues = new ArrayList<>();
                for (int i = 0; i < positionCount; i++) {
                    if (tsFileColumn.isNull(i)) {
                        longValues.add(null);
                    } else {
                        longValues.add(tsFileColumn.getLong(i));
                    }
                }
                return ColumnData.longValues(longValues);

            case DOUBLE:
            case FLOAT:
                List<Double> doubleValues = new ArrayList<>();
                for (int i = 0; i < positionCount; i++) {
                    if (tsFileColumn.isNull(i)) {
                        doubleValues.add(null);
                    } else {
                        doubleValues.add(dataType == TSDataType.FLOAT ?
                                (double) tsFileColumn.getFloat(i) : tsFileColumn.getDouble(i));
                    }
                }
                return ColumnData.doubleValues(doubleValues);

            case TEXT:
                List<String> stringValues = new ArrayList<>();
                for (int i = 0; i < positionCount; i++) {
                    if (tsFileColumn.isNull(i)) {
                        stringValues.add(null);
                    } else {
                        stringValues.add(tsFileColumn.getBinary(i).toString());
                    }
                }
                return ColumnData.stringValues(stringValues);

            case BOOLEAN:
                List<Boolean> boolValues = new ArrayList<>();
                for (int i = 0; i < positionCount; i++) {
                    if (tsFileColumn.isNull(i)) {
                        boolValues.add(null);
                    } else {
                        boolValues.add(tsFileColumn.getBoolean(i));
                    }
                }
                return ColumnData.boolValues(boolValues);

            default:
                return null;
        }
    }

    /**
     * Helper method to write ColumnData to TsBlockBuilder.
     */
    private static void writeColumnDataToBuilder(org.apache.tsfile.block.column.ColumnBuilder columnBuilder, ColumnData columnData, TSDataType dataType, int rowIndex) {
        if (columnData == null) {
            columnBuilder.appendNull();
            return;
        }

        try {
            switch (dataType) {
                case INT32:
                    if (columnData.isSetIntValues()) {
                        List<Integer> intValues = columnData.getIntValues();
                        if (rowIndex < intValues.size()) {
                            Integer value = intValues.get(rowIndex);
                            if (value == null) {
                                columnBuilder.appendNull();
                            } else {
                                columnBuilder.writeInt(value);
                            }
                        } else {
                            columnBuilder.appendNull();
                        }
                    } else {
                        columnBuilder.appendNull();
                    }
                    break;

                case INT64:
                    if (columnData.isSetLongValues()) {
                        List<Long> longValues = columnData.getLongValues();
                        if (rowIndex < longValues.size()) {
                            Long value = longValues.get(rowIndex);
                            if (value == null) {
                                columnBuilder.appendNull();
                            } else {
                                columnBuilder.writeLong(value);
                            }
                        } else {
                            columnBuilder.appendNull();
                        }
                    } else {
                        columnBuilder.appendNull();
                    }
                    break;

                case DOUBLE:
                case FLOAT:
                    if (columnData.isSetDoubleValues()) {
                        List<Double> doubleValues = columnData.getDoubleValues();
                        if (rowIndex < doubleValues.size()) {
                            Double value = doubleValues.get(rowIndex);
                            if (value == null) {
                                columnBuilder.appendNull();
                            } else {
                                if (dataType == TSDataType.FLOAT) {
                                    columnBuilder.writeFloat(value.floatValue());
                                } else {
                                    columnBuilder.writeDouble(value);
                                }
                            }
                        } else {
                            columnBuilder.appendNull();
                        }
                    } else {
                        columnBuilder.appendNull();
                    }
                    break;

                case TEXT:
                    if (columnData.isSetStringValues()) {
                        List<String> stringValues = columnData.getStringValues();
                        if (rowIndex < stringValues.size()) {
                            String value = stringValues.get(rowIndex);
                            if (value == null) {
                                columnBuilder.appendNull();
                            } else {
                                columnBuilder.writeBinary(new org.apache.tsfile.utils.Binary(value.getBytes(StandardCharsets.UTF_8)));
                            }
                        } else {
                            columnBuilder.appendNull();
                        }
                    } else {
                        columnBuilder.appendNull();
                    }
                    break;

                case BOOLEAN:
                    if (columnData.isSetBoolValues()) {
                        List<Boolean> boolValues = columnData.getBoolValues();
                        if (rowIndex < boolValues.size()) {
                            Boolean value = boolValues.get(rowIndex);
                            if (value == null) {
                                columnBuilder.appendNull();
                            } else {
                                columnBuilder.writeBoolean(value);
                            }
                        } else {
                            columnBuilder.appendNull();
                        }
                    } else {
                        columnBuilder.appendNull();
                    }
                    break;

                default:
                    columnBuilder.appendNull();
                    break;
            }
        } catch (Exception e) {
            // If any error occurs, append null
            columnBuilder.appendNull();
        }
    }

    /**
     * Data class to hold the result of TsBlock to Columns conversion.
     */
    public static class TsBlockColumns {
        private final TimeColumn timeColumn;
        private final List<Column> valueColumns;

        public TsBlockColumns(TimeColumn timeColumn, List<Column> valueColumns) {
            this.timeColumn = timeColumn;
            this.valueColumns = valueColumns;
        }

        public TimeColumn getTimeColumn() {
            return timeColumn;
        }

        public List<Column> getValueColumns() {
            return valueColumns;
        }
    }

    // Private constructor to prevent instantiation
    private ScanInfoConverter() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }
}