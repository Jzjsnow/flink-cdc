/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.common.utils;

import org.apache.flink.table.types.logical.LogicalTypeRoot;

import com.ververica.cdc.common.annotation.PublicEvolving;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.AlterColumnTypeEvent;
import com.ververica.cdc.common.event.DropColumnEvent;
import com.ververica.cdc.common.event.RenameColumnEvent;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataType;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Utils for {@link Schema} to perform the ability of evolution. */
@PublicEvolving
public class SchemaUtils {

    /**
     * create a list of {@link RecordData.FieldGetter} from given {@link Schema} to get Object from
     * RecordData.
     */
    public static List<RecordData.FieldGetter> createFieldGetters(Schema schema) {
        List<RecordData.FieldGetter> fieldGetters = new ArrayList<>(schema.getColumns().size());
        for (int i = 0; i < schema.getColumns().size(); i++) {
            fieldGetters.add(RecordData.createFieldGetter(schema.getColumns().get(i).getType(), i));
        }
        return fieldGetters;
    }

    /** apply SchemaChangeEvent to the old schema and return the schema after changing. */
    public static Schema applySchemaChangeEvent(Schema schema, SchemaChangeEvent event) {
        if (event instanceof AddColumnEvent) {
            return applyAddColumnEvent((AddColumnEvent) event, schema);
        } else if (event instanceof DropColumnEvent) {
            return applyDropColumnEvent((DropColumnEvent) event, schema);
        } else if (event instanceof RenameColumnEvent) {
            return applyRenameColumnEvent((RenameColumnEvent) event, schema);
        } else if (event instanceof AlterColumnTypeEvent) {
            return applyAlterColumnTypeEvent((AlterColumnTypeEvent) event, schema);
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported schema change event type \"%s\"",
                            event.getClass().getCanonicalName()));
        }
    }

    /**
     * apply SchemaChangeEvent to both the old schema/sink schema and return the schema after
     * changing. This method is useful when the schema of the source and sink tables are not
     * identical and the data types of the sink table columns are needed during the serialization
     * phase, especially between timestamp and timestamp_ltz
     */
    public static Schema applySchemaChangeEvent(
            Schema schema, SchemaChangeEvent event, List<LogicalTypeRoot> sinkDataTypeRoots) {
        if (event instanceof AddColumnEvent) {
            return applyAddColumnEvent((AddColumnEvent) event, schema, sinkDataTypeRoots);
        } else if (event instanceof DropColumnEvent) {
            return applyDropColumnEvent((DropColumnEvent) event, schema, sinkDataTypeRoots);
        } else if (event instanceof RenameColumnEvent) {
            return applyRenameColumnEvent((RenameColumnEvent) event, schema);
            // Renaming columns will not change the data type, so sinkDataTypeRoots does not need to
            // be passed in.
        } else if (event instanceof AlterColumnTypeEvent) {
            return applyAlterColumnTypeEvent(
                    (AlterColumnTypeEvent) event, schema, sinkDataTypeRoots);
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported schema change event type \"%s\"",
                            event.getClass().getCanonicalName()));
        }
    }

    private static Schema applyAddColumnEvent(AddColumnEvent event, Schema oldSchema) {
        LinkedList<Column> columns = new LinkedList<>(oldSchema.getColumns());
        for (AddColumnEvent.ColumnWithPosition columnWithPosition : event.getAddedColumns()) {
            if (columns.contains(columnWithPosition.getAddColumn())) {
                throw new IllegalArgumentException(
                        columnWithPosition.getAddColumn().getName()
                                + " of AddColumnEvent is already existed");
            }
            switch (columnWithPosition.getPosition()) {
                case FIRST:
                    {
                        columns.addFirst(columnWithPosition.getAddColumn());
                        break;
                    }
                case LAST:
                    {
                        columns.addLast(columnWithPosition.getAddColumn());
                        break;
                    }
                case BEFORE:
                    {
                        Preconditions.checkNotNull(
                                columnWithPosition.getExistingColumn(),
                                "existingColumn could not be null in BEFORE type AddColumnEvent");
                        List<String> columnNames =
                                columns.stream().map(Column::getName).collect(Collectors.toList());
                        int index =
                                columnNames.indexOf(
                                        columnWithPosition.getExistingColumn().getName());
                        if (index < 0) {
                            throw new IllegalArgumentException(
                                    columnWithPosition.getExistingColumn().getName()
                                            + " of AddColumnEvent is not existed");
                        }
                        columns.add(index, columnWithPosition.getAddColumn());
                        break;
                    }
                case AFTER:
                    {
                        Preconditions.checkNotNull(
                                columnWithPosition.getExistingColumn(),
                                "existingColumn could not be null in AFTER type AddColumnEvent");
                        List<String> columnNames =
                                columns.stream().map(Column::getName).collect(Collectors.toList());
                        int index =
                                columnNames.indexOf(
                                        columnWithPosition.getExistingColumn().getName());
                        if (index < 0) {
                            throw new IllegalArgumentException(
                                    columnWithPosition.getExistingColumn().getName()
                                            + " of AddColumnEvent is not existed");
                        }
                        columns.add(index + 1, columnWithPosition.getAddColumn());
                        break;
                    }
            }
        }
        return oldSchema.copy(columns);
    }

    private static Schema applyAddColumnEvent(
            AddColumnEvent event, Schema oldSchema, List<LogicalTypeRoot> sinkDataTypeRoots) {
        LinkedList<Column> columns = new LinkedList<>(oldSchema.getColumns());
        LinkedList<LogicalTypeRoot> newSinkDataTypes = new LinkedList<>(sinkDataTypeRoots);
        for (AddColumnEvent.ColumnWithPosition columnWithPosition : event.getAddedColumns()) {
            if (columns.contains(columnWithPosition.getAddColumn())) {
                throw new IllegalArgumentException(
                        columnWithPosition.getAddColumn().getName()
                                + " of AddColumnEvent is already existed");
            }
            switch (columnWithPosition.getPosition()) {
                case FIRST:
                    {
                        columns.addFirst(columnWithPosition.getAddColumn());
                        newSinkDataTypes.addFirst(
                                LogicalTypeRoot.valueOf(
                                        columnWithPosition
                                                .getAddColumn()
                                                .getType()
                                                .getTypeRoot()
                                                .name()));
                        break;
                    }
                case LAST:
                    {
                        columns.addLast(columnWithPosition.getAddColumn());
                        newSinkDataTypes.addLast(
                                LogicalTypeRoot.valueOf(
                                        columnWithPosition
                                                .getAddColumn()
                                                .getType()
                                                .getTypeRoot()
                                                .name()));
                        break;
                    }
                case BEFORE:
                    {
                        Preconditions.checkNotNull(
                                columnWithPosition.getExistingColumn(),
                                "existingColumn could not be null in BEFORE type AddColumnEvent");
                        List<String> columnNames =
                                columns.stream().map(Column::getName).collect(Collectors.toList());
                        int index =
                                columnNames.indexOf(
                                        columnWithPosition.getExistingColumn().getName());
                        if (index < 0) {
                            throw new IllegalArgumentException(
                                    columnWithPosition.getExistingColumn().getName()
                                            + " of AddColumnEvent is not existed");
                        }
                        columns.add(index, columnWithPosition.getAddColumn());
                        newSinkDataTypes.add(
                                index,
                                LogicalTypeRoot.valueOf(
                                        columnWithPosition
                                                .getAddColumn()
                                                .getType()
                                                .getTypeRoot()
                                                .name()));
                        break;
                    }
                case AFTER:
                    {
                        Preconditions.checkNotNull(
                                columnWithPosition.getExistingColumn(),
                                "existingColumn could not be null in AFTER type AddColumnEvent");
                        List<String> columnNames =
                                columns.stream().map(Column::getName).collect(Collectors.toList());
                        int index =
                                columnNames.indexOf(
                                        columnWithPosition.getExistingColumn().getName());
                        if (index < 0) {
                            throw new IllegalArgumentException(
                                    columnWithPosition.getExistingColumn().getName()
                                            + " of AddColumnEvent is not existed");
                        }
                        columns.add(index + 1, columnWithPosition.getAddColumn());
                        newSinkDataTypes.add(
                                index + 1,
                                LogicalTypeRoot.valueOf(
                                        columnWithPosition
                                                .getAddColumn()
                                                .getType()
                                                .getTypeRoot()
                                                .name()));
                        break;
                    }
            }
        }
        sinkDataTypeRoots.clear();
        sinkDataTypeRoots.addAll(newSinkDataTypes);
        return oldSchema.copy(columns);
    }

    private static List<Column> getColumnsAfterDropping(DropColumnEvent event, Schema oldSchema) {
        event.getDroppedColumns()
                .forEach(
                        column -> {
                            if (!oldSchema.getColumn(column.getName()).isPresent()) {
                                throw new IllegalArgumentException(
                                        column.getName() + " of DropColumnEvent is not existed");
                            }
                        });
        List<String> dropColumnNames =
                event.getDroppedColumns().stream()
                        .map(Column::getName)
                        .collect(Collectors.toList());
        return oldSchema.getColumns().stream()
                .filter((column -> !dropColumnNames.contains(column.getName())))
                .collect(Collectors.toList());
    }

    private static Schema applyDropColumnEvent(DropColumnEvent event, Schema oldSchema) {
        List<Column> columns = getColumnsAfterDropping(event, oldSchema);
        return oldSchema.copy(columns);
    }

    private static Schema applyDropColumnEvent(
            DropColumnEvent event, Schema oldSchema, List<LogicalTypeRoot> sinkDataTypeRoots) {
        event.getDroppedColumns()
                .forEach(
                        column -> {
                            if (!oldSchema.getColumn(column.getName()).isPresent()) {
                                throw new IllegalArgumentException(
                                        column.getName() + " of DropColumnEvent is not existed");
                            }
                        });
        List<Column> columns = getColumnsAfterDropping(event, oldSchema);

        List<String> columnNames =
                oldSchema.getColumns().stream().map(Column::getName).collect(Collectors.toList());
        List<Integer> dropIndexes =
                event.getDroppedColumns().stream()
                        .map(column -> columnNames.indexOf(column.getName()))
                        .collect(Collectors.toList());

        List<LogicalTypeRoot> newSinkDataTypes = new ArrayList<>();
        for (int i = 0; i < sinkDataTypeRoots.size(); i++) {
            if (dropIndexes.contains(i)) {
                continue;
            }
            newSinkDataTypes.add(sinkDataTypeRoots.get(i));
        }

        sinkDataTypeRoots.clear();
        sinkDataTypeRoots.addAll(newSinkDataTypes);
        return oldSchema.copy(columns);
    }

    private static Schema applyRenameColumnEvent(RenameColumnEvent event, Schema oldSchema) {
        event.getNameMapping()
                .forEach(
                        (name, newName) -> {
                            if (!oldSchema.getColumn(name).isPresent()) {
                                throw new IllegalArgumentException(
                                        name + " of RenameColumnEvent is not existed");
                            }
                        });
        List<Column> columns = new ArrayList<>();
        oldSchema
                .getColumns()
                .forEach(
                        column -> {
                            if (event.getNameMapping().containsKey(column.getName())) {
                                columns.add(
                                        column.copy(event.getNameMapping().get(column.getName())));
                            } else {
                                columns.add(column);
                            }
                        });
        return oldSchema.copy(columns);
    }

    private static Schema applyAlterColumnTypeEvent(AlterColumnTypeEvent event, Schema oldSchema) {
        event.getTypeMapping()
                .forEach(
                        (name, newType) -> {
                            if (!oldSchema.getColumn(name).isPresent()) {
                                throw new IllegalArgumentException(
                                        name + " of AlterColumnTypeEvent is not existed");
                            }
                        });
        List<Column> columns = new ArrayList<>();
        oldSchema
                .getColumns()
                .forEach(
                        column -> {
                            if (event.getTypeMapping().containsKey(column.getName())) {
                                columns.add(
                                        column.copy(event.getTypeMapping().get(column.getName())));
                            } else {
                                columns.add(column);
                            }
                        });
        return oldSchema.copy(columns);
    }

    private static Schema applyAlterColumnTypeEvent(
            AlterColumnTypeEvent event, Schema oldSchema, List<LogicalTypeRoot> sinkDataTypeRoots) {
        event.getTypeMapping()
                .forEach(
                        (name, newType) -> {
                            if (!oldSchema.getColumn(name).isPresent()) {
                                throw new IllegalArgumentException(
                                        name + " of AlterColumnTypeEvent is not existed");
                            }
                        });
        List<Column> columns = new ArrayList<>();
        List<LogicalTypeRoot> newSinkDataTypes = new ArrayList<>();
        final Integer[] index = {0};
        oldSchema
                .getColumns()
                .forEach(
                        column -> {
                            if (event.getTypeMapping().containsKey(column.getName())) {
                                columns.add(
                                        column.copy(event.getTypeMapping().get(column.getName())));
                                newSinkDataTypes.add(
                                        LogicalTypeRoot.valueOf(
                                                event.getTypeMapping()
                                                        .get(column.getName())
                                                        .getTypeRoot()
                                                        .name()));
                            } else {
                                columns.add(column);
                                newSinkDataTypes.add(sinkDataTypeRoots.get(index[0]));
                            }
                            index[0]++;
                        });
        sinkDataTypeRoots.clear();
        sinkDataTypeRoots.addAll(newSinkDataTypes);
        return oldSchema.copy(columns);
    }

    /**
     * This function determines if the given schema change event {@code event} should be sent to
     * downstream based on if the given transform rule has asterisk, and what columns are
     * referenced.
     *
     * <p>For example, if {@code hasAsterisk} is false, then all {@code AddColumnEvent} and {@code
     * DropColumnEvent} should be ignored since asterisk-less transform should not emit schema
     * change events that change number of downstream columns.
     *
     * <p>Also, {@code referencedColumns} will be used to determine if the schema change event
     * affects any referenced columns, since if a column has been projected out of downstream, its
     * corresponding schema change events should not be emitted, either.
     *
     * <p>For the case when {@code hasAsterisk} is true, things will be cleaner since we don't have
     * to filter out any schema change events. All we need to do is to change {@code
     * AddColumnEvent}'s inserting position, and replacing `FIRST` / `LAST` with column-relative
     * position indicators. This is necessary since extra calculated columns might be added, and
     * `FIRST` / `LAST` position might differ.
     */
    public static Optional<SchemaChangeEvent> transformSchemaChangeEvent(
            boolean hasAsterisk, List<Column> referencedColumns, SchemaChangeEvent event) {
        Optional<SchemaChangeEvent> evolvedSchemaChangeEvent = Optional.empty();
        if (event instanceof AddColumnEvent) {
            // Send add column events to downstream if there's an asterisk
            if (hasAsterisk) {
                List<AddColumnEvent.ColumnWithPosition> addedColumns =
                        ((AddColumnEvent) event)
                                .getAddedColumns().stream()
                                        .map(
                                                e -> {
                                                    if (AddColumnEvent.ColumnPosition.LAST.equals(
                                                            e.getPosition())) {
                                                        return new AddColumnEvent
                                                                .ColumnWithPosition(
                                                                e.getAddColumn(),
                                                                AddColumnEvent.ColumnPosition.AFTER,
                                                                referencedColumns.get(
                                                                        referencedColumns.size()
                                                                                - 1));
                                                    } else if (AddColumnEvent.ColumnPosition.FIRST
                                                            .equals(e.getPosition())) {
                                                        return new AddColumnEvent
                                                                .ColumnWithPosition(
                                                                e.getAddColumn(),
                                                                AddColumnEvent.ColumnPosition
                                                                        .BEFORE,
                                                                referencedColumns.get(0));
                                                    } else {
                                                        return e;
                                                    }
                                                })
                                        .collect(Collectors.toList());
                evolvedSchemaChangeEvent =
                        Optional.of(new AddColumnEvent(event.tableId(), addedColumns));
            }
        } else if (event instanceof AlterColumnTypeEvent) {
            AlterColumnTypeEvent alterColumnTypeEvent = (AlterColumnTypeEvent) event;
            if (hasAsterisk) {
                // In wildcard mode, all alter column type events should be sent to downstream
                evolvedSchemaChangeEvent = Optional.of(event);
            } else {
                // Or, we need to filter out those referenced columns and reconstruct
                // SchemaChangeEvents
                Map<String, DataType> newDataTypeMap =
                        alterColumnTypeEvent.getTypeMapping().entrySet().stream()
                                .filter(e -> referencedColumns.contains(e.getKey()))
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                if (!newDataTypeMap.isEmpty()) {
                    evolvedSchemaChangeEvent =
                            Optional.of(
                                    new AlterColumnTypeEvent(
                                            alterColumnTypeEvent.tableId(), newDataTypeMap));
                }
            }
        } else if (event instanceof RenameColumnEvent) {
            if (hasAsterisk) {
                evolvedSchemaChangeEvent = Optional.of(event);
            }
        } else if (event instanceof DropColumnEvent) {
            if (hasAsterisk) {
                evolvedSchemaChangeEvent = Optional.of(event);
            }
        } else {
            evolvedSchemaChangeEvent = Optional.of(event);
        }
        return evolvedSchemaChangeEvent;
    }
}
