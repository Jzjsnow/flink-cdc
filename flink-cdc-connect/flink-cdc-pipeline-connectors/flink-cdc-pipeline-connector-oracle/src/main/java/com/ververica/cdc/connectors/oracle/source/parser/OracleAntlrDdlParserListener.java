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

package com.ververica.cdc.connectors.oracle.source.parser;

import com.ververica.cdc.common.event.SchemaChangeEvent;
import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.antlr.ProxyParseTreeListenerUtil;
import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.connector.oracle.antlr.listener.CommentParserListener;
import io.debezium.connector.oracle.antlr.listener.CreateTableParserListener;
import io.debezium.connector.oracle.antlr.listener.DropTableParserListener;
import io.debezium.connector.oracle.antlr.listener.TruncateTableParserListener;
import io.debezium.ddl.parser.oracle.generated.PlSqlParserBaseListener;
import io.debezium.text.ParsingException;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTreeListener;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This class is Oracle main DDL parser listener class. It instantiates supported listeners, walks
 * listeners through every parsing rule and collects parsing exceptions.
 */
public class OracleAntlrDdlParserListener extends PlSqlParserBaseListener
        implements AntlrDdlParserListener {

    private final List<ParseTreeListener> listeners = new CopyOnWriteArrayList<>();
    private final Collection<ParsingException> errors = new ArrayList<>();

    public OracleAntlrDdlParserListener(
            final String catalogName,
            final String schemaName,
            final OracleDdlParser parser,
            LinkedList<SchemaChangeEvent> parsedEvents) {
        listeners.add(new CreateTableParserListener(catalogName, schemaName, parser, listeners));
        listeners.add(
                new OracleAlterTableParserListener(
                        catalogName, schemaName, parser, listeners, parsedEvents));
        listeners.add(new DropTableParserListener(catalogName, schemaName, parser));
        listeners.add(new CommentParserListener(catalogName, schemaName, parser));
        listeners.add(new TruncateTableParserListener(catalogName, schemaName, parser));
    }

    @Override
    public Collection<ParsingException> getErrors() {
        return errors;
    }

    @Override
    public void enterEveryRule(ParserRuleContext ctx) {
        ProxyParseTreeListenerUtil.delegateEnterRule(ctx, listeners, errors);
    }

    @Override
    public void exitEveryRule(ParserRuleContext ctx) {
        ProxyParseTreeListenerUtil.delegateExitRule(ctx, listeners, errors);
    }
}
