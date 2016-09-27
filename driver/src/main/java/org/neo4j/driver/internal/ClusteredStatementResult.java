/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
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
package org.neo4j.driver.internal;

import java.util.List;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ConnectionFailureException;
import org.neo4j.driver.v1.exceptions.NoSuchRecordException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.util.Function;

import static java.lang.String.format;

import static org.neo4j.driver.internal.ClusteredNetworkSession.filterFailureToWrite;
import static org.neo4j.driver.internal.ClusteredNetworkSession.sessionExpired;

public class ClusteredStatementResult implements StatementResult
{
    private final StatementResult delegate;
    private final AccessMode mode;
    private final BoltServerAddress address;
    private final ClusteredErrorHandler onError;

    ClusteredStatementResult( StatementResult delegate, AccessMode mode, BoltServerAddress address,
            ClusteredErrorHandler onError )
    {
        this.delegate = delegate;
        this.mode = mode;
        this.address = address;
        this.onError = onError;
    }

    @Override
    public List<String> keys()
    {
        try
        {
            return delegate.keys();
        }
        catch ( ConnectionFailureException e )
        {
            throw sessionExpired( e, onError, address );
        }
        catch ( ClientException e )
        {
            throw filterFailureToWrite( e, mode, onError, address );
        }
    }

    @Override
    public boolean hasNext()
    {
        try
        {
            return delegate.hasNext();
        }
        catch ( ConnectionFailureException e )
        {
            throw sessionExpired( e, onError, address );
        }
        catch ( ClientException e )
        {
            throw filterFailureToWrite( e, mode, onError, address );
        }
    }

    @Override
    public Record next()
    {
        try
        {
            return delegate.next();
        }
        catch ( ConnectionFailureException e )
        {
            throw sessionExpired( e, onError, address );
        }
        catch ( ClientException e )
        {
            throw filterFailureToWrite( e, mode, onError, address );
        }
    }


    @Override
    public Record single() throws NoSuchRecordException
    {
        try
        {
            return delegate.single();
        }
        catch ( ConnectionFailureException e )
        {
            throw sessionExpired( e, onError, address );
        }
        catch ( ClientException e )
        {
            throw filterFailureToWrite( e, mode, onError, address );
        }
    }

    @Override
    public Record peek()
    {
        try
        {
            return delegate.peek();
        }
        catch ( ConnectionFailureException e )
        {
            throw sessionExpired( e, onError, address );
        }
        catch ( ClientException e )
        {
            throw filterFailureToWrite( e, mode, onError, address );
        }
    }

    @Override
    public List<Record> list()
    {
        try
        {
            return delegate.list();
        }
        catch ( ConnectionFailureException e )
        {
            throw sessionExpired( e, onError, address );
        }
        catch ( ClientException e )
        {
            throw filterFailureToWrite( e, mode, onError, address );
        }
    }

    @Override
    public <T> List<T> list( Function<Record, T> mapFunction )
    {
        try
        {
            return delegate.list( mapFunction );
        }
        catch ( ConnectionFailureException e )
        {
            throw sessionExpired( e, onError, address );
        }
        catch ( ClientException e )
        {
            throw filterFailureToWrite( e, mode, onError, address );
        }
    }

    @Override
    public void remove()
    {
        throw new ClientException( "Removing records from a result is not supported." );
    }

    @Override
    public ResultSummary consume()
    {
        try
        {
            return delegate.consume();
        }
        catch ( ConnectionFailureException e )
        {
            throw sessionExpired( e, onError, address );
        }
        catch ( ClientException e )
        {
            throw filterFailureToWrite( e, mode, onError, address );
        }
    }

}
