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

import org.junit.Test;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.driver.v1.AccessMode.WRITE;

public class ClusteredWriteStatementResultTest
{
    private static final BoltServerAddress LOCALHOST = new BoltServerAddress( "localhost", 7687 );
    private StatementResult delegate = mock( StatementResult.class );
    private ClusteredErrorHandler onError = mock( ClusteredErrorHandler.class );

    @Test
    public void shouldHandleWriteFailureOnConsume()
    {
        // Given
        when( delegate.consume() )
                .thenThrow( new ClientException( "Neo.ClientError.Cluster.NotALeader", "oh no!" ) );
        ClusteredStatementResult result =
                new ClusteredStatementResult( delegate, WRITE, LOCALHOST, onError );

        // When
        try
        {
            result.consume();
            fail();
        }
        catch ( SessionExpiredException e )
        {
            //ignore
        }

        // Then
        verify( onError ).onWriteFailure( LOCALHOST );
        verifyNoMoreInteractions( onError );
    }

    @Test
    public void shouldHandleWriteFailureOnHasNext()
    {
        // Given
        when( delegate.hasNext() )
                .thenThrow( new ClientException( "Neo.ClientError.Cluster.NotALeader", "oh no!" ) );
        ClusteredStatementResult result =
                new ClusteredStatementResult( delegate, WRITE, LOCALHOST, onError );

        // When
        try
        {
            result.hasNext();
            fail();
        }
        catch ( SessionExpiredException e )
        {
            //ignore
        }

        // Then
        verify( onError ).onWriteFailure( LOCALHOST );
        verifyNoMoreInteractions( onError );
    }

    @Test
    public void shouldHandleWriteFailureOnKeys()
    {
        // Given
        when( delegate.keys() )
                .thenThrow( new ClientException( "Neo.ClientError.Cluster.NotALeader", "oh no!" ) );
        ClusteredStatementResult result =
                new ClusteredStatementResult( delegate, WRITE, LOCALHOST, onError );

        // When
        try
        {
            result.keys();
            fail();
        }
        catch ( SessionExpiredException e )
        {
            //ignore
        }

        // Then
        verify( onError ).onWriteFailure( LOCALHOST );
        verifyNoMoreInteractions( onError );
    }

    @Test
    public void shouldHandleWriteFailureOnList()
    {
        // Given
        when( delegate.list() )
                .thenThrow( new ClientException( "Neo.ClientError.Cluster.NotALeader", "oh no!" ) );
        ClusteredStatementResult result =
                new ClusteredStatementResult( delegate, WRITE, LOCALHOST, onError );

        // When
        try
        {
            result.list();
            fail();
        }
        catch ( SessionExpiredException e )
        {
            //ignore
        }

        // Then
        verify( onError ).onWriteFailure( LOCALHOST );
        verifyNoMoreInteractions( onError );
    }

    @Test
    public void shouldHandleWriteFailureOnNext()
    {
        // Given
        when( delegate.next() )
                .thenThrow( new ClientException( "Neo.ClientError.Cluster.NotALeader", "oh no!" ) );
        ClusteredStatementResult result =
                new ClusteredStatementResult( delegate, WRITE, LOCALHOST, onError );

        // When
        try
        {
            result.next();
            fail();
        }
        catch ( SessionExpiredException e )
        {
            //ignore
        }

        // Then
        verify( onError ).onWriteFailure( LOCALHOST );
        verifyNoMoreInteractions( onError );
    }

    @Test
    public void shouldHandleWriteFailureOnPeek()
    {
        // Given
        when( delegate.peek() )
                .thenThrow( new ClientException( "Neo.ClientError.Cluster.NotALeader", "oh no!" ) );
        ClusteredStatementResult result =
                new ClusteredStatementResult( delegate, WRITE, LOCALHOST, onError );

        // When
        try
        {
            result.peek();
            fail();
        }
        catch ( SessionExpiredException e )
        {
            //ignore
        }

        // Then
        verify( onError ).onWriteFailure( LOCALHOST );
        verifyNoMoreInteractions( onError );
    }

    @Test
    public void shouldHandleWriteFailureOnSingle()
    {
        // Given
        when( delegate.single() )
                .thenThrow( new ClientException( "Neo.ClientError.Cluster.NotALeader", "oh no!" ) );
        ClusteredStatementResult result =
                new ClusteredStatementResult( delegate, WRITE, LOCALHOST, onError );

        // When
        try
        {
            result.single();
            fail();
        }
        catch ( SessionExpiredException e )
        {
            //ignore
        }

        // Then
        verify( onError ).onWriteFailure( LOCALHOST );
        verifyNoMoreInteractions( onError );
    }
}
