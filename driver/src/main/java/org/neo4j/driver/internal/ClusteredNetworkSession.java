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


import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ConnectionFailureException;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;

import static java.lang.String.format;

public class ClusteredNetworkSession extends NetworkSession
{
    private final AccessMode mode;
    private final ClusteredErrorHandler onError;

    ClusteredNetworkSession( AccessMode mode, Connection connection,
            ClusteredErrorHandler onError, Logger logger )
    {
        super( connection, logger );
        this.mode = mode;
        this.onError = onError;
    }

    @Override
    public StatementResult run( Statement statement )
    {
        try
        {
            return new ClusteredStatementResult( super.run( statement ), mode, connection.address(), onError );
        }
        catch ( ConnectionFailureException e )
        {
            throw sessionExpired(e, onError, connection.address());
        }
        catch ( ClientException e )
        {
            throw filterFailureToWrite(e, mode, onError, connection.address() );
        }
    }

    @Override
    public void close()
    {
        try
        {
            super.close();
        }
        catch ( ConnectionFailureException e )
        {
            throw sessionExpired(e, onError, connection.address());
        }
    }

    static Neo4jException filterFailureToWrite( ClientException e, AccessMode mode, ClusteredErrorHandler onError,
            BoltServerAddress address )
    {
        if ( isFailedToWrite( e ) )
        {
            if ( mode.equals( AccessMode.READ ) )
            {
                return new ClientException( "Write queries cannot be performed in READ access mode." );
            }
            else
            {
                onError.onWriteFailure( address );
                return new SessionExpiredException( String.format( "Server at %s no longer accepts writes", address ) );
            }
        }
        else
        {
            return e;
        }
    }

    static SessionExpiredException sessionExpired( ConnectionFailureException e, ClusteredErrorHandler onError,
                                                   BoltServerAddress address )
    {
        onError.onConnectionFailure( address );
        return new SessionExpiredException( format( "Server at %s is no longer available", address.toString() ), e );
    }

    private static boolean isFailedToWrite( ClientException e )
    {
        return e.code().equals( "Neo.ClientError.Cluster.NotALeader" ) ||
                e.code().equals( "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase" );
    }
}
