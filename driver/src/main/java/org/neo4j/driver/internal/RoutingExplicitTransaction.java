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

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ConnectionFailureException;

import static org.neo4j.driver.internal.RoutingNetworkSession.filterFailureToWrite;
import static org.neo4j.driver.internal.RoutingNetworkSession.sessionExpired;

class RoutingExplicitTransaction extends ExplicitTransaction
{
    private final AccessMode mode;
    private final RoutingErrorHandler onError;

    RoutingExplicitTransaction( Connection conn, AccessMode mode, RoutingErrorHandler onError, Runnable cleanup, String bookmark )
    {
        super( conn, cleanup, bookmark );
        this.mode = mode;
        this.onError = onError;
    }

    @Override
    public synchronized StatementResult run( Statement statement )
    {
        try
        {
            return super.run( statement );
        }
        catch ( ConnectionFailureException e )
        {
            throw sessionExpired(e, onError, conn.address());
        }
        catch ( ClientException e )
        {
            throw filterFailureToWrite( e, mode, onError, conn.address() );
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
            throw sessionExpired(e, onError, conn.address());
        }
        catch ( ClientException e )
        {
            throw filterFailureToWrite( e, mode, onError, conn.address() );
        }
    }
}
