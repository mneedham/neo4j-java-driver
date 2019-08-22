/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.async.connection.BootstrapFactory;
import org.neo4j.driver.internal.async.connection.ChannelConnector;
import org.neo4j.driver.internal.async.connection.ChannelConnectorImpl;
import org.neo4j.driver.internal.async.pool.ConnectionPoolImpl;
import org.neo4j.driver.internal.async.pool.PoolSettings;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.cluster.loadbalancing.LeastConnectedLoadBalancingStrategy;
import org.neo4j.driver.internal.cluster.loadbalancing.LoadBalancer;
import org.neo4j.driver.internal.cluster.loadbalancing.LoadBalancingStrategy;
import org.neo4j.driver.internal.logging.NettyLogging;
import org.neo4j.driver.internal.metrics.InternalMetricsProvider;
import org.neo4j.driver.internal.metrics.MetricsProvider;
import org.neo4j.driver.internal.retry.ExponentialBackoffRetryLogic;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.net.ServerAddressResolver;

import static java.lang.String.format;
import static org.neo4j.driver.internal.cluster.IdentityResolver.IDENTITY_RESOLVER;
import static org.neo4j.driver.internal.metrics.MetricsProvider.METRICS_DISABLED_PROVIDER;
import static org.neo4j.driver.internal.security.SecurityPlan.insecure;
import static org.neo4j.driver.internal.util.ErrorUtil.addSuppressed;

public class DriverFactory
{
    public static final String BOLT_URI_SCHEME = "bolt";
    public static final String BOLT_ROUTING_URI_SCHEME = "neo4j";

    public final Driver newInstance( URI uri, AuthToken authToken, RoutingSettings routingSettings,
            RetrySettings retrySettings, Config config )
    {
        return newInstance( uri, authToken, routingSettings, retrySettings, config, null );
    }

    public final Driver newInstance ( URI uri, AuthToken authToken, RoutingSettings routingSettings,
            RetrySettings retrySettings, Config config,  EventLoopGroup eventLoopGroup )
    {
        Bootstrap bootstrap;
        boolean ownsEventLoopGroup;
        if ( eventLoopGroup == null )
        {
            bootstrap = createBootstrap();
            ownsEventLoopGroup = true;
        }
        else
        {
            bootstrap = createBootstrap( eventLoopGroup );
            ownsEventLoopGroup = false;
        }

        authToken = authToken == null ? AuthTokens.none() : authToken;

        BoltServerAddress address = new BoltServerAddress( uri );
        RoutingSettings newRoutingSettings = routingSettings.withRoutingContext( new RoutingContext( uri ) );
        SecurityPlan securityPlan = createSecurityPlan( address, config );

        InternalLoggerFactory.setDefaultFactory( new NettyLogging( config.logging() ) );
        EventExecutorGroup eventExecutorGroup = bootstrap.config().group();
        RetryLogic retryLogic = createRetryLogic( retrySettings, eventExecutorGroup, config.logging() );

        MetricsProvider metricsProvider = createDriverMetrics( config, createClock() );
        ConnectionPool connectionPool = createConnectionPool( authToken, securityPlan, bootstrap, metricsProvider, config, ownsEventLoopGroup );

        return createDriver( uri, securityPlan, address, connectionPool, eventExecutorGroup, newRoutingSettings, retryLogic, metricsProvider, config );
    }

    protected ConnectionPool createConnectionPool( AuthToken authToken, SecurityPlan securityPlan, Bootstrap bootstrap,
            MetricsProvider metricsProvider, Config config, boolean ownsEventLoopGroup )
    {
        Clock clock = createClock();
        ConnectionSettings settings = new ConnectionSettings( authToken, config.connectionTimeoutMillis() );
        ChannelConnector connector = createConnector( settings, securityPlan, config, clock );
        PoolSettings poolSettings = new PoolSettings( config.maxConnectionPoolSize(),
                config.connectionAcquisitionTimeoutMillis(), config.maxConnectionLifetimeMillis(),
                config.idleTimeBeforeConnectionTest()
        );
        return new ConnectionPoolImpl( connector, bootstrap, poolSettings, metricsProvider.metricsListener(), config.logging(), clock, ownsEventLoopGroup );
    }

    protected static MetricsProvider createDriverMetrics( Config config, Clock clock )
    {
        if( config.isMetricsEnabled() )
        {
            return new InternalMetricsProvider( clock );
        }
        else
        {
            return METRICS_DISABLED_PROVIDER;
        }
    }

    protected ChannelConnector createConnector( ConnectionSettings settings, SecurityPlan securityPlan,
            Config config, Clock clock )
    {
        return new ChannelConnectorImpl( settings, securityPlan, config.logging(), clock );
    }

    private InternalDriver createDriver( URI uri, SecurityPlan securityPlan, BoltServerAddress address, ConnectionPool connectionPool,
            EventExecutorGroup eventExecutorGroup, RoutingSettings routingSettings, RetryLogic retryLogic, MetricsProvider metricsProvider, Config config )
    {
        try
        {
            String scheme = uri.getScheme().toLowerCase();
            switch ( scheme )
            {
            case BOLT_URI_SCHEME:
                assertNoRoutingContext( uri, routingSettings );
                return createDirectDriver( securityPlan, address, connectionPool, retryLogic, metricsProvider, config );
            case BOLT_ROUTING_URI_SCHEME:
                return createRoutingDriver( securityPlan, address, connectionPool, eventExecutorGroup, routingSettings, retryLogic, metricsProvider, config );
            default:
                throw new ClientException( format( "Unsupported URI scheme: %s", scheme ) );
            }
        }
        catch ( Throwable driverError )
        {
            // we need to close the connection pool if driver creation threw exception
            closeConnectionPoolAndSuppressError( connectionPool, driverError );
            throw driverError;
        }
    }

    /**
     * Creates a new driver for "bolt" scheme.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected InternalDriver createDirectDriver( SecurityPlan securityPlan, BoltServerAddress address, ConnectionPool connectionPool, RetryLogic retryLogic,
            MetricsProvider metricsProvider, Config config )
    {
        ConnectionProvider connectionProvider = new DirectConnectionProvider( address, connectionPool );
        SessionFactory sessionFactory = createSessionFactory( connectionProvider, retryLogic, config );
        InternalDriver driver = createDriver( securityPlan, sessionFactory, metricsProvider, config );
        Logger log = config.logging().getLog( Driver.class.getSimpleName() );
        log.info( "Direct driver instance %s created for server address %s", driver.hashCode(), address );
        return driver;
    }

    /**
     * Creates new a new driver for "neo4j" scheme.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected InternalDriver createRoutingDriver( SecurityPlan securityPlan, BoltServerAddress address, ConnectionPool connectionPool,
            EventExecutorGroup eventExecutorGroup, RoutingSettings routingSettings, RetryLogic retryLogic, MetricsProvider metricsProvider, Config config )
    {
        ConnectionProvider connectionProvider = createLoadBalancer( address, connectionPool, eventExecutorGroup,
                config, routingSettings );
        SessionFactory sessionFactory = createSessionFactory( connectionProvider, retryLogic, config );
        InternalDriver driver = createDriver( securityPlan, sessionFactory, metricsProvider, config );
        Logger log = config.logging().getLog( Driver.class.getSimpleName() );
        log.info( "Routing driver instance %s created for server address %s", driver.hashCode(), address );
        return driver;
    }

    /**
     * Creates new {@link Driver}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected InternalDriver createDriver( SecurityPlan securityPlan, SessionFactory sessionFactory, MetricsProvider metricsProvider, Config config )
    {
        return new InternalDriver( securityPlan, sessionFactory, metricsProvider, config.logging() );
    }

    /**
     * Creates new {@link LoadBalancer} for the routing driver.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected LoadBalancer createLoadBalancer( BoltServerAddress address, ConnectionPool connectionPool,
            EventExecutorGroup eventExecutorGroup, Config config, RoutingSettings routingSettings )
    {
        LoadBalancingStrategy loadBalancingStrategy = new LeastConnectedLoadBalancingStrategy( connectionPool, config.logging() );
        ServerAddressResolver resolver = createResolver( config );
        return new LoadBalancer( address, routingSettings, connectionPool, eventExecutorGroup, createClock(),
                config.logging(), loadBalancingStrategy, resolver );
    }

    private static ServerAddressResolver createResolver( Config config )
    {
        ServerAddressResolver configuredResolver = config.resolver();
        return configuredResolver != null ? configuredResolver : IDENTITY_RESOLVER;
    }

    /**
     * Creates new {@link Clock}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected Clock createClock()
    {
        return Clock.SYSTEM;
    }

    /**
     * Creates new {@link SessionFactory}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected SessionFactory createSessionFactory( ConnectionProvider connectionProvider, RetryLogic retryLogic,
            Config config )
    {
        return new SessionFactoryImpl( connectionProvider, retryLogic, config );
    }

    /**
     * Creates new {@link RetryLogic}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected RetryLogic createRetryLogic( RetrySettings settings, EventExecutorGroup eventExecutorGroup,
            Logging logging )
    {
        return new ExponentialBackoffRetryLogic( settings, eventExecutorGroup, createClock(), logging );
    }

    /**
     * Creates new {@link Bootstrap}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected Bootstrap createBootstrap()
    {
        return BootstrapFactory.newBootstrap();
    }

    /**
     * Creates new {@link Bootstrap}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected Bootstrap createBootstrap( EventLoopGroup eventLoopGroup )
    {
        return BootstrapFactory.newBootstrap( eventLoopGroup );
    }

    private static SecurityPlan createSecurityPlan( BoltServerAddress address, Config config )
    {
        try
        {
            return createSecurityPlanImpl( config );
        }
        catch ( GeneralSecurityException | IOException ex )
        {
            throw new ClientException( "Unable to establish SSL parameters", ex );
        }
    }

    /*
     * Establish a complete SecurityPlan based on the details provided for
     * driver construction.
     */
    private static SecurityPlan createSecurityPlanImpl( Config config )
            throws GeneralSecurityException, IOException
    {
        if ( config.encrypted() )
        {
            Config.TrustStrategy trustStrategy = config.trustStrategy();
            boolean hostnameVerificationEnabled = trustStrategy.isHostnameVerificationEnabled();
            switch ( trustStrategy.strategy() )
            {
            case TRUST_CUSTOM_CA_SIGNED_CERTIFICATES:
                return SecurityPlan.forCustomCASignedCertificates( trustStrategy.certFile(), hostnameVerificationEnabled );
            case TRUST_SYSTEM_CA_SIGNED_CERTIFICATES:
                return SecurityPlan.forSystemCASignedCertificates( hostnameVerificationEnabled );
            case TRUST_ALL_CERTIFICATES:
                return SecurityPlan.forAllCertificates( hostnameVerificationEnabled );
            default:
                throw new ClientException(
                        "Unknown TLS authentication strategy: " + trustStrategy.strategy().name() );
            }
        }
        else
        {
            return insecure();
        }
    }

    private static void assertNoRoutingContext( URI uri, RoutingSettings routingSettings )
    {
        RoutingContext routingContext = routingSettings.routingContext();
        if ( routingContext.isDefined() )
        {
            throw new IllegalArgumentException(
                    "Routing parameters are not supported with scheme 'bolt'. Given URI: '" + uri + "'" );
        }
    }

    private static void closeConnectionPoolAndSuppressError( ConnectionPool connectionPool, Throwable mainError )
    {
        try
        {
            Futures.blockingGet( connectionPool.close() );
        }
        catch ( Throwable closeError )
        {
            addSuppressed( mainError, closeError );
        }
    }
}
