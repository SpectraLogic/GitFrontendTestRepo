/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.util.db.service;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.cache.CacheResultProvider;
import com.spectralogic.util.cache.StaticCache;
import com.spectralogic.util.db.domain.service.MutexService;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.db.manager.DatabasePhysicalSpaceState;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.db.service.api.CannotBeUsedOnTransactions;
import com.spectralogic.util.db.service.api.NestableTransaction;
import com.spectralogic.util.find.PackageContentFinder;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.notification.dispatch.NotificationEventDispatcher;
import com.spectralogic.util.predicate.UnaryPredicate;
import com.spectralogic.util.thread.wp.SystemWorkPool;

public final class BeansServiceManagerImpl implements NestableTransaction
{
	public static BeansServiceManager create(
    		final NotificationEventDispatcher notificationEventDispatcher,
            final DataManager dataManager,
            final Set< Class< ? > > seedClassesInPackagesToSearchForServices )
    {
		    return new BeansServiceManagerImpl( notificationEventDispatcher, 
		            null,
		            dataManager, 
		            seedClassesInPackagesToSearchForServices,
		            true );
    }
    
    
    private BeansServiceManagerImpl(
            final NotificationEventDispatcher notificationEventDispatcher,
            final BeansServiceManager transactionSource,
            final DataManager dataManager, 
            final Set< Class< ? > > seedClassesInPackagesToSearchForServices,
            final boolean initializeUnderGlobalMutex )
    {
        Validations.verifyNotNull( "Data manager", dataManager );
        Validations.verifyNotNull( "Notification event dispatcher", notificationEventDispatcher );
        m_dataManager = dataManager;
        m_notificationEventDispatcher = notificationEventDispatcher;
        m_transactionSource = transactionSource;
        
        if ( null != transactionSource )
        {
            return;
        }
        
        initServicesAndRetrievers( seedClassesInPackagesToSearchForServices );
        
        final ArrayBlockingQueue< BeansRetriever< ? > > servicesToInitialize =
                new ArrayBlockingQueue<>( m_retrievers.values().size() );
        servicesToInitialize.addAll( m_retrievers.values() );
        final Runnable serviceInitializer = new Runnable()
        {
            public void run()
            {
                while ( true )
                {
                    final BeansRetriever< ? > service;
                    synchronized ( servicesToInitialize )
                    {
                        service = servicesToInitialize.poll();
                        if ( null == service )
                        {
                            return;
                        }
                    }
                    service.initialize();
                }
            }
        };
        final Runnable servicesInitializer = new Runnable()
        {
            public void run()
            {
                final Set< Future< ? > > futures = new HashSet<>();
                for ( int i = 0; i < 4; ++i )
                {
                    futures.add( SystemWorkPool.getInstance().submit( serviceInitializer ) );
                }
                for ( final Future< ? > f : futures )
                {
                    try
                    {
                        f.get();
                    }
                    catch ( final Exception ex )
                    {
                        throw new RuntimeException( ex );
                    }
                }
            }
        };
        if ( initializeUnderGlobalMutex )
        {
            getService( MutexService.class ).run( 
                    getClass().getName() + "-initialize-services",
                    servicesInitializer );
        }
        else
        {
            servicesInitializer.run();
        }
        LOG.info( m_retrievers.size() + " services loaded." );
    }
    
    
    private void initServicesAndRetrievers( final Set< Class< ? > > seedClassesInPackagesToSearchForServices )
    {
        final Set< Class< ? extends BeansRetriever< ? > > > serviceTypes =
                SERVICE_TYPES_CACHE.get( seedClassesInPackagesToSearchForServices );
        for ( final Class< ? extends BeansRetriever< ? > > serviceType : serviceTypes )
        {
            if ( Modifier.isInterface( serviceType.getModifiers() )
                    || Modifier.isAbstract( serviceType.getModifiers() ) )
            {
                continue;
            }
            
            if ( Modifier.isPublic( serviceType.getModifiers() ) )
            {
                throw new RuntimeException( 
                        "Service implementations cannot be public: " + serviceType );
            }
            
            try
            {
                final Constructor< ? extends BeansRetriever< ? > > con = 
                        serviceType.getDeclaredConstructor();
                if ( Modifier.isPublic( con.getModifiers() ) )
                {
                    throw new RuntimeException(
                            "Service implementation constructors cannot be public: " + con );
                }
                con.setAccessible( true );
                final BeansRetriever< ? > service = con.newInstance();
                
                final Class< ? extends SimpleBeanSafeToProxy > servicedDataType = 
                        service.getServicedType();
                if ( DatabasePersistable.class.isAssignableFrom( servicedDataType ) 
                        && !m_dataManager.getSupportedTypes().contains( servicedDataType ) )
                {
                    throw new IllegalArgumentException( 
                            "Data manager supplied cannot service " + servicedDataType + "." );
                }
                m_retrievers.put( servicedDataType, service );
                m_services.put( serviceType, service );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
        }
        
        /*
         * Force clients to refer to the interface type if there is one.
         */
        final Map< Class< ? >, BeansRetriever< ? > > servicesByApi = new HashMap<>();
        for ( final Map.Entry< Class< ? >, BeansRetriever< ? > > e : new HashSet<>( m_services.entrySet() ) )
        {
            final Set< Class< ? > > apis = CollectionFactory.toSet( e.getValue().getClass().getInterfaces() );
            if ( 1 != apis.size() )
            {
                throw new RuntimeException( 
                        "Service impls must implement exactly one interface (their service API): " 
                        + e.getValue().getClass() );
            }
            
            final Class< ? > interfaceType = apis.iterator().next();
            if ( servicesByApi.containsKey( interfaceType ) )
            {
                throw new RuntimeException(
                        "Only one service impl can implement a service API: " + e.getValue().getClass() );
            }
            servicesByApi.put( interfaceType, e.getValue() );
            
            m_services.remove( e.getKey() );
            m_services.put( interfaceType, e.getValue() );
            e.getValue().setInitParams( this, m_dataManager, m_notificationEventDispatcher );
        }
    }
    

    public < T extends SimpleBeanSafeToProxy > BeansRetriever< T > getRetriever( final Class< T > type )
    {
        @SuppressWarnings( "unchecked" )
        final BeansRetriever< T > retval = (BeansRetriever< T >)m_retrievers.get( type );
        
        if ( null == retval )
        {
            throw new IllegalArgumentException(
                    "Retriever not found for domain: " + type.getSimpleName() );
        }
        
        return retval;
    }

    
    public < T extends BeansRetriever< ? > > T getService( final Class< T > service )
    {
        @SuppressWarnings( "unchecked" )
        final T retval = (T)m_services.get( service );
        
        if ( null == retval )
        {
            throw new IllegalArgumentException(
                    "Service not found: " + service.getSimpleName() );
        }
        
        return retval;
    }

    
    public < T extends SimpleBeanSafeToProxy > BeanCreator< T > getCreator( final Class< T > type )
    {
        final BeansRetriever< T > retval = getRetriever( type );
        if ( !BeanCreator.class.isAssignableFrom( retval.getClass() ) )
        {
            throw new IllegalArgumentException( 
                    "Not a " + BeanCreator.class.getSimpleName() + ": " + retval.getClass() );
        }
        
        @SuppressWarnings( "unchecked" )
        final BeanCreator< T > castedRetval = (BeanCreator< T >)retval;
        return castedRetval;
    }

    
    public < T extends SimpleBeanSafeToProxy > BeanUpdater< T > getUpdater( final Class< T > type )
    {
        final BeansRetriever< T > retval = getRetriever( type );
        if ( !BeanUpdater.class.isAssignableFrom( retval.getClass() ) )
        {
            throw new IllegalArgumentException( 
                    "Not a " + BeanUpdater.class.getSimpleName() + ": " + retval.getClass() );
        }
        
        @SuppressWarnings( "unchecked" )
        final BeanUpdater< T > castedRetval = (BeanUpdater< T >)retval;
        return castedRetval;
    }

    
    public BeanDeleter getDeleter( final Class< ? extends SimpleBeanSafeToProxy > type )
    {
        final BeansRetriever< ? > retval = getRetriever( type );
        if ( !BeanDeleter.class.isAssignableFrom( retval.getClass() ) )
        {
            throw new IllegalArgumentException( 
                    "Not a " + BeanDeleter.class.getSimpleName() + ": " + retval.getClass() );
        }
        
        final BeanDeleter castedRetval = (BeanDeleter)retval;
        return castedRetval;
    }
    
    
    public Set< BeansRetriever< ? > > getServices( final UnaryPredicate< Class< ? > > serviceFilter )
    {
        final Set< BeansRetriever< ? > > retval = new HashSet<>();
        for ( final BeansRetriever< ? > retriever : m_services.values() )
        {
            if ( serviceFilter.test( retriever.getClass() ) )
            {
                retval.add( retriever );
            }
        }
        
        return retval;
    }
    
    
    public BeansServiceManager startTransaction()
    {
    	return startTransactionInternal();
    }
    
    
    public NestableTransaction startTransactionInternal()
    {
        final BeansServiceManagerImpl retval = new BeansServiceManagerImpl( 
                m_notificationEventDispatcher.startTransaction(),
                this,
                m_dataManager.startTransaction(),
                null,
                true );
        try
        {
            for ( final Map.Entry< Class< ? >, BeansRetriever< ? > > e : m_services.entrySet() )
            {
                if ( null != e.getKey().getAnnotation( CannotBeUsedOnTransactions.class ) )
                {
                    continue;
                }
                
                final Constructor< ? > con = e.getValue().getClass().getDeclaredConstructor();
                con.setAccessible( true );
                final BeansRetriever< ? > svc = (BeansRetriever< ? >)con.newInstance();
                svc.setInitParams( retval, retval.m_dataManager, retval.m_notificationEventDispatcher );
                retval.m_services.put( e.getKey(), svc );
                retval.m_retrievers.put( svc.getServicedType(), svc );
            }
            return retval;
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }


    public void commitTransaction()
    {
        m_notificationEventDispatcher.commitTransaction();
        m_dataManager.commitTransaction();
    }
    
    
    public void closeTransaction()
    {
        m_dataManager.closeTransaction();
    }
    
    
    public NestableTransaction startNestableTransaction()
    {
    	if ( ! isTransaction() )
    	{
    		return startTransactionInternal();
    	}
    	else
    	{
    		m_transactionNestDepth++;
    		return this;
    	}
    }
    
    
    public void commitNestableTransaction()
    {
    	if ( 1 >= m_transactionNestDepth )
    	{
    		commitTransaction();
    	}
    }
    
    
    public void closeNestableTransaction()
    {
    	m_transactionNestDepth--;
    	if ( 0 >= m_transactionNestDepth )
    	{
    		closeTransaction();
    	}
    }
    
    
    @Override public DatabasePhysicalSpaceState getDatabaseSpaceState()
    {
        return m_dataManager.getDatabaseSpaceState();
    }
    
    
    @Override public DatabasePhysicalSpaceState getFreeToTotalDiskSpaceRatioState()
    {
        return m_dataManager.getFreeToTotalDiskSpaceRatioState();
    }
    
    
    @Override public DatabasePhysicalSpaceState getMaxTableToFreeRatioState()
    {
        return m_dataManager.getMaxTableToFreeRatioState();
    }
    
    
    public NotificationEventDispatcher getNotificationEventDispatcher()
    {
        return m_notificationEventDispatcher;
    }
    
    
    public boolean isTransaction()
    {
        return ( null != m_transactionSource );
    }


    public BeansServiceManager getTransactionSource()
    {
        if ( !isTransaction() )
        {
            throw new IllegalStateException( "This method can only be called on transactions." );
        }
        return m_transactionSource;
    }
    
    
    public void reserveConnections(
            final int numberOfAutoCommitConnections,
            final int numberOfExplicitCommitConnections )
    {
        m_dataManager.reserveConnections( numberOfAutoCommitConnections, numberOfExplicitCommitConnections );
    }
    
    
    public void releaseReservedConnections()
    {
        m_dataManager.releaseReservedConnections();
    }
    
    
    private final static class ServiceTypesCacheResultProvider 
        implements CacheResultProvider< Set< Class< ? > >, Set< Class< ? extends BeansRetriever< ? > > > >
    {
        @Override
        public Set< Class< ? extends BeansRetriever< ? > > > generateCacheResultFor(
                final Set< Class< ? > > originalSeedClassesInPackagesToSearchForServices )
        {
            Validations.verifyNotNull( "Seed classes", originalSeedClassesInPackagesToSearchForServices );
            final Set< Class< ? > > seedClassesInPackagesToSearchForServices =
                    new HashSet<>( originalSeedClassesInPackagesToSearchForServices );
            seedClassesInPackagesToSearchForServices.add( MutexService.class );
            final Set< Class< ? extends BeansRetriever< ? > > > serviceTypes = new HashSet<>();
            for ( final Class< ? > seed : seedClassesInPackagesToSearchForServices )
            {
                serviceTypes.addAll( getServicesFromSeed( seed ) );
            }
            
            final List< String > seedClassStrings = new ArrayList<>();
            for ( final Class< ? > clazz : seedClassesInPackagesToSearchForServices )
            {
                seedClassStrings.add( clazz.getName() );
            }
            Collections.sort( seedClassStrings );
            LOG.info( "Discovered " + serviceTypes.size() + " service types from seed classes: "
                      + seedClassStrings );
            
            return serviceTypes;
        }
        
        private Set< Class< ? extends BeansRetriever< ? > > > getServicesFromSeed( final Class< ? > seed )
        {
            final PackageContentFinder finder = new PackageContentFinder(
                    seed.getPackage().getName(), 
                    seed,
                    null );
            final Set< ? > classes = finder.getClasses( new UnaryPredicate< Class< ? > >()
            {
                public boolean test( final Class< ? > element )
                {
                    return BeansRetriever.class.isAssignableFrom( element );
                }
            } );
            
            if ( classes.isEmpty() )
            {
                throw new IllegalArgumentException( "Failed to find any services." );
            }
            
            @SuppressWarnings( "unchecked" )
            final Set< Class< ? extends BeansRetriever< ? > > > castedClasses =
                    (Set< Class< ? extends BeansRetriever< ? > > >)classes;
            return castedClasses;
        }
    } // end inner class def
    
    
    @Override
	public void close()
	{
		closeNestableTransaction();
	}
    
    
    private int m_transactionNestDepth = 1;
    private final Map< Class< ? >, BeansRetriever< ? > > m_services = new HashMap<>();
    private final Map< Class< ? >, BeansRetriever< ? > > m_retrievers = new HashMap<>();
    private final DataManager m_dataManager;
    private final NotificationEventDispatcher m_notificationEventDispatcher;
    private final BeansServiceManager m_transactionSource;
    
    private final static StaticCache< Set< Class< ? > >, Set< Class< ? extends BeansRetriever< ? > > > >
        SERVICE_TYPES_CACHE = new StaticCache<>( new ServiceTypesCacheResultProvider() );
    private final static Logger LOG = Logger.getLogger( BeansServiceManagerImpl.class );
}
