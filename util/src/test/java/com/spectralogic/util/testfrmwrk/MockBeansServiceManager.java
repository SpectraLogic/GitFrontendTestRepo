/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.util.testfrmwrk;

import java.util.HashSet;
import java.util.Set;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.manager.DatabasePhysicalSpaceState;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.db.service.api.NestableTransaction;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.notification.dispatch.NotificationEventDispatcher;
import com.spectralogic.util.predicate.UnaryPredicate;

public final class MockBeansServiceManager implements BeansServiceManager
{
    @SuppressWarnings( "unchecked" )
    public < T extends SimpleBeanSafeToProxy > BeansRetriever< T > getRetriever( final Class< T > type )
    {
        return InterfaceProxyFactory.getProxy( BeansRetriever.class, null );
    }

    
    public < T extends BeansRetriever< ? > > T getService( final Class< T > service )
    {
        return InterfaceProxyFactory.getProxy( service, null );
    }

    
    @SuppressWarnings( "unchecked" )
    public < T extends SimpleBeanSafeToProxy > BeanCreator< T > getCreator( final Class< T > type )
    {
        return InterfaceProxyFactory.getProxy( BeanCreator.class, null );
    }

    
    @SuppressWarnings( "unchecked" )
    public < T extends SimpleBeanSafeToProxy > BeanUpdater< T > getUpdater( final Class< T > type )
    {
        return InterfaceProxyFactory.getProxy( BeanUpdater.class, null );
    }

    
    public BeanDeleter getDeleter( final Class< ? extends SimpleBeanSafeToProxy > type )
    {
        return InterfaceProxyFactory.getProxy( BeanDeleter.class, null );
    }

    
    public Set< BeansRetriever< ? > > getServices( final UnaryPredicate< Class< ? > > serviceFilter )
    {
        return new HashSet<>();
    }


    public BeansServiceManager startTransaction()
    {
        return new MockBeansServiceManager();
    }


    public void commitTransaction()
    {
        // empty
    }
    
    
    public void closeTransaction()
    {
        // empty
    }


    @Override public DatabasePhysicalSpaceState getDatabaseSpaceState()
    {
        return DatabasePhysicalSpaceState.NORMAL;
    }
    
    
    @Override public DatabasePhysicalSpaceState getFreeToTotalDiskSpaceRatioState()
    {
        return DatabasePhysicalSpaceState.NORMAL;
    }
    
    
    @Override public DatabasePhysicalSpaceState getMaxTableToFreeRatioState()
    {
        return DatabasePhysicalSpaceState.NORMAL;
    }
    
    
    public NotificationEventDispatcher getNotificationEventDispatcher()
    {
        return InterfaceProxyFactory.getProxy( NotificationEventDispatcher.class, null );
    }


    public boolean isTransaction()
    {
        return false;
    }


    public BeansServiceManager getTransactionSource()
    {
        throw new IllegalStateException( "Not a transaction." );
    }
    
    
    public void reserveConnections(
            final int numberOfAutoCommitConnections,
            final int numberOfExplicitCommitConnections )
    {
        // empty
    }
    
    
    public void releaseReservedConnections()
    {
        // empty
    }


	@Override
	public NestableTransaction startNestableTransaction()
	{
		throw new UnsupportedOperationException("Not yet implemented");
	}
}
