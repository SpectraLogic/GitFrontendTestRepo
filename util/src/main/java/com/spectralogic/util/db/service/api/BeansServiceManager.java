/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.util.db.service.api;

import java.util.Set;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.manager.DatabasePhysicalSpaceState;
import com.spectralogic.util.notification.dispatch.NotificationEventDispatcher;
import com.spectralogic.util.predicate.UnaryPredicate;

public interface BeansServiceManager extends BeansRetrieverManager
{
    public < T extends BeansRetriever< ? > > T getService( final Class< T > service );

    
    public < T extends SimpleBeanSafeToProxy > BeanCreator< T > getCreator( final Class< T > type );

    
    public < T extends SimpleBeanSafeToProxy > BeanUpdater< T > getUpdater( final Class< T > type );

    
    public BeanDeleter getDeleter( final Class< ? extends SimpleBeanSafeToProxy > type );
    
    
    public Set< BeansRetriever< ? > > getServices( final UnaryPredicate< Class< ? > > serviceFilter );
    
    
    public NotificationEventDispatcher getNotificationEventDispatcher();
    
    
    BeansServiceManager startTransaction();
    
    
    void commitTransaction();
    
    
    void closeTransaction();
    
    
    public NestableTransaction startNestableTransaction();
    
    
    DatabasePhysicalSpaceState getFreeToTotalDiskSpaceRatioState();

    DatabasePhysicalSpaceState getMaxTableToFreeRatioState();
    
    DatabasePhysicalSpaceState getDatabaseSpaceState();
    
    boolean isTransaction();
    
    
    BeansServiceManager getTransactionSource();
    
    
    /**
     * Client code that must open SQL connections in a nested manner from within an open SQL connection must
     * use this method to reserve all connections that may be needed in advance, to ensure no live lock is
     * introduced.  In general, such nested designs should be avoided.  <br><br>
     * 
     * @param numberOfAutoCommitConnections     - The number of SQL connections to reserve for this thread on
     *                                            the auto-commit connection pool
     *                                            
     * @param numberOfExplicitCommitConnections - The number of SQL connections to reserve for this thread on
     *                                            the explicit-commit connection pool
     */
    void reserveConnections(
            final int numberOfAutoCommitConnections,
            final int numberOfExplicitCommitConnections );
    
    
    /**
     * Release the reserved SQL connections for this thread
     */
    void releaseReservedConnections();
}
