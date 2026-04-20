/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.ds3target;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.TargetState;
import com.spectralogic.s3.common.dao.service.target.Ds3TargetService;
import com.spectralogic.s3.common.rpc.target.Ds3Connection;
import com.spectralogic.s3.common.rpc.target.Ds3ConnectionFactory;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.cache.CacheResultProvider;
import com.spectralogic.util.cache.MutableCache;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetrieverManager;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.exception.FailureTypeObservable;
import com.spectralogic.util.lang.Validations;

import ds3fatjar.org.apache.commons.codec.binary.Base64;

public final class DefaultDs3ConnectionFactory implements Ds3ConnectionFactory
{
    public DefaultDs3ConnectionFactory( final BeansServiceManager serviceManager )
    {
        Validations.verifyNotNull( "Service manager", serviceManager );
        m_service = serviceManager.getService( Ds3TargetService.class );
        m_brm = serviceManager;
    }
    
    
    public Ds3Connection discover( final Ds3Target target )
    {
        Validations.verifyNotNull( "Target", target );
        
        Ds3Connection retval = null;
        try
        {
            retval = connectInternal( target );
        }
        catch ( final Exception ex )
        {
            Validations.verifyNotNull( "First attempt could fail.", ex );
            target.setAdminAuthId( Base64.encodeBase64String( target.getAdminAuthId().getBytes() ) );
           try
           {
               retval = connectInternal( target );
           }
           catch ( final Exception ex2 ) 
           {
               if ( FailureTypeObservable.class.isAssignableFrom( ex2.getClass() ) 
                       && ( (FailureTypeObservable)ex2 ).getFailureType().getHttpResponseCode() == 403 )
               {
                   throw ex;
               }
               throw ex2;
           }
        }
        
        verifyIsAdministrator( retval, target );
        
        return retval;
    }
    
    
    public Ds3Connection connect( final UUID userId, final Ds3Target target )
    {
        Validations.verifyNotNull( "User", m_service );
        Validations.verifyNotNull( "Target", target );
        
        if ( null == userId )
        {
            final Ds3Connection retval = connectInternal( target );
            verifyIsAdministrator( retval, target );
            return retval;
        }
        
        final User user = m_brm.getRetriever( User.class ).attain( userId );
        final Ds3Target param = BeanFactory.newBean( Ds3Target.class );
        BeanCopier.copy( param, target );
        param.setAdminAuthId( user.getAuthId() );
        param.setAdminSecretKey( user.getSecretKey() );
        return connectInternal( param );
    }
    
    
    private Ds3Connection connectInternal( final Ds3Target target )
    {
        Ds3Connection retval = null;
        try
        {
            retval = new Ds3ConnectionImpl( 
                    m_sourceInstanceIdCache.get( null ),
                    target, 
                    target.getAdminAuthId(),
                    target.getAdminSecretKey() );
        }
        catch ( final Exception ex )
        {
            //NOTE: normally we wouldn't check if ID is null, but registerDs3Target explicitly sets it null
            if (target.getId() != null
                    && m_service.retrieve(target.getId()) != null
                    && TargetState.OFFLINE != target.getState()) {
                m_service.update(target.setState(TargetState.OFFLINE), ReplicationTarget.STATE);
            }
            throw ExceptionUtil.toRuntimeException( ex );
        }
        
        return retval;
    }
    
    
    private void verifyIsAdministrator( final Ds3Connection connection, final Ds3Target target )
    {
        try
        {
            connection.verifyIsAdministrator();
        }
        catch ( final Exception ex )
        {
            if ( TargetState.OFFLINE != target.getState() )
            {
                m_service.update( target.setState( TargetState.OFFLINE ), ReplicationTarget.STATE );
            }
            connection.shutdown();
            throw ExceptionUtil.toRuntimeException( ex );
        }
        
        if ( TargetState.ONLINE != target.getState() )
        {
            m_service.update( target.setState( TargetState.ONLINE ), ReplicationTarget.STATE );
        }
    }
    
    
    private final class SourceInstanceIdCacheResultProvider implements CacheResultProvider< Object, UUID >
    {
        public UUID generateCacheResultFor( final Object param )
        {
            return m_brm.getRetriever( DataPathBackend.class ).attain( Require.nothing() ).getInstanceId();
        }
    } // end inner class def
    
    
    private final Ds3TargetService m_service;
    private final BeansRetrieverManager m_brm;
    private final MutableCache< Object, UUID > m_sourceInstanceIdCache = new MutableCache<>( 
            1000, new SourceInstanceIdCacheResultProvider() );
}
