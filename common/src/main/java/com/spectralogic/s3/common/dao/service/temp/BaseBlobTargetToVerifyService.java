/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.temp;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.target.BlobTarget;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.iterate.EnhancedIterable;

/**
 * Any client verifying a blob target must take a global application-layer lock to do so.  This service is
 * specifically intended not to be thread-safe or safe to use across multiple targets being verified 
 * concurrently.  This service should only be used under the context of a global lock.
 */
abstract class BaseBlobTargetToVerifyService
    < BT extends BlobTarget< ? > & DatabasePersistable, 
       V extends BlobTarget< ? > & DatabasePersistable,
      SB extends BlobTarget< ? > & DatabasePersistable >
    extends BaseService< V > implements BlobTargetToVerifyService< V >
{
    protected BaseBlobTargetToVerifyService( 
            final Class< BT > blobTargetType,
            final Class< V > blobTargetToVerifyType,
            final Class< SB > suspectBlobTargetType )
    {
        super( blobTargetToVerifyType );
        m_blobTargetType = blobTargetType;
        m_suspectBlobTargetType = suspectBlobTargetType;
        Validations.verifyNotNull( "Blob target type", m_blobTargetType );
        Validations.verifyNotNull( "Suspect blob target type", m_suspectBlobTargetType );
    }
    
    
    final public void verifyBegun( final UUID targetId )
    {
        Validations.verifyNotNull( "Target id", targetId );
        getDataManager().deleteBeans( getServicedType(), Require.nothing() );
        try ( final EnhancedIterable< BT > iterable = 
                getServiceManager().getRetriever( m_blobTargetType ).retrieveAll( Require.beanPropertyEquals(
                        BlobTarget.TARGET_ID, targetId ) ).toIterable() )
        {
            final Set< V > blobTargetsToVerify = new HashSet<>();
            for ( final BT bt : iterable )
            {
                final V bean = BeanFactory.newBean( getServicedType() );
                BeanCopier.copy( bean, bt );
                blobTargetsToVerify.add( bean );
                if ( 10000 == blobTargetsToVerify.size() )
                {
                    createBlobTargetsToVerify( blobTargetsToVerify );
                    blobTargetsToVerify.clear();
                }
            }
            createBlobTargetsToVerify( blobTargetsToVerify );
        }
    }
    
    
    private void createBlobTargetsToVerify( final Set< V > beans )
    {
        if ( beans.isEmpty() )
        {
            return;
        }
        
        final DataManager transaction = getDataManager().startTransaction();
        try
        {
            transaction.createBeans( beans );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
    }


    final public Set< UUID > blobsVerified( final UUID targetId, final Set< UUID > blobIds )
    {
        Validations.verifyNotNull( "Target id", targetId );
        Validations.verifyNotNull( "Blob ids", blobIds );
        getDataManager().deleteBeans( 
                getServicedType(), 
                Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, blobIds ) );
        
        final Set< UUID > retval = new HashSet<>( blobIds );
        retval.removeAll( BeanUtils.< UUID >extractPropertyValues( 
                getServiceManager().getRetriever( m_blobTargetType ).retrieveAll( Require.all( 
                        Require.beanPropertyEquals( BlobTarget.TARGET_ID, targetId ),
                        Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, blobIds ) ) ).toSet(),
                BlobObservable.BLOB_ID ) );
        return retval;
    }


    final public void verifyCompleted( final UUID targetId )
    {
        Validations.verifyNotNull( "Target id", targetId );
        try ( final EnhancedIterable< V > iterable = 
                getServiceManager().getRetriever( getServicedType() ).retrieveAll( 
                        Require.nothing() ).toIterable() )
        {
            final Set< SB > suspectBlobTargets = new HashSet<>();
            for ( final V btv : iterable )
            {
                final SB bean = BeanFactory.newBean( m_suspectBlobTargetType );
                BeanCopier.copy( bean, btv );
                suspectBlobTargets.add( bean );
                if ( 10000 == suspectBlobTargets.size() )
                {
                    registerSuspectBlobs( suspectBlobTargets );
                    suspectBlobTargets.clear();
                }
            }
            registerSuspectBlobs( suspectBlobTargets );
        }
        getDataManager().deleteBeans( getServicedType(), Require.nothing() );
    }
    
    
    private void registerSuspectBlobs( final Set< SB > suspectBlobTargets )
    {
        if ( suspectBlobTargets.isEmpty() )
        {
            return;
        }
        
        final BeansServiceManager transaction = getServiceManager().startTransaction();
        try
        {
            transaction.getCreator( m_suspectBlobTargetType ).create( suspectBlobTargets );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
    }


    private final Class< BT > m_blobTargetType;
    private final Class< SB > m_suspectBlobTargetType;
}
