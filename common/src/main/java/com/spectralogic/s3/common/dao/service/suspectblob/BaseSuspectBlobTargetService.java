/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.suspectblob;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.SystemFailureType;
import com.spectralogic.s3.common.dao.domain.pool.SuspectBlobPool;
import com.spectralogic.s3.common.dao.domain.tape.SuspectBlobTape;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobDs3Target;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobS3Target;
import com.spectralogic.s3.common.dao.service.ds3.SystemFailureService;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;

public abstract class BaseSuspectBlobTargetService< T extends DatabasePersistable > extends BaseService< T >
{
    protected BaseSuspectBlobTargetService( final Class< T > clazz )
    {
        super( clazz );
    }
    
    
    public void delete( final Set< UUID > ids )
    {
        verifyInsideTransaction();
        if ( null == ids )
        {
            super.deleteAll();
        }
        else
        {
            final int existingBlobsCountForThisClassType = getDataManager().getCount( 
                    getServicedType(), Require.beanPropertyEqualsOneOf( Identifiable.ID, ids ) );
            if ( ids.size() != existingBlobsCountForThisClassType )
            {
                throw new FailureTypeObservableException(
                        GenericFailure.CONFLICT,
                        "Cannot perform delete since "
                                + ids.size() + " were specified, but only "
                                + existingBlobsCountForThisClassType 
                                + " exist of type "
                                + getServicedType().getSimpleName() + "." );
            }
             int numSuspectBlobS3TargetsBefore =
                    getDataManager().getCount( SuspectBlobDs3Target.class, Require.nothing() );
            getDataManager().deleteBeans(
                    getServicedType(),
                    Require.beanPropertyEqualsOneOf(Identifiable.ID, ids));
            int numSuspectBlobS3TargetsAfter =
                    getDataManager().getCount( SuspectBlobDs3Target.class, Require.nothing() );

            if ((numSuspectBlobS3TargetsBefore > numSuspectBlobS3TargetsAfter) || (numSuspectBlobS3TargetsAfter == 0)) {
                updateSuspectFailureExistence();
            }
        }
    }
    
    
    @Override
    public void create( final T bean )
    {
        super.create( bean );
        updateSuspectFailureExistence();
    }
    
    
    @Override
    public void create( final Set< T > beans )
    {
        removeExistentPersistedBeansFromSet( beans );
        super.create( beans );
        updateSuspectFailureExistence();
    }
    
    
    public void updateSuspectFailureExistence()
    {
        final int numSuspectBlobTapes =
                getDataManager().getCount( SuspectBlobTape.class, Require.nothing() );
        final int numSuspectBlobPools = 
                getDataManager().getCount( SuspectBlobPool.class, Require.nothing() );
        final int numSuspectBlobDs3Targets =
                getDataManager().getCount( SuspectBlobDs3Target.class, Require.nothing() );
        final int numSuspectBlobAzureTargets =
                getDataManager().getCount( SuspectBlobAzureTarget.class, Require.nothing() );
        final int numSuspectBlobS3Targets =
                getDataManager().getCount( SuspectBlobS3Target.class, Require.nothing() );
        
        if ( 0 == numSuspectBlobTapes + numSuspectBlobPools + numSuspectBlobDs3Targets
                  + numSuspectBlobAzureTargets + numSuspectBlobS3Targets )
        {
            LOG.info( "There are no suspect blobs at this time." );
            getServiceManager().getService( SystemFailureService.class ).deleteAll( 
                    SystemFailureType.CRITICAL_DATA_VERIFICATION_ERROR_REQUIRES_USER_CONFIRMATION );
        }
        else
        {
            final String message =
                    "Current suspect blobs: "
                    + numSuspectBlobTapes + " on tape, " 
                    + numSuspectBlobPools + " on pool, "
                    + numSuspectBlobDs3Targets + " on DS3 target, "
                    + numSuspectBlobAzureTargets + " on Azure target, and "
                    + numSuspectBlobS3Targets + " on S3 target.";
            LOG.warn( message );
            getServiceManager().getService( SystemFailureService.class ).create( 
                    SystemFailureType.CRITICAL_DATA_VERIFICATION_ERROR_REQUIRES_USER_CONFIRMATION, 
                    "A critical data verification error has been detected.  " 
                    + "Contact Spectra Support to verify data is properly written.  " + message,
                    Integer.valueOf( 60 * 24 ) );
        }
    }
}
