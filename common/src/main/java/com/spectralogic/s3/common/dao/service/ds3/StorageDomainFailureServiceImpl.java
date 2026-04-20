/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainFailure;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainFailureType;
import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.s3.common.dao.service.shared.ActiveFailures;
import com.spectralogic.s3.common.dao.service.shared.ActiveFailuresImpl;
import com.spectralogic.s3.common.dao.service.shared.BaseFailureService;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.ExceptionUtil;

final class StorageDomainFailureServiceImpl 
    extends BaseFailureService< StorageDomainFailure > implements StorageDomainFailureService
{
    StorageDomainFailureServiceImpl()
    {
        super( StorageDomainFailure.class );
    }
    
    
    public void create( 
            final UUID storageDomainId, 
            final StorageDomainFailureType type,
            final Throwable t,
            final Integer minMinutesSinceLastFailureOfSameType )
    {
        create( 
                storageDomainId, 
                type, 
                ExceptionUtil.getReadableMessage( t ),
                minMinutesSinceLastFailureOfSameType );
    }
    
    
    public void create( 
            final UUID storageDomainId, 
            final StorageDomainFailureType type,
            final String error,
            final Integer minMinutesSinceLastFailureOfSameType )
    {
        create( BeanFactory.newBean( StorageDomainFailure.class )
                .setErrorMessage( error )
                .setStorageDomainId( storageDomainId )
                .setType( type ), minMinutesSinceLastFailureOfSameType );
    }
    
    
    public void deleteAll( final UUID storageDomainId, final StorageDomainFailureType type )
    {
        deleteAll( Require.all( 
                Require.beanPropertyEquals( StorageDomainFailure.STORAGE_DOMAIN_ID, storageDomainId ),
                Require.beanPropertyEquals( Failure.TYPE, type ) ) );
    }
    
    
    public void deleteAll( final UUID storageDomainId )
    {
        deleteAll( Require.beanPropertyEquals( StorageDomainFailure.STORAGE_DOMAIN_ID, storageDomainId ) );
    }
    
    
    public ActiveFailures startActiveFailures( 
            final UUID storageDomainId,
            final StorageDomainFailureType type )
    {
        return new ActiveFailuresImpl<>(
                this,
                BeanFactory.newBean( StorageDomainFailure.class )
                    .setStorageDomainId( storageDomainId ).setType( type ) ,6000);
    }
}
