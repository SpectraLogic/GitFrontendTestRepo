/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.pool;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.pool.ImportPoolDirective;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;

final class ImportPoolDirectiveServiceImpl
    extends BaseService< ImportPoolDirective >
    implements ImportPoolDirectiveService
{
    ImportPoolDirectiveServiceImpl()
    {
        super( ImportPoolDirective.class );
    }
    
    
    public ImportPoolDirective attainByEntityToImport( final UUID idOfEntityToImport )
    {
        return attain( ImportPoolDirective.POOL_ID, idOfEntityToImport );
    }
    
    
    public void deleteByEntityToImport( final UUID idOfEntityToImport )
    {
        getDataManager().deleteBeans(
                getServicedType(),
                Require.beanPropertyEquals( ImportPoolDirective.POOL_ID, idOfEntityToImport ) );
    }

    
    @Override
    public void deleteAll()
    {
        if ( 0 < getServiceManager().getRetriever( Pool.class ).getCount(
                Pool.STATE, PoolState.IMPORT_IN_PROGRESS ) )
        {
            throw new FailureTypeObservableException(
                    GenericFailure.CONFLICT,
                    "Can only delete all " + ImportPoolDirective.class.getSimpleName() 
                    + "s if no pools are import in progress." );
        }
        getDataManager().deleteBeans( ImportPoolDirective.class, Require.nothing() );
    }
}
