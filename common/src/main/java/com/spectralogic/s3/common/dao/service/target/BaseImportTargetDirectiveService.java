/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.ImportPublicCloudTargetDirective;
import com.spectralogic.s3.common.dao.service.shared.ImportDirectiveService;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BaseService;

abstract class BaseImportTargetDirectiveService
    < T extends DatabasePersistable & ImportPublicCloudTargetDirective< T > > 
    extends BaseService< T > implements ImportDirectiveService< T >
{
    protected BaseImportTargetDirectiveService( final Class< T > clazz )
    {
        super( clazz );
    }
    
    
    public T attainByEntityToImport( final UUID idOfEntityToImport )
    {
        return attain( ImportPublicCloudTargetDirective.TARGET_ID, idOfEntityToImport );
    }

    
    public void deleteByEntityToImport( final UUID idOfEntityToImport )
    {
        getDataManager().deleteBeans(
                getServicedType(),
                Require.beanPropertyEquals(
                        ImportPublicCloudTargetDirective.TARGET_ID,
                        idOfEntityToImport ) );
    }
}
