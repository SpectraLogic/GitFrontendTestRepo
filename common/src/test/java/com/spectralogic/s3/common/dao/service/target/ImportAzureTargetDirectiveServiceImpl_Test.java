/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;

public final class ImportAzureTargetDirectiveServiceImpl_Test 
{
    @Test
    public void testDeleteByBeanPropertyDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final AzureTarget target1 = mockDaoDriver.createAzureTarget( null );
        final AzureTarget target2 = mockDaoDriver.createAzureTarget( null );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( 
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.createImportAzureTargetDirective( 
                target1.getId(), user.getId(), dataPolicy.getId(), null );
        mockDaoDriver.createImportAzureTargetDirective( 
                target2.getId(), user.getId(), dataPolicy.getId(), null );
        
        final ImportAzureTargetDirectiveService service =
                dbSupport.getServiceManager().getService( ImportAzureTargetDirectiveService.class );
        service.deleteByEntityToImport( target1.getId() );
        
        assertEquals(
                1,
                service.getCount(),
                "Shoulda whacked single directive."
                );
    }
}
