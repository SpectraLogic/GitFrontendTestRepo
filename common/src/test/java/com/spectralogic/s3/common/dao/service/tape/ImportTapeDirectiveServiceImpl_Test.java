/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.tape;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class ImportTapeDirectiveServiceImpl_Test 
{
    @Test
    public void testDeleteByBeanPropertyDoesSo()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( 
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.createImportTapeDirective( tape1.getId(), user.getId(), dataPolicy.getId() );
        mockDaoDriver.createImportTapeDirective( tape2.getId(), user.getId(), dataPolicy.getId() );
        
        final ImportTapeDirectiveService service =
                dbSupport.getServiceManager().getService( ImportTapeDirectiveService.class );
        service.deleteByEntityToImport( tape1.getId() );

        assertEquals(1,  service.getCount(), "Shoulda whacked single directive.");
    }
    
    
    @Test
    public void testDeleteAllWhenTapesExistThatAreImportPendingNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.updateBean( tape.setState( TapeState.IMPORT_PENDING ), Tape.STATE );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( 
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );
        
        final ImportTapeDirectiveService service =
                dbSupport.getServiceManager().getService( ImportTapeDirectiveService.class );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
                {
                    service.deleteAll();
                }
            } );

        assertEquals(1,  service.getCount(), "Should notta whacked any directives.");
    }
    
    
    @Test
    public void testDeleteAllWhenTapesExistThatAreImportInProgressNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.updateBean( tape.setState( TapeState.IMPORT_IN_PROGRESS ), Tape.STATE );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( 
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );
        
        final ImportTapeDirectiveService service =
                dbSupport.getServiceManager().getService( ImportTapeDirectiveService.class );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
                {
                    service.deleteAll();
                }
            } );

        assertEquals(1,  service.getCount(), "Should notta whacked any directives.");
    }
    
    
    @Test
    public void testDeleteAllWhenTapesExistThatAreForeignAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.updateBean( tape.setState( TapeState.FOREIGN ), Tape.STATE );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( 
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );
        
        final ImportTapeDirectiveService service =
                dbSupport.getServiceManager().getService( ImportTapeDirectiveService.class );
        service.deleteAll();

        assertEquals(0,  service.getCount(), "Shoulda whacked any directives.");
    }
}
