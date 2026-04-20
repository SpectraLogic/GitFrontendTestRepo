/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.storagedomain;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.http.RequestType;

public final class ModifyStorageDomainRequestHandler_Test 
{
    @Test
    public void testModifyDoesDelegateToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN + "/" + storageDomain.getName() )
            .addParameter( NameObservable.NAME, "newname" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        support.getDatabaseSupport().getServiceManager().getRetriever( StorageDomain.class ).attain(
                Require.nothing() );
        assertEquals("newname", support.getDatabaseSupport().getServiceManager().getRetriever(
                        StorageDomain.class ).attain( Require.nothing() ).getName(), "Shoulda updated bean.");
        assertEquals(1,  support.getDataPolicyBtih().getTotalCallCount(), "Shoulda invoked data planner rpc resource.");
        assertEquals(1,  support.getTapeInterfaceBtih().getTotalCallCount(), "Shoulda told tape rpc resource to refresh CRON triggers for auto-eject.");
    }
    
    
    @Test
    public void testModifyToCreateViolationBetweenAutoEjectTriggersAndMediaEjectionPolicyNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN + "/" + storageDomain.getName() )
            .addParameter( StorageDomain.MEDIA_EJECTION_ALLOWED, "false" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN + "/" + storageDomain.getName() )
            .addParameter( StorageDomain.AUTO_EJECT_UPON_CRON, "0 15 1 L * ?" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );
        
        support.getDatabaseSupport().getServiceManager().getRetriever( StorageDomain.class ).attain(
                Require.nothing() );
        assertEquals(null, support.getDatabaseSupport().getServiceManager().getRetriever(
                        StorageDomain.class ).attain( Require.nothing() ).getAutoEjectUponCron(), "Should notta updated bean.");
        assertEquals(1,  support.getDataPolicyBtih().getTotalCallCount(), "Shoulda invoked data planner rpc resource.");
    }
    
    
    @Test
    public void testModifyAutoEjectUponStringFromNonNullToNullWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN + "/" + storageDomain.getName() )
            .addParameter( StorageDomain.AUTO_EJECT_UPON_CRON, "0 15 1 L * ?" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertEquals("0 15 1 L * ?", support.getDatabaseSupport().getServiceManager().getRetriever( StorageDomain.class ).attain(
                        Require.nothing() ).getAutoEjectUponCron(), "Shoulda updated storage domain.");

        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN + "/" + storageDomain.getName() )
            .addParameter( StorageDomain.AUTO_EJECT_UPON_CRON, "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertEquals(null, support.getDatabaseSupport().getServiceManager().getRetriever( StorageDomain.class ).attain(
                        Require.nothing() ).getAutoEjectUponCron(), "Shoulda updated storage domain.");
    }
    

    @Test
    public void testModifyMaxAutoVerificationWithValidValuesRet200ThenInvalidRet400()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );

        assertNull(
                support.getDatabaseSupport().getServiceManager().getRetriever( StorageDomain.class ).attain(
                        Require.nothing() ).getMaximumAutoVerificationFrequencyInDays(),
                "Shoulda be the default which for this is null."
                 );
        
        final int maximumAutoVerificationFrequencyInDays = 999999;
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN + "/" + storageDomain.getName() )
                .addParameter( StorageDomain.MAXIMUM_AUTO_VERIFICATION_FREQUENCY_IN_DAYS, 
                        String.valueOf( maximumAutoVerificationFrequencyInDays ));
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertEquals(maximumAutoVerificationFrequencyInDays,  support.getDatabaseSupport().getServiceManager().getRetriever(StorageDomain.class).attain(
                Require.nothing()).getMaximumAutoVerificationFrequencyInDays().intValue(), "Shoulda updated the auto verification frequency.");

        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN + "/" + storageDomain.getName() )
        .addParameter( StorageDomain.MAXIMUM_AUTO_VERIFICATION_FREQUENCY_IN_DAYS, "" );
        driver.run();
        assertNull(
                support.getDatabaseSupport().getServiceManager().getRetriever( StorageDomain.class ).attain(
                        Require.nothing() ).getMaximumAutoVerificationFrequencyInDays(),
                "Shoulda set value to null."
                 );
    }        
}
