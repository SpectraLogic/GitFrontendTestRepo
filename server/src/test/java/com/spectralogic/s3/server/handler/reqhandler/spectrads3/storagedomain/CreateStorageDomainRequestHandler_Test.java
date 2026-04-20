/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.storagedomain;

import javax.servlet.http.HttpServletResponse;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;

public final class CreateStorageDomainRequestHandler_Test 
{
    @Test
    public void testCreateDoesNotDelegateToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN )
                .addParameter( NameObservable.NAME, "sd1" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        support.getDatabaseSupport().getServiceManager().getRetriever( StorageDomain.class ).attain(
                Require.nothing() );
        assertEquals(0,  support.getDataPolicyBtih().getTotalCallCount(), "Should notta invoked data planner rpc resource.");
        assertEquals(1,  support.getTapeInterfaceBtih().getTotalCallCount(), "Shoulda told tape rpc resource to refresh CRON triggers for auto-eject.");
    }
    
    
    @Test
    public void testCreateWithAutoEjectPoliciesPermittedProvidedMediaEjectionAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN )
                .addParameter( NameObservable.NAME, "sd1" )
                .addParameter( StorageDomain.MEDIA_EJECTION_ALLOWED, "true" )
                .addParameter( StorageDomain.AUTO_EJECT_UPON_JOB_CANCELLATION, "true" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        support.getDatabaseSupport().getServiceManager().getRetriever( StorageDomain.class ).attain(
                Require.nothing() );
        assertEquals(0,  support.getDataPolicyBtih().getTotalCallCount(), "Should notta invoked data planner rpc resource.");
    }
    
    
    @Test
    public void testCreateWithoutAutoEjectPoliciesPermittedWhenMediaEjectionDisallowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN )
                .addParameter( NameObservable.NAME, "sd1" )
                .addParameter( StorageDomain.MEDIA_EJECTION_ALLOWED, "false" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        support.getDatabaseSupport().getServiceManager().getRetriever( StorageDomain.class ).attain(
                Require.nothing() );
        assertEquals(0,  support.getDataPolicyBtih().getTotalCallCount(), "Should notta invoked data planner rpc resource.");
    }
    
    
    @Test
    public void testCreateWithAutoEjectUponJobCompletionNotAllowedIfMediaEjectionNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN )
                .addParameter( NameObservable.NAME, "sd1" )
                .addParameter( StorageDomain.MEDIA_EJECTION_ALLOWED, "false" )
                .addParameter( StorageDomain.AUTO_EJECT_UPON_JOB_COMPLETION, "true" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );

        assertEquals(0,  support.getDataPolicyBtih().getTotalCallCount(), "Should notta invoked data planner rpc resource.");
    }
    
    
    @Test
    public void testCreateWithAutoEjectUponJobCancellationNotAllowedIfMediaEjectionNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN )
                .addParameter( NameObservable.NAME, "sd1" )
                .addParameter( StorageDomain.MEDIA_EJECTION_ALLOWED, "false" )
                .addParameter( StorageDomain.AUTO_EJECT_UPON_JOB_CANCELLATION, "true" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );

        assertEquals(0,  support.getDataPolicyBtih().getTotalCallCount(), "Should notta invoked data planner rpc resource.");
    }
    
    
    @Test
    public void testCreateWithAutoEjectUponMediaFullNotAllowedIfMediaEjectionNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN )
                .addParameter( NameObservable.NAME, "sd1" )
                .addParameter( StorageDomain.MEDIA_EJECTION_ALLOWED, "false" )
                .addParameter( StorageDomain.AUTO_EJECT_UPON_MEDIA_FULL, "true" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );

        assertEquals(0,  support.getDataPolicyBtih().getTotalCallCount(), "Should notta invoked data planner rpc resource.");
    }
    
    
    @Test
    public void testCreateWithAutoEjectUponCronNotAllowedIfCronInvalid()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN )
                .addParameter( NameObservable.NAME, "sd1" )
                .addParameter( StorageDomain.MEDIA_EJECTION_ALLOWED, "false" )
                .addParameter( StorageDomain.AUTO_EJECT_UPON_CRON, "abc" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        assertEquals(0,  support.getDataPolicyBtih().getTotalCallCount(), "Should notta invoked data planner rpc resource.");
    }
    
    
    @Test
    public void testCreateWithAutoEjectUponCronNotAllowedIfMediaEjectionNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN )
                .addParameter( NameObservable.NAME, "sd1" )
                .addParameter( StorageDomain.MEDIA_EJECTION_ALLOWED, "false" )
                .addParameter( StorageDomain.AUTO_EJECT_UPON_CRON, "0 15 1 L * ?" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );

        assertEquals(0,  support.getDataPolicyBtih().getTotalCallCount(), "Should notta invoked data planner rpc resource.");
    }
    
    
    @Test
    public void testCreateWithMediaEjectionNotAllowedAndAutoEjectMediaFullThresholdAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN )
                .addParameter( NameObservable.NAME, "sd1" )
                .addParameter( StorageDomain.MEDIA_EJECTION_ALLOWED, "false" )
                .addParameter( StorageDomain.AUTO_EJECT_MEDIA_FULL_THRESHOLD, "99" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );

        assertEquals(0,  support.getDataPolicyBtih().getTotalCallCount(), "Should notta invoked data planner rpc resource.");
    }
    
    
    @Test
    public void testCreateWithMaxTapeFragmentationSetTooLowNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN )
                .addParameter( NameObservable.NAME, "sd1" )
                .addParameter( StorageDomain.MAX_TAPE_FRAGMENTATION_PERCENT, "9" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        assertEquals(0,  support.getDataPolicyBtih().getTotalCallCount(), "Should notta invoked data planner rpc resource.");
    }
    
    
    @Test
    public void testCreateWithMaxTapeFragmentationSetTooHighNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN )
                .addParameter( NameObservable.NAME, "sd1" )
                .addParameter( StorageDomain.MAX_TAPE_FRAGMENTATION_PERCENT, "101" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        assertEquals(0,  support.getDataPolicyBtih().getTotalCallCount(), "Should notta invoked data planner rpc resource.");
    }

    
    @Test
    public void testCreateMaxAutoVerificationNotSpecifiedSoWithDefaultValueReturns201()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN )
                    .addParameter( NameObservable.NAME, "sd1" );
        driver.run();
        driver.assertHttpResponseCodeEquals( HttpServletResponse.SC_CREATED );

        support.getDatabaseSupport().getServiceManager().getRetriever( StorageDomain.class ).attain(
                Require.nothing() );
        assertEquals(0,  support.getDataPolicyBtih().getTotalCallCount(), "Should notta invoked data planner rpc resource.");

        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final StorageDomain storageDomain = dbSupport.getServiceManager().getRetriever( 
                StorageDomain.class ).attain( NameObservable.NAME, "sd1" );
        assertNull(
                storageDomain.getMaximumAutoVerificationFrequencyInDays(),
                "Shoulda created the default."
                 );
    }

    
    @Test
    public void testCreateMaxAutoVerificationHasValidValueReturns201()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final int maximumAutoVerificationFrequencyInDays = 999999;
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN )
        .addParameter( NameObservable.NAME, "sd1" )
        .addParameter( StorageDomain.MAXIMUM_AUTO_VERIFICATION_FREQUENCY_IN_DAYS, 
                String.valueOf( maximumAutoVerificationFrequencyInDays ) );
        
        driver.run();
        driver.assertHttpResponseCodeEquals( HttpServletResponse.SC_CREATED );

        support.getDatabaseSupport().getServiceManager().getRetriever( StorageDomain.class ).attain(
                Require.nothing() );
        assertEquals(0,  support.getDataPolicyBtih().getTotalCallCount(), "Should notta invoked data planner rpc resource.");

        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final StorageDomain storageDomain = dbSupport.getServiceManager().getRetriever( 
                StorageDomain.class ).attain( NameObservable.NAME, "sd1" );
        assertEquals(maximumAutoVerificationFrequencyInDays,  storageDomain.getMaximumAutoVerificationFrequencyInDays().intValue(), "Shoulda updated storage domain.");
    }
    
    
    @Test
    public void testCreateMaxAutoVerificationHasNullValueAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN )
        .addParameter( NameObservable.NAME, "sd1" )
        .addParameter( StorageDomain.MAXIMUM_AUTO_VERIFICATION_FREQUENCY_IN_DAYS, "" );
        
        driver.run();
        driver.assertHttpResponseCodeEquals(
                "maximumAutoVerificationFrequencyInDays was passed in with a empty/null value,"
                        + " but since it is a primitive, we will not assume what the user intended.",
                        201 );
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final StorageDomain storageDomain = dbSupport.getServiceManager().getRetriever( 
                StorageDomain.class ).attain( NameObservable.NAME, "sd1" );
        assertNull(
                storageDomain.getMaximumAutoVerificationFrequencyInDays(),
                "Shoulda created storage domain with null auto verify."
                 );
    }
}
