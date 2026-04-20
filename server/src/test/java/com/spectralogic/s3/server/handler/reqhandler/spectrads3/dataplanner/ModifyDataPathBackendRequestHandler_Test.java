/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.dataplanner;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.service.ds3.DataPathBackendService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

import static com.spectralogic.s3.server.handler.reqhandler.spectrads3.dataplanner.ModifyDataPathBackendRequestHandler.MAX_ACTIVE_JOB_LIMIT;
import static com.spectralogic.s3.server.handler.reqhandler.spectrads3.dataplanner.ModifyDataPathBackendRequestHandler.MIN_ACTIVE_JOB_LIMIT;
import static org.junit.jupiter.api.Assertions.*;

public final class ModifyDataPathBackendRequestHandler_Test 
{
    @Test
    public void testModifyDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
            .addParameter( DataPathBackend.AUTO_ACTIVATE_TIMEOUT_IN_MINS, "44" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        assertEquals(44,  support.getDatabaseSupport().getServiceManager().getService(
                        DataPathBackendService.class).attain(Require.nothing())
                .getAutoActivateTimeoutInMins().intValue(), "Shoulda updated bean.");
    }
    
    
    @Test
    public void testModifyAutoActivateHasToBePositive()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
                .addParameter( DataPathBackend.AUTO_ACTIVATE_TIMEOUT_IN_MINS, "-44" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        driver.assertResponseToClientContains( DataPathBackend.AUTO_ACTIVATE_TIMEOUT_IN_MINS );
        driver.assertResponseToClientContains( "has to be greater or equal to zero if specified" );

        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
                .addParameter( DataPathBackend.AUTO_ACTIVATE_TIMEOUT_IN_MINS, "0" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

    }

    
    @Test
    public void testModifyAutoActivateHasToBePositiveDoesNotBreakOldCode()
    {
        final String autoActivateAsNotANumber = "string should not break the existing code";
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
            .addParameter( DataPathBackend.AUTO_ACTIVATE_TIMEOUT_IN_MINS, 
                    autoActivateAsNotANumber);
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        driver.assertResponseToClientContains( "NumberFormatException");
        driver.assertResponseToClientContains( autoActivateAsNotANumber);
    }
    

    @Test
    public void testDeactivateBackendNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        assertTrue(support.getDatabaseSupport().getServiceManager().getService(
                        DataPathBackendService.class ).isActivated(), "Shoulda initialized bean.");

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
            .addParameter( DataPathBackend.ACTIVATED, "false" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );
        assertTrue(support.getDatabaseSupport().getServiceManager().getService(
                        DataPathBackendService.class ).isActivated(), "Should notta modified bean.");
    }
    
    
    @Test
    public void testVerifyBothPriorAndAfterNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
       
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
                        .addParameter( DataPathBackend.DEFAULT_VERIFY_DATA_PRIOR_TO_IMPORT, "true" )
                        .addParameter(
                                DataPathBackend.DEFAULT_VERIFY_DATA_AFTER_IMPORT,
                                BlobStoreTaskPriority.values()[ 1 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
       
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
                        .addParameter( DataPathBackend.DEFAULT_VERIFY_DATA_PRIOR_TO_IMPORT, "true" )
                        .addParameter(
                                DataPathBackend.DEFAULT_VERIFY_DATA_AFTER_IMPORT,
                                "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
       
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
                        .addParameter( DataPathBackend.DEFAULT_VERIFY_DATA_PRIOR_TO_IMPORT, "false" )
                        .addParameter(
                                DataPathBackend.DEFAULT_VERIFY_DATA_AFTER_IMPORT,
                                BlobStoreTaskPriority.values()[ 1 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }
    
    
    @Test
    public void testModifyPartiallyVerifyLastPercentOfTapesWorksAndValidatesInputCorrectly()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        assertNull(
                mockDaoDriver.attainOneAndOnly( DataPathBackend.class )
                        .getPartiallyVerifyLastPercentOfTapes(),
                "By default, we shoulda gone with full verifies."
                 );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
            .addParameter( DataPathBackend.PARTIALLY_VERIFY_LAST_PERCENT_OF_TAPES, "0" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        driver.assertResponseToClientContains( DataPathBackend.PARTIALLY_VERIFY_LAST_PERCENT_OF_TAPES );
        assertNull(
                mockDaoDriver.attainOneAndOnly( DataPathBackend.class )
                        .getPartiallyVerifyLastPercentOfTapes(),
                "Bad request should notta changed configured value."
                 );
        
        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
            .addParameter( DataPathBackend.PARTIALLY_VERIFY_LAST_PERCENT_OF_TAPES, "101" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        driver.assertResponseToClientContains( DataPathBackend.PARTIALLY_VERIFY_LAST_PERCENT_OF_TAPES );
        assertNull(
                mockDaoDriver.attainOneAndOnly( DataPathBackend.class )
                        .getPartiallyVerifyLastPercentOfTapes(),
                "Bad request should notta changed configured value."
                 );

        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
            .addParameter( DataPathBackend.PARTIALLY_VERIFY_LAST_PERCENT_OF_TAPES, "1" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertEquals(1,  mockDaoDriver.attainOneAndOnly(DataPathBackend.class)
                .getPartiallyVerifyLastPercentOfTapes().intValue(), "Shoulda honored request to change value.");

        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
            .addParameter( DataPathBackend.PARTIALLY_VERIFY_LAST_PERCENT_OF_TAPES, "100" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertNull(
                mockDaoDriver.attainOneAndOnly( DataPathBackend.class )
                        .getPartiallyVerifyLastPercentOfTapes(),
                "Setting to 100 shoulds configured full verifies."
                );
        
        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
            .addParameter( DataPathBackend.PARTIALLY_VERIFY_LAST_PERCENT_OF_TAPES, "99" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertEquals(99,  mockDaoDriver.attainOneAndOnly(DataPathBackend.class)
                .getPartiallyVerifyLastPercentOfTapes().intValue(), "Shoulda honored request to change value.");

        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
            .addParameter( DataPathBackend.PARTIALLY_VERIFY_LAST_PERCENT_OF_TAPES, "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertEquals(null,  mockDaoDriver.attainOneAndOnly(DataPathBackend.class)
                .getPartiallyVerifyLastPercentOfTapes(), "Shoulda honored request to change value.");
    }

    @Test
    public void testModifyIomCacheLimitationPercentWorksAndValidatesInputCorrectly()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        assertEquals(0.5,  mockDaoDriver.attainOneAndOnly(DataPathBackend.class)
                .getIomCacheLimitationPercent(), "By default, value should be 0.5");

        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
                .addParameter( DataPathBackend.IOM_CACHE_LIMITATION_PERCENT, "-1.5" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        driver.assertResponseToClientContains( DataPathBackend.IOM_CACHE_LIMITATION_PERCENT );
        assertEquals(0.5,  mockDaoDriver.attainOneAndOnly(DataPathBackend.class)
                .getIomCacheLimitationPercent(), "Bad request should not have changed configured value on negative value");

        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
                .addParameter( DataPathBackend.IOM_CACHE_LIMITATION_PERCENT, "0" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        driver.assertResponseToClientContains( DataPathBackend.IOM_CACHE_LIMITATION_PERCENT );
        assertEquals(0.5,  mockDaoDriver.attainOneAndOnly(DataPathBackend.class)
                .getIomCacheLimitationPercent(), "Bad request should not have changed configured value on zero");

        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
                .addParameter( DataPathBackend.IOM_CACHE_LIMITATION_PERCENT, "1.01" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        driver.assertResponseToClientContains( DataPathBackend.IOM_CACHE_LIMITATION_PERCENT );
        assertEquals(0.5,  mockDaoDriver.attainOneAndOnly(DataPathBackend.class)
                .getIomCacheLimitationPercent(), "Bad request should not have changed configured value on value over 1.0");

        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
                .addParameter( DataPathBackend.IOM_CACHE_LIMITATION_PERCENT, "0.75" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertEquals(0.75,  mockDaoDriver.attainOneAndOnly(DataPathBackend.class)
                .getIomCacheLimitationPercent(), "Should have honored request to change value.");

        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
                .addParameter( DataPathBackend.IOM_CACHE_LIMITATION_PERCENT, "1" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertEquals(1.0,  mockDaoDriver.attainOneAndOnly(DataPathBackend.class)
                .getIomCacheLimitationPercent(), "Setting to full value of 1 should verify.");

        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
                .addParameter( DataPathBackend.IOM_CACHE_LIMITATION_PERCENT, "0.0001" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertEquals(0.0001,  mockDaoDriver.attainOneAndOnly(DataPathBackend.class)
                .getIomCacheLimitationPercent(), "Setting to minimal value should verify.");
    }


    @Test
    public void testModifyMaxAggregatedBlobsPerChunkWorksAndValidatesInputCorrectly()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        assertEquals(20000,  mockDaoDriver.attainOneAndOnly(DataPathBackend.class)
                .getMaxAggregatedBlobsPerChunk(), "By default, value should be 20,000");

        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
                .addParameter( DataPathBackend.MAX_AGGREGATED_BLOBS_PER_CHUNK, "-10" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        driver.assertResponseToClientContains( DataPathBackend.MAX_AGGREGATED_BLOBS_PER_CHUNK );
        assertEquals(20000,  mockDaoDriver.attainOneAndOnly(DataPathBackend.class)
                .getMaxAggregatedBlobsPerChunk(), "Bad request should not have changed configured value on negative value");

        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
                .addParameter( DataPathBackend.MAX_AGGREGATED_BLOBS_PER_CHUNK, "500001" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        driver.assertResponseToClientContains( DataPathBackend.MAX_AGGREGATED_BLOBS_PER_CHUNK );
        assertEquals(20000,  mockDaoDriver.attainOneAndOnly(DataPathBackend.class)
                .getMaxAggregatedBlobsPerChunk(), "Bad request should not have changed configured value on value over max of 500,000");

        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
                .addParameter( DataPathBackend.MAX_AGGREGATED_BLOBS_PER_CHUNK, "0" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertEquals(0,  mockDaoDriver.attainOneAndOnly(DataPathBackend.class)
                .getMaxAggregatedBlobsPerChunk(), "Should have honored request to change value to zero.");

        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
                .addParameter( DataPathBackend.MAX_AGGREGATED_BLOBS_PER_CHUNK, "1" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertEquals(1,  mockDaoDriver.attainOneAndOnly(DataPathBackend.class)
                .getMaxAggregatedBlobsPerChunk(), "Should have honored request to change value to one.");

        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
                .addParameter( DataPathBackend.MAX_AGGREGATED_BLOBS_PER_CHUNK, "500000" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertEquals(500000,  mockDaoDriver.attainOneAndOnly(DataPathBackend.class)
                .getMaxAggregatedBlobsPerChunk(), "Setting to full value of 500,000 should verify.");

        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
                .addParameter( DataPathBackend.MAX_AGGREGATED_BLOBS_PER_CHUNK, "250555" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertEquals(250555,  mockDaoDriver.attainOneAndOnly(DataPathBackend.class)
                .getMaxAggregatedBlobsPerChunk(), "Setting to intermediate value should verify.");
    }


    @Test
    public void testSetMaxNumberOfConcurrentJobsTooLowNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        assertTrue(support.getDatabaseSupport().getServiceManager().getService(
                        DataPathBackendService.class ).isActivated(), "Shoulda initialized bean.");

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
                .addParameter( DataPathBackend.MAX_NUMBER_OF_CONCURRENT_JOBS, String.valueOf(MIN_ACTIVE_JOB_LIMIT-1));
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        assertEquals(40000,  support.getDatabaseSupport().getServiceManager().getService(
                        DataPathBackendService.class).attain(Require.nothing())
                .getMaxNumberOfConcurrentJobs(), "Shoulda updated bean.");
    }


    @Test
    public void testSetMaxNumberOfConcurrentJobsTooHighNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        assertTrue(support.getDatabaseSupport().getServiceManager().getService(
                        DataPathBackendService.class ).isActivated(), "Shoulda initialized bean.");

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
                .addParameter( DataPathBackend.MAX_NUMBER_OF_CONCURRENT_JOBS, String.valueOf(MAX_ACTIVE_JOB_LIMIT+1));
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        assertEquals(40000,  support.getDatabaseSupport().getServiceManager().getService(
                        DataPathBackendService.class).attain(Require.nothing())
                .getMaxNumberOfConcurrentJobs(), "Shoulda updated bean.");
    }


    @Test
    public void testSetMaxNumberOfConcurrentJobsToMinAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        assertTrue(support.getDatabaseSupport().getServiceManager().getService(
                        DataPathBackendService.class ).isActivated(), "Shoulda initialized bean.");

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
                .addParameter( DataPathBackend.MAX_NUMBER_OF_CONCURRENT_JOBS, String.valueOf(MIN_ACTIVE_JOB_LIMIT));
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Object actual = support.getDatabaseSupport().getServiceManager().getService(
                        DataPathBackendService.class ).attain( Require.nothing() )
                .getMaxNumberOfConcurrentJobs();
        assertEquals(MIN_ACTIVE_JOB_LIMIT, actual, "Shoulda updated bean.");
    }


    @Test
    public void testSetMaxNumberOfConcurrentJobsToMaxAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        assertTrue(support.getDatabaseSupport().getServiceManager().getService(
                        DataPathBackendService.class ).isActivated(), "Shoulda initialized bean.");

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.DATA_PATH_BACKEND )
                .addParameter( DataPathBackend.MAX_NUMBER_OF_CONCURRENT_JOBS, String.valueOf(MAX_ACTIVE_JOB_LIMIT));
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Object actual = support.getDatabaseSupport().getServiceManager().getService(
                        DataPathBackendService.class ).attain( Require.nothing() )
                .getMaxNumberOfConcurrentJobs();
        assertEquals(MAX_ACTIVE_JOB_LIMIT, actual, "Shoulda updated bean.");
    }
}
