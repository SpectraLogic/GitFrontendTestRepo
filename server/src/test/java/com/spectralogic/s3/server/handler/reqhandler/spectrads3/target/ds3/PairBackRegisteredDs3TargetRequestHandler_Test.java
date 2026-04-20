/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.ds3;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.domain.ds3.Node;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.Ds3TargetAccessControlReplication;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.TargetReadPreferenceType;
import com.spectralogic.s3.common.dao.domain.target.TargetState;
import com.spectralogic.s3.common.dao.service.ds3.NodeService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.http.RequestType;

public final class PairBackRegisteredDs3TargetRequestHandler_Test 
{
    @Test
    public void testCreateDoesDelegateToDataPlannerWhenNoParamsSpecifiedHttpAndHttps()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Ds3Target target = mockDaoDriver.createDs3Target( "remote" );
        final User user = mockDaoDriver.createUser( "jason" );
        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.ADMINISTRATORS, user.getId() );
        final Node node = 
              support.getDatabaseSupport().getServiceManager().getService( NodeService.class ).getThisNode();
        mockDaoDriver.updateBean(
                node.setDnsName( "remote.spectra.com" ), 
                Node.DNS_NAME );
        mockDaoDriver.updateBean(
                node.setDataPathIpAddress( "192.168.1.99" ),
                Node.DATA_PATH_IP_ADDRESS );
        mockDaoDriver.updateBean(
                node.setDataPathHttpPort( Integer.valueOf( 80 ) ),
                Node.DATA_PATH_HTTP_PORT );
        mockDaoDriver.updateBean( 
                node.setDataPathHttpsPort( Integer.valueOf( 443 ) ), 
                Node.DATA_PATH_HTTPS_PORT );
        mockDaoDriver.updateBean( 
                node.setName( "sourcenode" ), 
                NameObservable.NAME );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockUserAuthorizationStrategy( user.getName() ), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DS3_TARGET + "/" + target.getName() )
                .addParameter(
                        RequestParameterType.OPERATION.toString(), 
                        RestOperationType.PAIR_BACK.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );

        assertEquals(1,
                support.getTargetInterfaceBtih().getTotalCallCount(),
                "Shoulda invoked data planner rpc resource to pair back DS3 target.");
        final String message = String.valueOf(target.getId());
        assertEquals(message,
                String.valueOf(support.getTargetInterfaceBtih().getMethodInvokeData().get( 0 ).getArgs().get( 0 )),
                "Shoulda invoked data planner rpc resource to pair back DS3 target.");
        final Ds3Target arg = (Ds3Target)
                support.getTargetInterfaceBtih().getMethodInvokeData().get( 0 ).getArgs().get( 1 );
        final Object expected5 = node.getDnsName();
        assertEquals(expected5,
                arg.getDataPathEndPoint(),
                "Shoulda sent down correct request.");
        final Object expected4 = node.getDataPathHttpPort();
        assertEquals(expected4,
                arg.getDataPathPort(),
                "Shoulda sent down correct request.");
        assertEquals(false,
                arg.isDataPathHttps(),
                "Shoulda sent down correct request.");
        assertEquals(true,
                arg.isDataPathVerifyCertificate(),
                "Shoulda sent down correct request.");
        final Object expected3 = user.getAuthId();
        assertEquals(expected3,
                arg.getAdminAuthId(),
                "Shoulda sent down correct request.");
        final Object expected2 = user.getSecretKey();
        assertEquals(expected2,
                arg.getAdminSecretKey(),
                "Shoulda sent down correct request.");
        assertEquals(null,
                arg.getDataPathProxy(),
                "Shoulda sent down correct request.");
        final Object expected1 = node.getName();
        assertEquals(expected1, arg.getName(), "Shoulda sent down correct request.");
        assertEquals(null, arg.getReplicatedUserDefaultDataPolicy(), "Shoulda sent down correct request.");
        assertEquals(Ds3TargetAccessControlReplication.NONE, arg.getAccessControlReplication(), "Shoulda sent down correct request.");
        assertEquals(TargetReadPreferenceType.LAST_RESORT, arg.getDefaultReadPreference(), "Shoulda sent down correct request.");
        final Object expected = mockDaoDriver.attainOneAndOnly( DataPathBackend.class ).getInstanceId();
        assertEquals(expected,
                arg.getId(),
                "Shoulda sent down correct request.");
        assertEquals(Quiesced.NO,
                arg.getQuiesced(),
                "Shoulda sent down correct request.");
        assertEquals(TargetState.ONLINE,
                arg.getState(),
                "Shoulda sent down correct request.");
    }
    
    
    @Test
    public void testCreateDoesDelegateToDataPlannerWhenNoParamsSpecifiedHttpsOnly()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Ds3Target target = mockDaoDriver.createDs3Target( "remote" );
        final User user = mockDaoDriver.createUser( "jason" );
        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.ADMINISTRATORS, user.getId() );
        final Node node = 
              support.getDatabaseSupport().getServiceManager().getService( NodeService.class ).getThisNode();
        mockDaoDriver.updateBean(
                node.setDataPathIpAddress( "192.168.1.99" ),
                Node.DATA_PATH_IP_ADDRESS );
        mockDaoDriver.updateBean( 
                node.setDataPathHttpsPort( Integer.valueOf( 443 ) ), 
                Node.DATA_PATH_HTTPS_PORT );
        mockDaoDriver.updateBean( 
                node.setName( "sourcenode" ), 
                NameObservable.NAME );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockUserAuthorizationStrategy( user.getName() ), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DS3_TARGET + "/" + target.getName() )
                .addParameter(
                        RequestParameterType.OPERATION.toString(), 
                        RestOperationType.PAIR_BACK.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );

        assertEquals(1,  support.getTargetInterfaceBtih().getTotalCallCount(), "Shoulda invoked data planner rpc resource to pair back DS3 target.");
        final Object expected6 = target.getId();
        assertEquals(expected6, support.getTargetInterfaceBtih().getMethodInvokeData().get( 0 ).getArgs().get( 0 ), "Shoulda invoked data planner rpc resource to pair back DS3 target.");
        final Ds3Target arg = (Ds3Target)
                support.getTargetInterfaceBtih().getMethodInvokeData().get( 0 ).getArgs().get( 1 );
        final Object expected5 = node.getDataPathIpAddress();
        assertEquals(expected5, arg.getDataPathEndPoint(), "Shoulda sent down correct request.");
        final Object expected4 = node.getDataPathHttpsPort();
        assertEquals(expected4, arg.getDataPathPort(), "Shoulda sent down correct request.");
        assertEquals(true, arg.isDataPathHttps(), "Shoulda sent down correct request.");
        assertEquals(true, arg.isDataPathVerifyCertificate(), "Shoulda sent down correct request.");
        final Object expected3 = user.getAuthId();
        assertEquals(expected3, arg.getAdminAuthId(), "Shoulda sent down correct request.");
        final Object expected2 = user.getSecretKey();
        assertEquals(expected2, arg.getAdminSecretKey(), "Shoulda sent down correct request.");
        assertEquals(null, arg.getDataPathProxy(), "Shoulda sent down correct request.");
        final Object expected1 = node.getName();
        assertEquals(expected1, arg.getName(), "Shoulda sent down correct request.");
        assertEquals(null, arg.getReplicatedUserDefaultDataPolicy(), "Shoulda sent down correct request.");
        assertEquals(Ds3TargetAccessControlReplication.NONE, arg.getAccessControlReplication(), "Shoulda sent down correct request.");
        assertEquals(TargetReadPreferenceType.LAST_RESORT, arg.getDefaultReadPreference(), "Shoulda sent down correct request.");
        final Object expected = mockDaoDriver.attainOneAndOnly( DataPathBackend.class ).getInstanceId();
        assertEquals(expected, arg.getId(), "Shoulda sent down correct request.");
        assertEquals(Quiesced.NO, arg.getQuiesced(), "Shoulda sent down correct request.");
        assertEquals(TargetState.ONLINE, arg.getState(), "Shoulda sent down correct request.");
    }
    
    
    @Test
    public void testCreateDoesDelegateToDataPlannerWhenAllParamsSpecified()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Ds3Target target = mockDaoDriver.createDs3Target( "remote" );
        final User user = mockDaoDriver.createUser( "jason" );
        final User user2 = mockDaoDriver.createUser( "rusty" );
        mockDaoDriver.addUserMemberToGroup( BuiltInGroup.ADMINISTRATORS, user.getId() );
        final Node node = 
              support.getDatabaseSupport().getServiceManager().getService( NodeService.class ).getThisNode();
        mockDaoDriver.updateBean(
                node.setDataPathIpAddress( "192.168.1.99" ),
                Node.DATA_PATH_IP_ADDRESS );
        mockDaoDriver.updateBean( 
                node.setDataPathHttpsPort( Integer.valueOf( 443 ) ), 
                Node.DATA_PATH_HTTPS_PORT );
        mockDaoDriver.updateBean( 
                node.setName( "sourcenode" ), 
                NameObservable.NAME );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockUserAuthorizationStrategy( user.getName() ), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DS3_TARGET + "/" + target.getName() )
                .addParameter( 
                        RequestParameterType.OPERATION.toString(), 
                        RestOperationType.PAIR_BACK.toString() )
                .addParameter(
                        Ds3Target.ACCESS_CONTROL_REPLICATION,
                        Ds3TargetAccessControlReplication.USERS.toString() )
                .addParameter(
                        Ds3Target.ADMIN_AUTH_ID,
                        user2.getAuthId() )
                .addParameter(
                        Ds3Target.ADMIN_SECRET_KEY,
                        user2.getSecretKey() )
                .addParameter(
                        Ds3Target.DATA_PATH_END_POINT,
                        "other.spectra.com" )
                .addParameter(
                        Ds3Target.DATA_PATH_HTTPS,
                        "true" )
                .addParameter(
                        Ds3Target.DATA_PATH_PORT,
                        "3333" )
                .addParameter(
                        Ds3Target.DATA_PATH_VERIFY_CERTIFICATE,
                        "false" )
                .addParameter(
                        Ds3Target.DATA_PATH_PROXY,
                        "proxy.spectra.com" )
                .addParameter(
                        ReplicationTarget.DEFAULT_READ_PREFERENCE,
                        TargetReadPreferenceType.AFTER_NEARLINE_POOL.toString() )
                .addParameter(
                        Ds3Target.REPLICATED_USER_DEFAULT_DATA_POLICY,
                        "somedp" )
                .addParameter(
                        NameObservable.NAME,
                        "srcbp" );
                        
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );

        assertEquals(1,  support.getTargetInterfaceBtih().getTotalCallCount(), "Shoulda invoked data planner rpc resource to pair back DS3 target.");
        final Object expected3 = target.getId();
        assertEquals(expected3, support.getTargetInterfaceBtih().getMethodInvokeData().get( 0 ).getArgs().get( 0 ), "Shoulda invoked data planner rpc resource to pair back DS3 target.");
        final Ds3Target arg = (Ds3Target)
                support.getTargetInterfaceBtih().getMethodInvokeData().get( 0 ).getArgs().get( 1 );
        assertEquals("other.spectra.com", arg.getDataPathEndPoint(), "Shoulda sent down correct request.");
        final Object actual = arg.getDataPathPort();
        assertEquals(Integer.valueOf( 3333 ), actual, "Shoulda sent down correct request.");
        assertEquals(true, arg.isDataPathHttps(), "Shoulda sent down correct request.");
        assertEquals(false, arg.isDataPathVerifyCertificate(), "Shoulda sent down correct request.");
        final Object expected2 = user2.getAuthId();
        assertEquals(expected2, arg.getAdminAuthId(), "Shoulda sent down correct request.");
        final Object expected1 = user2.getSecretKey();
        assertEquals(expected1, arg.getAdminSecretKey(), "Shoulda sent down correct request.");
        assertEquals("proxy.spectra.com", arg.getDataPathProxy(), "Shoulda sent down correct request.");
        assertEquals("srcbp", arg.getName(), "Shoulda sent down correct request.");
        assertEquals("somedp", arg.getReplicatedUserDefaultDataPolicy(), "Shoulda sent down correct request.");
        assertEquals(Ds3TargetAccessControlReplication.USERS, arg.getAccessControlReplication(), "Shoulda sent down correct request.");
        assertEquals(TargetReadPreferenceType.AFTER_NEARLINE_POOL, arg.getDefaultReadPreference(), "Shoulda sent down correct request.");
        final Object expected = mockDaoDriver.attainOneAndOnly( DataPathBackend.class ).getInstanceId();
        assertEquals(expected, arg.getId(), "Shoulda sent down correct request.");
        assertEquals(Quiesced.NO, arg.getQuiesced(), "Shoulda sent down correct request.");
        assertEquals(TargetState.ONLINE, arg.getState(), "Shoulda sent down correct request.");
    }
}
