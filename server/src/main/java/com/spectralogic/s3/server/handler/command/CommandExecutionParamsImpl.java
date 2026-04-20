/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.command;

import org.springframework.ui.ModelMap;

import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationServiceImpl;
import com.spectralogic.s3.common.platform.security.DataPolicyAclAuthorizationService;
import com.spectralogic.s3.common.platform.security.DataPolicyAclAuthorizationServiceImpl;
import com.spectralogic.s3.common.platform.security.GroupMembershipCache;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.rpc.dataplanner.DataPolicyManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.PoolManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.TapeManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.TargetManagementResource;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.Validations;

public final class CommandExecutionParamsImpl implements CommandExecutionParams
{
    public CommandExecutionParamsImpl(
            final BeansServiceManager serviceManager,
            final DataPlannerResource plannerResource,
            final DataPolicyManagementResource dataPolicyResource,
            final TapeManagementResource tapeResource,
            final PoolManagementResource poolResource,
            final TargetManagementResource targetResource,
            final DS3Request request,
            final ModelMap model )
    {
        m_serviceManager = serviceManager;
        m_plannerResource = plannerResource;
        m_dataPolicyResource = dataPolicyResource;
        m_tapeResource = tapeResource;
        m_poolResource = poolResource;
        m_targetResource = targetResource;
        m_request = request;
        m_groupMemberships = new GroupMembershipCache( m_serviceManager, 5000 );
        m_bucketAclAuthenticationService = new BucketAclAuthorizationServiceImpl( 
                m_serviceManager, m_groupMemberships, 10000 );
        m_dataPolicyAclAuthenticationService = new DataPolicyAclAuthorizationServiceImpl( 
                m_serviceManager, m_groupMemberships, 10000 );
        m_model = model;
        
        Validations.verifyNotNull( "Planner resource", m_plannerResource );
        Validations.verifyNotNull( "Data policy resource", m_dataPolicyResource );
        Validations.verifyNotNull( "Tape resource", m_tapeResource );
        Validations.verifyNotNull( "Pool resource", m_poolResource );
        Validations.verifyNotNull( "Request", m_request );
        Validations.verifyNotNull( "Model", m_model );
    }
    
    
    public BeansServiceManager getServiceManager()
    {
        return m_serviceManager;
    }
    

    public DataPlannerResource getPlannerResource()
    {
        return m_plannerResource;
    }
    
    
    public DataPolicyManagementResource getDataPolicyResource()
    {
        return m_dataPolicyResource;
    }
    

    public TapeManagementResource getTapeResource()
    {
        return m_tapeResource;
    }
    
    
    public PoolManagementResource getPoolResource()
    {
        return m_poolResource;
    }
    
    
    public TargetManagementResource getTargetResource()
    {
        return m_targetResource;
    }

    
    public DS3Request getRequest()
    {
        return m_request;
    }
    
    
    public GroupMembershipCache getGroupMembershipCache()
    {
        return m_groupMemberships;
    }
    
    
    public BucketAclAuthorizationService getBucketAclAuthorizationService()
    {
        return m_bucketAclAuthenticationService;
    }
    
    
    public DataPolicyAclAuthorizationService getDataPolicyAclAuthorizationService()
    {
        return m_dataPolicyAclAuthenticationService;
    }

    
    public ModelMap getModel()
    {
        return m_model;
    }
    
    
    private final BeansServiceManager m_serviceManager;
    private final DataPlannerResource m_plannerResource;
    private final DataPolicyManagementResource m_dataPolicyResource;
    private final TapeManagementResource m_tapeResource;
    private final PoolManagementResource m_poolResource;
    private final TargetManagementResource m_targetResource;
    private final DS3Request m_request;
    private final GroupMembershipCache m_groupMemberships;
    private final BucketAclAuthorizationService m_bucketAclAuthenticationService;
    private final DataPolicyAclAuthorizationService m_dataPolicyAclAuthenticationService;
    private final ModelMap m_model;
}
