/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.command.api;

import org.springframework.ui.ModelMap;

import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService;
import com.spectralogic.s3.common.platform.security.DataPolicyAclAuthorizationService;
import com.spectralogic.s3.common.platform.security.GroupMembershipCache;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.rpc.dataplanner.DataPolicyManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.PoolManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.TapeManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.TargetManagementResource;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.util.db.service.api.BeansServiceManager;

public interface CommandExecutionParams
{
    public BeansServiceManager getServiceManager();
    
    
    public DataPlannerResource getPlannerResource();
    
    
    public DataPolicyManagementResource getDataPolicyResource();
    
    
    public TapeManagementResource getTapeResource();
    
    
    public PoolManagementResource getPoolResource();
    
    
    public TargetManagementResource getTargetResource();
    
    
    public DS3Request getRequest();
    
    
    public GroupMembershipCache getGroupMembershipCache();
    
    
    public BucketAclAuthorizationService getBucketAclAuthorizationService();
    
    
    public DataPolicyAclAuthorizationService getDataPolicyAclAuthorizationService();
    
    
    public ModelMap getModel();
}
