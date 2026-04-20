/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.system;

import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.manager.DatabasePhysicalSpaceState;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Duration;

public final class VerifySystemHealthRequestHandler extends BaseRequestHandler
{
    public VerifySystemHealthRequestHandler()
    {
        super( new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.NONE ),
               new RestfulCanHandleRequestDeterminer( 
                       RestActionType.LIST,
                       RestDomainType.SYSTEM_HEALTH ) );
    }

    
    @Override
    protected ServletResponseStrategy handleRequestInternal( 
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final HealthVerificationResult retval = BeanFactory.newBean( HealthVerificationResult.class );
        final Duration durationToVerifyDataPlanner = new Duration();
        if ( !params.getPlannerResource().isServiceable() )
        {
            throw new S3RestException( 
                    GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT, 
                    "The data planner is currently initializing or offline.  Try again later." );
        }
        retval.setMsRequiredToVerifyDataPlannerHealth( durationToVerifyDataPlanner.getElapsedMillis() );
        retval.setDatabaseFilesystemFreeSpace( params.getServiceManager().
                getDatabaseSpaceState() );
        return BeanServlet.serviceGet( params, retval );
    }
    
    
    interface HealthVerificationResult extends SimpleBeanSafeToProxy
    {
        String MS_REQUIRED_TO_VERIFY_DATA_PLANNER_HEALTH = "msRequiredToVerifyDataPlannerHealth";
        
        long getMsRequiredToVerifyDataPlannerHealth();
        
        void setMsRequiredToVerifyDataPlannerHealth( final long value );
        
        
        String DATABASE_FILESYSTEM_FREE_SPACE = "databaseFilesystemFreeSpace";
        
        DatabasePhysicalSpaceState getDatabaseFilesystemFreeSpace();
        
        void setDatabaseFilesystemFreeSpace( final DatabasePhysicalSpaceState value );
    } // end inner class def
}
