/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrainternal.system;

import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.server.handler.auth.InternalAccessOnlyAuthenticationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDaoTypedRequestHandler;
import com.spectralogic.s3.server.handler.reqhandler.spectrainternal.system.CreateHeapDumpRequestHandler.CreateHeapDumpParams;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.io.lang.HeapDumper;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class CreateHeapDumpRequestHandler extends BaseDaoTypedRequestHandler< CreateHeapDumpParams >
{
    public CreateHeapDumpRequestHandler()
    {
        super( CreateHeapDumpParams.class,
               new InternalAccessOnlyAuthenticationStrategy(), 
               new RestfulCanHandleRequestDeterminer(
                       RestActionType.CREATE,
                       RestDomainType.HEAP_DUMP ) );
        
        registerRequiredBeanProperties( CreateHeapDumpParams.APPLICATION );
    }

    
    public interface CreateHeapDumpParams extends SimpleBeanSafeToProxy
    {
        String APPLICATION = "application";
        
        Application getApplication();
        
        void setApplication( final Application value );
        
        
        String PATH = "path";
        
        String getPath();
        
        void setPath( final String value );
    } // end inner class def
    
    
    public enum Application
    {
        S3_SERVER,
        DATA_PLANNER
    } // end inner class def

    
    @Override
    protected ServletResponseStrategy handleRequestInternal( 
            final DS3Request request, 
            final CommandExecutionParams params )
    {
        final CreateHeapDumpParams createHeapDumpParams = getBeanSpecifiedViaQueryParameters( 
                params, AutoPopulatePropertiesWithDefaults.YES );
        createHeapDumpParams.setPath( createHeapDump( 
                createHeapDumpParams.getApplication(),
                params.getPlannerResource() ) );
        
        return BeanServlet.serviceCreate( params, createHeapDumpParams );
    }
    
    
    private String createHeapDump(
            final Application application, 
            final DataPlannerResource plannerResource )
    {
        switch ( application )
        {
            case S3_SERVER:
                return createS3ServerHeapDump();
            case DATA_PLANNER:
                return createDataPlannerHeapDump( plannerResource );
            default:
                throw new UnsupportedOperationException( "No code to handle " + application + "." );
        }
    }
    
    
    private String createS3ServerHeapDump()
    {
        return HeapDumper.dumpAndZipHeap(false).getAbsolutePath();
    }
    
    
    private String createDataPlannerHeapDump( final DataPlannerResource plannerResource )
    {
        return plannerResource.dumpHeap().get( Timeout.LONG );
    }
}
