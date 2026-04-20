/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.system;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.platform.lang.BuildInformation;
import com.spectralogic.s3.common.platform.lang.ConfigurationInformationProvider;
import com.spectralogic.s3.common.platform.lang.HardwareInformationProvider;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.system.GetRequestHandlersRequestHandler.RequestHandlerInfo;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.security.ChecksumGenerator;

public final class GetSystemInformationRequestHandler extends BaseRequestHandler
{
    public GetSystemInformationRequestHandler()
    {
        super( new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.NONE ),
               new RestfulCanHandleRequestDeterminer( 
                       RestActionType.LIST,
                       RestDomainType.SYSTEM_INFORMATION ) );
    }

    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        return BeanServlet.serviceGet( 
                params, 
                getSystemInformation( params.getServiceManager() ) );
    }
    
    
    private SystemInformationApiBean getSystemInformation( final BeansServiceManager serviceManager )
    {
        synchronized ( m_systemInformation )
        {
            if ( null != m_systemInformation.getBuildInformation() )
            {
                populateMutableProperties( serviceManager );
                return m_systemInformation;
            }
        }
        
        final StringBuilder allShortVersions = new StringBuilder();
        final StringBuilder allFullVersions = new StringBuilder();
        for ( final RequestHandlerInfo rh 
                : GetRequestHandlersRequestHandler.getAllRequestHandlers().getRequestHandlers() )
        {
            final String fullVersion = rh.getVersion();
            final int index = fullVersion.indexOf( '.' );
            final String shortVersion = fullVersion.substring( 0, index );
            allFullVersions.append( fullVersion );
            allShortVersions.append( shortVersion );
        }
        m_systemInformation.setApiVersion( 
                ChecksumGenerator.generateMd5( allShortVersions.toString() ).toUpperCase()
                + "." + ChecksumGenerator.generateMd5( allFullVersions.toString() ).toUpperCase() );
        populateMutableProperties( serviceManager );
        
        return m_systemInformation;
    }
    
    
    private void populateMutableProperties( final BeansServiceManager serviceManager )
    {
        final DataPathBackend dpb = 
                serviceManager.getRetriever( DataPathBackend.class ).attain( Require.nothing() );
        m_systemInformation.setBuildInformation( 
                ConfigurationInformationProvider.getInstance().getBuildInformation() );
        m_systemInformation.setSerialNumber(
                HardwareInformationProvider.getSerialNumber() );
        m_systemInformation.setBackendActivated( dpb.isActivated() );
        m_systemInformation.setInstanceId( dpb.getInstanceId() );
        m_systemInformation.setNow( System.currentTimeMillis() );
    }
    
    
    interface SystemInformationApiBean extends SimpleBeanSafeToProxy
    {
        String BUILD_INFORMATION = "buildInformation";
        
        BuildInformation getBuildInformation();
        
        void setBuildInformation( final BuildInformation value );
        
        
        String SERIAL_NUMBER = "serialNumber";
        
        String getSerialNumber();
        
        void setSerialNumber( final String value );
        
        
        String API_VERSION = "apiVersion";
        
        String getApiVersion();
        
        void setApiVersion( final String value );
        
        
        String BACKEND_ACTIVATED = "backendActivated";
        
        boolean isBackendActivated();
        
        void setBackendActivated( final boolean value );
        
        
        String INSTANCE_ID = "instanceId";
        
        UUID getInstanceId();
        
        void setInstanceId( final UUID value );
        
        
        String NOW = "now";
        
        long getNow();
        
        void setNow( final long value );
    } // end inner class def
    
    
    private final SystemInformationApiBean m_systemInformation = 
            BeanFactory.newBean( SystemInformationApiBean.class );
}
