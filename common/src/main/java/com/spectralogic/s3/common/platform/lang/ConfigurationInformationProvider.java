/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.lang;

import java.io.File;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.platform.spectraview.SpectraViewRestRequest;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.thread.RecurringRunnableExecutor;

public final class ConfigurationInformationProvider
{
    private ConfigurationInformationProvider()
    {
        m_updaterExecutor.start();
        populateBuildInformation();
    }
    
    
    private void populateBuildInformation()
    {
        try
        {
            final Set< String > propertiesWritten = new HashSet<>();
            final List< String > lines =
                    Files.readAllLines( new File( "/etc/version.conf" ).toPath(), StandardCharsets.UTF_8 );
            for ( final String line : lines )
            {
                final String [] parts = line.split( Pattern.quote( ":" ) );
                if ( 2 > parts.length )
                {
                    continue;
                }
                
                final String key = NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE.convert(
                        parts[ 0 ].trim() );
                final String value = parts[ 1 ].trim();
                if ( !BeanUtils.hasPropertyName( BuildInformation.class, key ) )
                {
                    LOG.warn( BuildInformation.class.getSimpleName()
                              + " needs to be updated to include property '" + key + "'." );
                    continue;
                }
                propertiesWritten.add( key );
                BeanUtils.getWriter( BuildInformation.class, key ).invoke( m_buildInformation, value );
            }
            
            final Set< String > allProperties = BeanUtils.getPropertyNames( BuildInformation.class );
            allProperties.removeAll( propertiesWritten );
            if ( !allProperties.isEmpty() )
            {
                LOG.warn( BuildInformation.class.getSimpleName()
                          + " needs to be updated to not include properties: " + allProperties );
            }
        }
        catch ( final Exception ex )
        {
            LOG.warn( "Failed to determine version configuration.", ex );
        }
    }
    
    
    public static ConfigurationInformationProvider getInstance()
    {
        return INSTANCE;
    }
    
    
    public String getDataPathIpAddress()
    {
        m_updater.waitUntilInitialized();
        return m_dataPathIpAddress;
    }
    
    
    /**
     * @return null if no HTTP port is configured, non-null otherwise
     */
    public Integer getDataPathHttpPort()
    {
        m_updater.waitUntilInitialized();
        return m_httpPort;
    }
    
    
    /**
     * @return null if no HTTPS port is configured, non-null otherwise
     */
    public Integer getDataPathHttpsPort()
    {
        m_updater.waitUntilInitialized();
        return m_httpsPort;
    }
    
    
    public BuildInformation getBuildInformation()
    {
        return m_buildInformation;
    }
    
    
    private final class ConfigurationInformationCollector implements Runnable
    {
        private void waitUntilInitialized()
        {
            if ( null != m_dataPathIpAddress )
            {
                return;
            }
            
            synchronized ( this )
            { 
                if ( null != m_dataPathIpAddress )
                {
                    return;
                }
                run();
            }
        }
        
        
        synchronized public void run()
        {
            try
            {
                updatePorts();
            }
            catch ( final Exception ex )
            {
                LOG.warn( "Failed to determine data path ports.", ex );
            }
            
            String dataPathIpAddress;
            try
            {
                dataPathIpAddress = getDataPathIpAddress();
            }
            catch ( final Exception ex )
            {
                LOG.warn( "Failed to determine data path ip address.", ex );
                dataPathIpAddress = "FAILED WITH EXCEPTION - SEE LOGS AROUND " + new Date();
            }
            
            if ( !dataPathIpAddress.equals( m_dataPathIpAddress ) )
            {
                m_dataPathIpAddress = dataPathIpAddress;
                LOG.info( "Data path IP address is now " + m_dataPathIpAddress + "." );
            }
        }
        
        
        private String getDataPathIpAddress()
        {
            if (TEST_VERSION.equals(m_buildInformation.getVersion())) {
                LOG.info("Current version is " + TEST_VERSION + ". Will use localhost as data path IP address.");
                return "127.0.0.1";
            }
            if (DOCKER_VERSION.equals(m_buildInformation.getVersion())) {
                LOG.info("Current version is " + DOCKER_VERSION + ". Will use localhost as data path IP address.");
                return "dataplanner";
            }
            String rawResponse = "";
            try
            {
                final String prefix = "[{\"address\":\"";
                rawResponse = new SpectraViewRestRequest(
                        RequestType.GET, "logical_network_data_interfaces.json", Level.DEBUG ).run();
                String ipAndNetmask = rawResponse.replace( " ", "" );
                ipAndNetmask = ipAndNetmask.substring( ipAndNetmask.indexOf( prefix ) + prefix.length() );
                ipAndNetmask = ipAndNetmask.substring( 0, ipAndNetmask.indexOf( '"' ) );
                final int netmaskDelimiterIndex = ipAndNetmask.indexOf( '/' );
                final String ip = ipAndNetmask.substring( 0, netmaskDelimiterIndex );
                return ip;
            }
            catch ( final Exception ex )
            {
                LOG.warn( "Failed to determine data path ip address from '" + rawResponse + "'.", ex );
                return "FAILED_TO_DETERMINE_DATAPATH_IP_ADDRESS";
            }
        }
        
        
        private void updatePorts() throws Exception
        {
            final Properties properties = new Properties();
            final FileReader fileReader = new FileReader( "/etc/spectra/pf-s3port.conf" );
            properties.load( fileReader );
            fileReader.close();
            
            m_httpPort = getPortNumber( m_httpPort, "Data path HTTP port", properties, "s3_http_port" );
            m_httpsPort = getPortNumber( m_httpsPort, "Data path HTTPS port", properties, "s3_https_port" );
        }
        
        
        private Integer getPortNumber(
                final Integer oldValue,
                final String portDescription,
                final Properties properties, 
                final String propertyName )
        {
            Integer retval = null;
            try
            {
                final String propertyValue = properties.getProperty( propertyName ).replace( "\"", "" );
                retval = Integer.valueOf( Integer.parseInt( propertyValue ) );
                if ( !retval.equals( oldValue ) )
                {
                    LOG.info( portDescription + " is now " + retval + "." );
                }
            }
            catch ( final Exception ex )
            {
                if ( null != oldValue )
                {
                    LOG.info( portDescription + " has been disabled.", ex );
                }
            }
            return retval;
        }
    } // end inner class def
    
    
    private volatile Integer m_httpPort;
    private volatile Integer m_httpsPort;
    private volatile String m_dataPathIpAddress;
    
    private final BuildInformation m_buildInformation = BeanFactory.newBean( BuildInformation.class );
    private final ConfigurationInformationCollector m_updater =
            new ConfigurationInformationCollector();
    private final RecurringRunnableExecutor m_updaterExecutor =
            new RecurringRunnableExecutor( m_updater, 5 * 60 * 1000 );
    
    private final static Logger LOG = Logger.getLogger( ConfigurationInformationProvider.class );
    private final static ConfigurationInformationProvider INSTANCE = new ConfigurationInformationProvider();
    private final static String TEST_VERSION = "TEST_VERSION";
    private final static String DOCKER_VERSION = "DOCKER_VERSION";
}
