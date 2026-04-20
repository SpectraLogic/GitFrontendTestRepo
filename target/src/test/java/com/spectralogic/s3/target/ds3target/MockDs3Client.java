package com.spectralogic.s3.target.ds3target;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientImpl;
import com.spectralogic.ds3client.commands.interfaces.Ds3Request;
import com.spectralogic.ds3client.models.SystemInformation;
import com.spectralogic.ds3client.models.common.Credentials;
import com.spectralogic.ds3client.networking.ConnectionDetails;
import com.spectralogic.ds3client.networking.Headers;
import com.spectralogic.ds3client.networking.NetworkClient;
import com.spectralogic.ds3client.networking.WebResponse;
import com.spectralogic.ds3client.serializer.XmlOutput;
import com.spectralogic.s3.common.platform.lang.ConfigurationInformationProvider;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.lang.CollectionFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class MockDs3Client {
    public MockDs3Client() {
        // do nothing
         m_networkClient = new MockNetworkClient();
         m_client = new Ds3ClientImpl(m_networkClient);
    }

    public Ds3Client getClient()
    {
        return m_client;
    }

    synchronized public void setResponse(
            final Class< ? extends Ds3Request> requestType,
            final Object result,
            final int statusCode,
            final Map< String, String > headers ) {
        final WebResponse webResponse = new MockWebResponse(result, statusCode, new MockHeaders(headers));
        m_networkClient.setResponse(requestType, webResponse);
    }

    synchronized public void setResponse(
            final Class< ? extends Ds3Request> requestType,
            final byte[] result,
            final int statusCode,
            final Map< String, String > headers ) {
        final WebResponse webResponse = new MockWebResponse(result, statusCode, new MockHeaders(headers));
        m_networkClient.setResponse(requestType, webResponse);
    }

    public <T extends Ds3Request> T getRequest(final Class<T> requestType) {
        T retval = null;
        for ( final Ds3Request r : getRequests() )
        {
            if ( requestType.isAssignableFrom( r.getClass() ) )
            {
                if ( null != retval )
                {
                    throw new RuntimeException( "There are multiple requests of type " + requestType + "." );
                }

                @SuppressWarnings( "unchecked" )
                final T castedR = (T)r;
                retval = castedR;
            }
        }

        return retval;
    }

    public int getRequestCount( final Class< ? extends Ds3Request > requestType )
    {
        return getRequests( requestType ).size();
    }


    synchronized public List< Ds3Request > getRequests()
    {
        return new ArrayList<>( m_networkClient.m_requests );
    }


    public < T extends Ds3Request > List< T > getRequests( final Class< T > requestType )
    {
        final List< T > retval = new ArrayList<>();
        for ( final Ds3Request r : getRequests() )
        {
            if ( requestType.isAssignableFrom( r.getClass() ) )
            {
                @SuppressWarnings( "unchecked" )
                final T castedR = (T)r;
                retval.add( castedR );
            }
        }

        return retval;
    }

    synchronized public < D extends SimpleBeanSafeToProxy, S extends Ds3Request >
    void verifyRequestMatchesDao(
            final Class< D > daoType,
            final D daoBean,
            final S request )
    {
        for ( final String prop : BeanUtils.getPropertyNames( daoType ) )
        {
            final Method reader1 = BeanUtils.getReader( daoType, prop );
            final Method reader2 = BeanUtils.getReader( request.getClass(), prop );
            if ( null == reader2 )
            {
                continue;
            }

            try
            {
                final Object o1 = reader1.invoke( daoBean );
                final Object o2 = reader2.invoke( request );
                if ( ( null == o1 ) == ( null == o2 ) )
                {
                    if (null == o1 || o1.toString().equals(o2.toString()))
                    {
                        continue;
                    }
                }
                throw new RuntimeException(
                        "Shoulda sent request as target was defined for prop " + prop + "." );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
        }
    }

    public static SystemInformation createSystemInformation(final UUID instanceId )
    {
        final SystemInformation retval = new SystemInformation();
        retval.setInstanceId( instanceId );
        retval.setNow( System.currentTimeMillis() );
        retval.setBuildInformation( new com.spectralogic.ds3client.models.BuildInformation() );
        retval.getBuildInformation().setVersion(
                ConfigurationInformationProvider.getInstance().getBuildInformation().getVersion() );
        retval.getBuildInformation().setRevision(
                ConfigurationInformationProvider.getInstance().getBuildInformation().getRevision() );

        return retval;
    }

    public boolean isShutdown() {
        return m_networkClient.getClosed();
    }

    public void clearShutdown() {
        m_networkClient.resetClosed();
    }

    private static class MockNetworkClient implements NetworkClient {

        @Override
        public WebResponse getResponse(Ds3Request ds3Request) {
            m_requests.add(ds3Request);
            return m_responses.get(ds3Request.getClass());
        }

        @Override
        public ConnectionDetails getConnectionDetails() {
            return new ConnectionDetails() {
                @Override
                public String getEndpoint() {
                    return null;
                }

                @Override
                public Credentials getCredentials() {
                    return null;
                }

                @Override
                public boolean isHttps() {
                    return false;
                }

                @Override
                public URI getProxy() {
                    return null;
                }

                @Override
                public int getRetries() {
                    return 0;
                }

                @Override
                public int getBufferSize() {
                    return 1024;
                }

                @Override
                public int getConnectionTimeout() {
                    return 0;
                }

                @Override
                public int getSocketTimeout() {
                    return 0;
                }

                @Override
                public boolean isCertificateVerification() {
                    return false;
                }

                @Override
                public String getUserAgent() {
                    return null;
                }
            };
        }

        @Override
        public void close() {
            m_closed = true;
        }

        public void resetClosed() {
            m_closed = false;
        }

        public boolean getClosed() {
            return m_closed;
        }

        public void setResponse(final Class< ? extends Ds3Request> requestType, final WebResponse webResponse) {
            m_responses.put(requestType, webResponse);
        }

        private boolean m_closed = false;
        private final List< Ds3Request > m_requests = new ArrayList<>();
        private final Map< Class< ? extends Ds3Request >, WebResponse> m_responses = new HashMap<>();
    }

    private static class MockWebResponse implements WebResponse {
        public MockWebResponse(Object result, int statusCode, Headers headers) {
            m_result = result;
            m_result_bytes = null;
            m_statusCode = statusCode;
            m_headers = headers;
        }

        public MockWebResponse(byte[] result, int statusCode, Headers headers) {
            m_result_bytes = result;
            m_result = null;
            m_statusCode = statusCode;
            m_headers = headers;
        }

        @Override
        public InputStream getResponseStream() {
            InputStream inputStream = null;
            if (m_result != null) {
                if (m_result.getClass().isAssignableFrom(String.class)) {
                    final String tmp = (String) m_result;
                    inputStream = new ByteArrayInputStream(tmp.getBytes(StandardCharsets.UTF_8));
                } else {
                    inputStream = new ByteArrayInputStream(XmlOutput.toXml(m_result).getBytes(StandardCharsets.UTF_8));
                }
            } else if (m_result_bytes != null) {
                inputStream = new ByteArrayInputStream(m_result_bytes);
            }
            return inputStream;
        }

        @Override
        public int getStatusCode() {
            return m_statusCode;
        }

        @Override
        public Headers getHeaders() {
            return m_headers;
        }

        @Override
        public void close() {

        }
        
        final Object m_result;
        final byte[] m_result_bytes;
        final int m_statusCode;
        final Headers m_headers;
    }

    private final static class MockHeaders implements Headers
    {
        private MockHeaders( final Map< String, String > headers )
        {
            m_headers = ( null == headers ) ? new HashMap<>() : headers;
        }


        public List< String > get( final String key )
        {
            if ( m_headers.containsKey( key ) )
            {
                return CollectionFactory.toList( m_headers.get( key ) );
            }
            return new ArrayList<>();
        }


        public Set< String > keys()
        {
            return new HashSet<>( m_headers.keySet() );
        }


        private final Map< String, String > m_headers;
    }

    private final MockNetworkClient m_networkClient;
    private final Ds3Client m_client;
}
