package com.spectralogic.s3.server.request.api;

import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.server.authorization.api.S3Authorization;
import com.spectralogic.s3.server.request.rest.RestRequest;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.http.HttpRequest;

public interface DS3Request 
{
    public String getRequestPath();

    
    public HttpRequest getHttpRequest();

    
    public HttpServletResponse getHttpResponse();

    
    public S3Authorization getAuthorization();

    
    public long getRequestId();
    

    public boolean hasRequestParameter( final RequestParameterType param );
    
    
    public Set< RequestParameterType > getRequestParameters();
    
    
    public Map< String, String > getBeanPropertyValueMapFromRequestParameters();
    
    
    /**
     * @return the {@link RequestParameterValue} representing the request parameter's value, or null if the 
     * request parameter was not specified
     */
    public RequestParameterValue getRequestParameter( final RequestParameterType param );
    

    /**
     * @return the value of the request header, or null if the request header was not specified
     */
    public String getRequestHeader( final S3HeaderType headerType );

    
    /**
     * @return message that provides request received relevant information for this request
     */
    public String getRequestReceivedMessage( final String handlerName );
    

    /**
     * @return message that provides request processed relevant information for this request
     */
    public String getRequestProcessedMessage( final ServletResponseStrategy rs );
    
    
    public String getBucketName();
    
    
    public void setBucketName( final String bucketName );
    
    
    public String getObjectName();
    
    
    public void setObjectName( final String objectName );
    
    
    /**
     * @return the restful information about this request
     */
    public RestRequest getRestRequest();
}
