/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.http;

import java.io.IOException;
import java.util.Map;

import javax.servlet.ServletInputStream;

public interface HttpRequest
{
    /**
     * Returns the name of the HTTP method with which this request was made, for example, GET, POST, or PUT.
     *
     * @return a <code>String</code> specifying the name of the method with which this request was made
     */
    RequestType getType();

    
    /**
     * Reconstructs the URL the client used to make the request.  The returned URL contains a protocol, server
     * name, port number, and server path, but it does not include query string parameters.
     *
     * <p>If this request has been forwarded using {@link javax.servlet.RequestDispatcher#forward}, the server
     * path in the reconstructed URL must reflect the path used to obtain the RequestDispatcher, and not the 
     * server path specified by the client.
     *
     * @return a <code>String</code> object containing the reconstructed URL
     */
    public String getOriginalClientRequestUrl();

    
    /**
     * Reconstructs the URL the client used to make the request.  The returned URL contains a protocol, server
     * name, port number, and server path, as well as the query string parameters.
     *
     * <p>If this request has been forwarded using {@link javax.servlet.RequestDispatcher#forward}, the server
     * path in the reconstructed URL must reflect the path used to obtain the RequestDispatcher, and not the 
     * server path specified by the client.
     *
     * @return a <code>String</code> object containing the reconstructed URL
     */
    public String getFullOriginalClientRequestUrl();


    /**
     * Returns any extra path information associated with the URL the client sent when it made this request.
     * The extra path information follows the servlet path but precedes the query string and will start with
     * a "/" character.
     *
     * <p>This method returns <code>null</code> if there
     * was no extra path information.
     *
     * @return a <code>String</code>, decoded by the web container, specifying extra path information that 
     * comes after the servlet path but before the query string in the request URL; or <code>null</code> if 
     * the URL does not have any extra path information
     */
    public String getPathInfo();
    

    /**
     * Returns the value of an HTTP header as a <code>String</code>, or <code>null</code> if the header does
     * not exist.
     *
     * @param name a <code>String</code> specifying the name of the HTTP header
     *
     * @return a <code>String</code> representing the single value of the header
     */
    public String getHeader( final String name );
    

    /**
     * Returns the value of an HTTP header as a <code>String</code>, or <code>null</code> if the header does
     * not exist.
     *
     * @param header a <code>String</code> specifying the name of the HTTP header
     *
     * @return a <code>String</code> representing the single value of the header
     */
    public String getHeader( final HttpHeaderType header );
    
    
    /**
     * Returns a java.util.Map of the headers of this request.
     * 
     * @return an immutable java.util.Map containing parameter names as keys and parameter values as values
     */
    public Map< String, String > getHeaders();
    

    /**
     * Returns the value of a request parameter as a <code>String</code>, or <code>null</code> if the 
     * parameter does not exist. Request parameters are extra information sent with the request.  For HTTP 
     * servlets, parameters are contained in the query string or posted form data.
     *
     * @param name a <code>String</code> specifying the name of the parameter
     *
     * @return a <code>String</code> representing the single value of the parameter
     */
    public String getQueryParam( final String name );
    
    
    /**
     * Returns a java.util.Map of the parameters of this request.
     * 
     * <p>Request parameters are extra information sent with the request.  For HTTP servlets, parameters are 
     * contained in the query string or posted form data.
     *
     * @return an immutable java.util.Map containing parameter names as keys and parameter values as values
     */
    public Map< String, String > getQueryParams();
    

    /**
     * Returns the Internet Protocol (IP) address of the client or last proxy that sent the request.
     *
     * @return a <code>String</code> containing the IP address of the client that sent the request
     */
    public String getRemoteAddr();
    

    /**
     * Returns the fully qualified name of the client or the last proxy that sent the request.  If the engine 
     * cannot or chooses not to resolve the hostname (to improve performance), this method returns the 
     * dotted-string form of the IP address.
     *
     * @return a <code>String</code> containing the fully qualified name of the client
     */
    public String getRemoteHost();  
    
    
    /**
     * Returns the Internet Protocol (IP) source port of the client or last proxy that sent the request.
     *
     * @return an integer specifying the port number
     */    
    public int getRemotePort();
    

    /**
     * Returns the login of the user making this request, if the user has been authenticated, or 
     * <code>null</code> if the user has not been authenticated.   Whether the user name is sent with each 
     * subsequent request depends on the browser and type of authentication. 
     * 
     * @return a <code>String</code> specifying the login of the user making this request, or 
     * <code>null</code> if the user login is not known
     */
    public String getRemoteUser();
    

    /**
     * Returns the MIME type of the body of the request, or <code>null</code> if the type is not known.
     *
     * @return a <code>String</code> containing the name of the MIME type of the request, or null if the 
     * type is not known
     */
    public String getContentType();
    

    /**
     * Retrieves the body of the request as binary data using a {@link ServletInputStream}.
     *
     * @return a {@link ServletInputStream} object containing the body of the request
     *
     * @exception IOException if an input or output exception occurred
     */
    public ServletInputStream getInputStream() throws IOException; 
}
