/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.servlet.api;

/**
 * A servlet-based response strategy for requests.
 */
public interface ServletResponseStrategy
{
    String getServletNameToProvideResponseWith();
}
