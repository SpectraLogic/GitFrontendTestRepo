/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.request;


final class S3RequestPathParser
{
    S3RequestPathParser( 
            final String requestPath,
            final String bucketNameOverrideValue,
            final String objectNameOverrideValue )
    {
        if (requestPath == null || requestPath.length() < 2)
        {
            m_bucketName = bucketNameOverrideValue;
            m_objectName = objectNameOverrideValue;
            return;
        }

        StringBuilder sb;
        int i = 1;
        char c;
        if (m_bucketName == null)
        {
            // bucket was not picked in the host, pull it out of the path
            sb = new StringBuilder();
            for (; i < requestPath.length(); i ++)
            {
                c = requestPath.charAt(i);
                if (c == '/')
                {
                    i++;
                    break;
                }
                sb.append(c);
            }
            m_bucketName = sb.toString();
        }
        if (i < requestPath.length())
        {
            sb = new StringBuilder();
            for (; i < requestPath.length(); i ++)
            {
                sb.append(requestPath.charAt(i));
            }
            m_objectName = sb.toString();
        }
        
        if ( null != bucketNameOverrideValue )
        {
            m_bucketName = bucketNameOverrideValue;
        }
        if ( null != objectNameOverrideValue )
        {
            m_objectName = objectNameOverrideValue;
        }
    }
    

    String getBucketName()
    {
        return m_bucketName;
    }

    
    String getObjectName()
    {
        return m_objectName;
    }
    

    private String m_bucketName;
    private String m_objectName;
}
