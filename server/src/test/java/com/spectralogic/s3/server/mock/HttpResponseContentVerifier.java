/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.mock;

import com.spectralogic.s3.common.dao.domain.notification.NotificationRegistrationObservable;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapeLibrary;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.util.bean.lang.Identifiable;

public class HttpResponseContentVerifier
{
    public HttpResponseContentVerifier( final MockHttpRequestDriver driver )
    {
        m_driver = driver;
    }


    public < T extends NotificationRegistrationObservable< ? > & Identifiable >
            HttpResponseContentVerifier verifyNotificationRegistrationNode(
                    final String prefix,
                    final T registration )
    {
        new PrefixedXPathVerifier( prefix )
                .assertXPath( "Format", registration.getFormat() )
                .assertXPath( "Id", registration.getId() )
                .assertXPath( "LastHttpResponseCode", registration.getLastHttpResponseCode() )
                .assertXPath( "LastNotification", registration.getLastNotification() )
                .assertXPath( "NamingConvention", registration.getNamingConvention() )
                .assertXPath( "NotificationEndPoint", registration.getNotificationEndPoint() )
                .assertXPath( "NotificationHttpMethod", registration.getNotificationHttpMethod() )
                .assertXPath( "UserId", registration.getUserId() )
                .assertXPath( "NumberOfFailuresSinceLastSuccess",
                        Integer.valueOf( registration.getNumberOfFailuresSinceLastSuccess() ) );
        return this;
    }


    public HttpResponseContentVerifier verifyTapeLibrary(
            final String prefix,
            final TapeLibrary library )
    {
        new PrefixedXPathVerifier( prefix )
                .assertXPath( "Id", library.getId() )
                .assertXPath( "ManagementUrl", library.getManagementUrl() )
                .assertXPath( "Name", library.getName() )
                .assertXPath( "SerialNumber", library.getSerialNumber() );
        return this;
    }
    
    
    public HttpResponseContentVerifier verifyTapeDriveNode(
            final String prefix,
            final TapeDrive tapeDrive )
    {
        new PrefixedXPathVerifier( prefix )
                .assertXPath( "ErrorMessage", tapeDrive.getErrorMessage() )
                .assertXPath( "Id", tapeDrive.getId() )
                .assertXPath( "PartitionId", tapeDrive.getPartitionId() )
                .assertXPath( "SerialNumber", tapeDrive.getSerialNumber() )
                .assertXPath( "State", tapeDrive.getState() )
                .assertXPath( "TapeId", tapeDrive.getTapeId() )
                .assertXPath( "Type", tapeDrive.getType() );
        return this;
    }


    public HttpResponseContentVerifier verifyTapePartitionNode(
            final String prefix,
            final TapePartition partition )
    {
        new PrefixedXPathVerifier( prefix )
                .assertXPath( "ErrorMessage", partition.getErrorMessage() )
                .assertXPath( "Id", partition.getId() )
                .assertXPath( "LibraryId", partition.getLibraryId() )
                .assertXPath( "Name", partition.getName() )
                .assertXPath( "SerialNumber", partition.getSerialNumber() )
                .assertXPath( "Quiesced", partition.getQuiesced() )
                .assertXPath( "State", partition.getState() );
        return this;
    }
    
    
    public HttpResponseContentVerifier verifyTapeNode(
            final String prefix,
            final Tape tape )
    {
        new PrefixedXPathVerifier( prefix )
                .assertXPath( "AvailableRawCapacity", tape.getAvailableRawCapacity() )
                .assertXPath( "BarCode", tape.getBarCode() )
                .assertXPath( "BucketId", tape.getBucketId() )
                .assertXPath( "StorageDomainMemberId", tape.getStorageDomainMemberId() )
                .assertXPath( "DescriptionForIdentification", tape.getDescriptionForIdentification() )
                .assertXPath( "FullOfData", Boolean.valueOf( tape.isFullOfData() ) )
                .assertXPath( "Id", tape.getId() )
                .assertXPath( "LastAccessed", tape.getLastAccessed() )
                .assertXPath( "LastCheckpoint", tape.getLastCheckpoint() )
                .assertXPath( "LastModified", tape.getLastModified() )
                .assertXPath( "LastVerified", tape.getLastVerified() )
                .assertXPath( "PartitionId", tape.getPartitionId() )
                .assertXPath( "PreviousState", tape.getPreviousState() )
                .assertXPath( "SerialNumber", tape.getSerialNumber() )
                .assertXPath( "State", tape.getState() )
                .assertXPath( "TotalRawCapacity", tape.getTotalRawCapacity() )
                .assertXPath( "Type", tape.getType() );
        return this;
    }
    
    
    private class PrefixedXPathVerifier
    {
        PrefixedXPathVerifier( final String prefix )
        {
            m_prefix = prefix;
        }
        
        
        public <T> PrefixedXPathVerifier assertXPath( final String xPathExpression, final T value )
        {
            m_driver.assertResponseToClientXPathEquals(
                    m_prefix + xPathExpression,
                    ( value == null ) ? "" : value.toString() );
            return this;
        }


        private final String m_prefix;
    }//end inner class


    private final MockHttpRequestDriver m_driver;
}
