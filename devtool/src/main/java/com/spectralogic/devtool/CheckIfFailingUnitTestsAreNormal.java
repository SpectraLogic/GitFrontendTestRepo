/*******************************************************************************
*
* Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
* All rights reserved.
*
******************************************************************************/
package com.spectralogic.devtool;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Platform;


public final class CheckIfFailingUnitTestsAreNormal
{  
   public static void main( String[] args ) // $codepro.audit.disable illegalMainMethod
   {
       final Map< String, String > classToTest = new HashMap<>();
       classToTest.put( "TapeBlobStoreIntegration_Test ",
               "testTasksScheduledToTapeDriveWhenMultiplePartitions FAILED" );
       classToTest.put( "CreateStorageDomainRequestHandler",
               " -> testCreateMaxAutoVerificationHasValidValueReturns201" );                                
       classToTest.put( "OnlineTapeProcessor_Test",
               "testWillNotOnlineTapeUntilTapePartitionInStateToAllowIt" );
       classToTest.put( "BlobStoreDriverImpl_Test", "several" );
       
       LOG.info( classToTest.toString().replaceAll( ",", Platform.NEWLINE ) );
   }


   private final static Logger LOG = Logger.getLogger( CheckIfFailingUnitTestsAreNormal.class );
}