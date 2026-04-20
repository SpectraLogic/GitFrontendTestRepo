/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.find;

import java.io.File;
import java.io.IOException;



import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class FlagDetector_Test
{
    @Test
    public void testIsFlagSetReturnsTrueWhenFileExists() throws IOException
    {
        final String flagName = "testisflagsetreturnstruewhenfileexists";
        
        final File flagFile = FlagDetector.getFlagFile( flagName );
        flagFile.createNewFile();
        
        try
        {
            assertTrue(
                    FlagDetector.isFlagSet( flagName ),
                    "Shoulda returned true since the flag file was there.");
        }
        finally
        {
            flagFile.delete();
        }
    }
    
    
    @Test
    public void testIsFlagSetReturnsFalseWhenFileDoesNotExist()
    {
        final String flagName = "testisflagsetreturnsfalsewhenfiledoesnotexist";
        
        final File flagFile = FlagDetector.getFlagFile( flagName );
        assertFalse(
                flagFile.exists(),
                "Shoulda not seen a flag file that we never set.");
        
        assertFalse(
                FlagDetector.isFlagSet( flagName ),
                "Shoulda returned false since the flag was not there.");
    }
}
