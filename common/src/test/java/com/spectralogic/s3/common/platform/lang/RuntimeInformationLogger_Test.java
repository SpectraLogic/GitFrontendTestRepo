/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.lang;




import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class RuntimeInformationLogger_Test 
{
    @Test
    public void testLogRuntimeInformationWithIdempotencyLogicDoesNotBlowUp()
    {
        RuntimeInformationLogger.logRuntimeInformation();
        RuntimeInformationLogger.logRuntimeInformation();
    }
}
