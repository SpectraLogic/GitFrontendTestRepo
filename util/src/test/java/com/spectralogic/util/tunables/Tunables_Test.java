/*******************************************************************************
 *
 * Copyright C 2026, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.tunables;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class Tunables_Test
{
    @BeforeEach
    void resetBefore()
    {
        // Defend against state left behind by another test class running first in this JVM.
        Tunables.uninstall();
    }


    @AfterEach
    void resetAfter()
    {
        Tunables.uninstall();
    }


    @Test
    void getters_returnCompiledDefaults_bothBeforeAndAfterInstall()
    {
        // Before install: int getter returns its compiled default.
        // (Also logs a one-time pre-install warning, which we don't assert on here.)
        assertEquals( 1000, Tunables.requestPagingPropertiesMaxPageLength() );

        // Before install: long getter returns its compiled default.
        assertEquals( 1024L * 1024 * 1024 * 1024, Tunables.cacheManagerMaxChunkSize() );

        // install(null) treats the absence of a KeyValueService as "no overrides", logs INFO.
        Tunables.install( null );

        // After install: same defaults, no further pre-install warnings on subsequent reads.
        assertEquals( 1000, Tunables.requestPagingPropertiesMaxPageLength() );
        assertEquals( 1024L * 1024 * 1024 * 1024, Tunables.cacheManagerMaxChunkSize() );
    }
}
