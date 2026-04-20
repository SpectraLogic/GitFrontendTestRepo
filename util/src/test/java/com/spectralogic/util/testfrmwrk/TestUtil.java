/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.testfrmwrk;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import net.bytebuddy.implementation.bytecode.Throw;
import org.apache.log4j.Logger;

import com.spectralogic.util.exception.FailureType;
import com.spectralogic.util.exception.FailureTypeObservable;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.find.PackageContentFinder;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.predicate.UnaryPredicate;
import com.spectralogic.util.thread.wp.SystemWorkPool;
import org.opentest4j.AssertionFailedError;

/**
 * Contains various test utilities.
 */
public final class TestUtil
{
    private TestUtil()
    {
        // singleton
    }
    
    
    public static void sleep( final int millis )
    {
        try
        {
            Thread.sleep( millis );
        }
        catch ( final InterruptedException ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    public static void assertEventually( final int timeoutInSeconds, final Runnable r )
    {
        final Duration duration = new Duration();
        while ( true )
        {
            try
            {
                r.run();
                return;
            }
            catch ( final Throwable t )
            {
                if ( duration.getElapsedSeconds() > timeoutInSeconds )
                {
                    throw t;
                }
                
                try
                {
                    Thread.sleep( 10 );
                }
                catch ( final InterruptedException ex )
                {
                    throw new RuntimeException( ex );
                }
            }
        }
    }
    
    
    public static void assertSame( final List< ? > expected, final List< ? > actual )
    {
        if ( expected == actual )
        {
            return;
        }
        if ( null == expected )
        {
            throw new RuntimeException( "Expected null, but was " + actual );
        }
        if ( null == actual )
        {
            throw new RuntimeException( "Expected " + expected + ", but was null." );
        }
        
        assertSame( new HashSet<>( expected ), new HashSet<>( actual ) );
        
        if ( !expected.equals( actual ) )
        {
            throw new RuntimeException( "Expected " + expected + ", but was " + actual + "." );
        }
    }
    
    
    public static void assertSame( final Set< ? > expected, final Set< ? > actual )
    {
        if ( expected == actual )
        {
            return;
        }
        if ( null == expected )
        {
            throw new RuntimeException( "Expected null, but was " + actual );
        }
        if ( null == actual )
        {
            throw new RuntimeException( "Expected " + expected + ", but was null." );
        }
        if ( expected.containsAll( actual ) && actual.containsAll( expected ) )
        {
            return;
        }
        
        String msg = "Contents of set were not as expected:";
        
        final Set< Object > missings = new HashSet<>( expected );
        missings.removeAll( actual );
        for ( final Object missing : missings )
        {
            msg += Platform.NEWLINE + "Expected '" + missing + "', but object was not in set.";
        }

        final Set< Object > extras = new HashSet<>( actual );
        extras.removeAll( expected );
        for ( final Object extra : extras )
        {
            msg += Platform.NEWLINE + "Did not expect '" + extra + "'.";
        }
        
        throw new RuntimeException( msg );
    }

    
    public static Throwable assertThrows(
            String message, 
            final FailureType expectedFailureType,
            final BlastContainer codeThatShouldThrowException )
    {
        message = ( null == message ) ? "" : message + "  ";
        final Throwable retval = 
                assertThrows( message, FailureTypeObservableException.class, codeThatShouldThrowException );
        final FailureType actualFailureType = ( (FailureTypeObservable)retval ).getFailureType();
        if ( !actualFailureType.equals( expectedFailureType ) )
        {
            throw new RuntimeException( 
                    message + "Expected a " + FailureTypeObservable.class
                        + " with a failure type of " + expectedFailureType
                        + ", but the failure type was " + actualFailureType 
                        + " (code = " + actualFailureType.getHttpResponseCode() 
                        + "/" + actualFailureType.getCode() + ").",
                    retval );
        }
        return retval;
    }

    
    /**
     * There are facilities built into JUnit (like using <code>@Test( expected = {class} )</code>),
     * but every built-in facility has some drawbacks.  The example just given won't verify that
     * the exception occurs where it should occur.  Thus, this facility exists to explicitly assert
     * that a particular piece of code within a test throws one of the expected exceptions.
     */
    public static Throwable assertThrows(
            String message, 
            final Set< Class< ? extends Throwable > > expectedExceptionTypes,
            final BlastContainer codeThatShouldThrowException )
    {
        Validations.verifyNotNull( "Expected exception type", expectedExceptionTypes );
        Validations.verifyNotNull( "Code that should throw exception", codeThatShouldThrowException );
        
        message = ( null == message ) ? "" : message + "  ";
        
        final Throwable exception = getException( codeThatShouldThrowException );
        if ( exception == null )
        {
            final StringBuilder sb = new StringBuilder( 200 );
            for ( Class< ? extends Throwable > c : expectedExceptionTypes )
            {
                sb.append( c.getSimpleName() ).append( ' ' );
            }
            throw new RuntimeException( message +
                "No exception was thrown. Expected one of: " + sb.toString() );
        }
        
        for ( Class< ? extends Throwable > c : expectedExceptionTypes )
        {
            if ( c.isAssignableFrom( exception.getClass() ) )
            {
                LOG.info( "Exception was expected: " + message, exception );
                return exception;
            }
        }
        
        final StringBuilder sb = new StringBuilder( 200 );
        for ( Class< ? extends Throwable > c : expectedExceptionTypes )
        {
            sb.append( c.getSimpleName() ).append( ' ' );
        }
        throw new RuntimeException( message + "Expected one of " + sb.toString()
            + ", but " + exception.getClass().getSimpleName() + " was thrown.",
                                                                    exception );
    }
    
    
     /**
     * There are facilities built into JUnit (like using <code>@Test( expected = {class} )</code>),
     * but every built-in facility has some drawbacks.  The example just given won't verify that
     * the exception occurs where it should occur.  Thus, this facility exists to explicitly assert
     * that a particular piece of code within a test throws the expected exception.
     */
    public static Throwable assertThrows(
            String message, 
            final Class< ? extends Throwable > expectedExceptionType,
            final BlastContainer codeThatShouldThrowException )
    {
        final Set< Class< ? extends Throwable > > s = new HashSet<>();
        s.add( expectedExceptionType );
        return assertThrows( message, s, codeThatShouldThrowException );
    }   
    
    
    private static Throwable getException( final BlastContainer codeThatShouldThrowException )
    {
        Validations.verifyNotNull( "Code that should throw exception", codeThatShouldThrowException );
        try
        {
            codeThatShouldThrowException.test();
            return null;
        }
        catch ( final Throwable ex )
        {
            Validations.verifyNotNull( "Shut up CodePro", ex );
            return ex;
        }
    }
    
    
    public static void assertJvmEncodingIsUtf8()
    {
        /* Do a stronger test than what might be expected necessary of the JVM's
         * default char encoding to avoid possibility of running into a JVM bug
         * that was demonstrably in at least Oracle's Java 5.
         * http://stackoverflow.com/questions/1749064/how-to-find-the-default-charset-encoding-in-java
         */
        if ( !"UTF-8".equals( System.getProperty( "file.encoding" ) ) ||
             !Charset.defaultCharset().name().equals( "UTF-8" ) )
        {
            throw new RuntimeException(
                    "The default JVM charset must be 'UTF-8' in testing and production." + Platform.NEWLINE
                    + " If you're getting this error while running tests in eclipse you can"
                    + " fix the issue in your eclipse.ini:" + Platform.NEWLINE
                    + " 1. Close eclipse." + Platform.NEWLINE
                    + " 2. Open the eclipse.ini in a text editor." + Platform.NEWLINE
                    + " 3. Find the line '-vmargs' or add it to the end of the file." + Platform.NEWLINE
                    + " 4. After '-vmargs' add the line '-Dfile.encoding=UTF8'." + Platform.NEWLINE
                    + " 5. Re-open eclipse." + Platform.NEWLINE );
        }
    }
    
    
    public interface BlastContainer
    {
    public void test() throws Throwable;
    }
        
    
    /**
     * Call with getClass() in a package with enums when code coverage tools
     * erroneously show less than 100% code coverage on enums.
     */
    public static void loadStaticallyGeneratedEnumCode( final Class< ? > seed )
    {
        final Set< Class< ? > > classes = new PackageContentFinder( null, seed, null )
                .getClasses( new IsEnumClassPredicate() );
        for ( final Class< ? > clazz : classes )
        {
            @SuppressWarnings( "unchecked" )
            final Class< ? extends Enum< ? > > enumType = (Class< ? extends Enum< ? > >)clazz;
            try
            {
                final Enum< ? >[] enumConstants = enumType.getEnumConstants();
                if ( enumConstants.length > 0 )
                {
                    final Method valueOfMethod = clazz.getMethod( "valueOf", String.class );
                    valueOfMethod.setAccessible( true );
                    valueOfMethod.invoke( null, enumConstants[ 0 ].name() );
                }
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException(
                        "Error while loading enum type " + clazz.getCanonicalName(),
                        ex );
            }
        }
    }


    private static final class IsEnumClassPredicate implements UnaryPredicate< Class< ? > >
    {
        @Override
        public boolean test( final Class< ? > element )
        {
            return element.isEnum();
        }
    } //end inner class
    
    
    /**
     * <em>If</em> a Runnable impl's run() changes the name of its host Thread,
     * then use this method or its sibling to run it, <em>rather</em> than
     * directly calling <em>implInstance.run()</em> within a test method. Use
     * this version if the test requires the Runnable impl <em>to</em> throw an
     * execption. Use its sibiling if the Runnable impl is <em>not</em> expected
     * to throw an exception.
     * 
     * Directly calling <em>implInstance.run()</em> likely will result in the
     * test method exiting with a name set on its host Thread that is not the
     * name the Thread had when the test method started. This behavior is not
     * good, because it turns the test run log file entires into a confusing
     * mess.
     * 
     * @param r
     * @throws Throwable
     */
    public static void invokeAndWaitChecked( final Runnable r ) throws Throwable
    {
        try
        {
            SystemWorkPool.getInstance().submit( r ).get();
        }
        catch ( final InterruptedException ex )
        {
            throw new RuntimeException( ex );
        }
        catch ( final ExecutionException ee )
        {
            throw ee.getCause();
        }
    }
    
    
    /**
     * <em>If</em> a Runnable impl's run() changes the name of its host Thread,
     * then use this method or its sibling to run it, <em>rather</em> than
     * directly calling <em>implInstance.run()</em> within a test method. Use
     * this version if the test expects the Runnable impl to <em>not</em> throw
     * execption. Use its sibiling if the test needs the Runnable impl
     * <em>to</em> throw an exception.
     *  
     * Directly calling <em>implInstance.run()</em> likely will result in the
     * test method exiting with a name set on its host Thread that is not the
     * name the Thread had when the test method started. This behavior is not
     * good, because it turns the test run log file entires into a confusing
     * mess.
     * 
     * @param r
     */
    public static void invokeAndWaitUnchecked( final Runnable r )
    {
        try
        {
            SystemWorkPool.getInstance().submit( r ).get();
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    /**
     * This is a pseudo-lock, as in, it lacks many things it would need to make
     * it anything near a genuine "file lock". All actors in test use cases in
     * which this "lock" is used must know it's in use and not do things to
     * subvert it, at least if they want it to simulate a locked file.
     * 
     * If the process running the tests in which this "lock" is used does not
     * own the file being locked, this method will likely throw a permissions
     * exception.
     */
    public static LockedFile lockFile( final File f )
    {
        return new LockedFile( f );
    }
    
    
    /** */
    public static final class LockedFile
    {
        private LockedFile( final File f )
        {
            m_read =    f.canRead();
            m_write =   f.canWrite();
            m_execute = f.canExecute();
            f.setReadable(   false, false );
            f.setWritable(   false, false );
            f.setExecutable( false, false );
            m_file = f;
        }
        
        
        public void unlock()
        {
            m_file.setReadable(   m_read,    true );
            m_file.setWritable(   m_write,   true );
            m_file.setExecutable( m_execute, true );
        }
        
        
        private final File m_file;
        private final boolean m_read;
        private final boolean m_write;
        private final boolean m_execute;
    }

    public static void waitUpTo(final int time, final TimeUnit timeUnit, final Runnable runnableThatMayThrow) {
        final long endTime = System.currentTimeMillis() + timeUnit.toMillis(time);
        while (System.currentTimeMillis() < endTime) {
            try {
                runnableThatMayThrow.run();
                return; // Success
            } catch (final AssertionError e) {
                sleep(10);
            }
        }
        runnableThatMayThrow.run();
    }
    
    
    private final static Logger LOG = Logger.getLogger( TestUtil.class );
}
