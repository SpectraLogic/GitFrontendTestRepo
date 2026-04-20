/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.aspectj;


import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.apache.log4j.Logger;
import org.aspectj.lang.JoinPoint;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;


/**
 * The primary purpose of this aspect is to detect and attmpt to stop threads
 * created during a test method that stay alive beyond the end/exit of the
 * method (no matter how the method exits). It also clears out some final static
 * collections of threads and non-thread resources, also after a test methods
 * exits. These two activities ensure the biz state independence of each test
 * method from all others.
 *  
 * Assumes all JUnit test methods, be they conceptually unit or integration, or
 * whatever, when run in groups of any size, are always configured to run one
 * test*() method at a time (i.e. strictly sequentially). If not, this aspect
 * needs updated to handle JUnit runs in which tests may execute in parallel,
 * that is, each test method instance will need its own threads set (this will
 * be uglier and more complex so we'll not code it until for sure necessary).
 * 
 * Setting OS env variable ONLY_LOG_TEST_THREAD_PROBLEMS to ANY value causes
 * thread leaks to only be logged (as errors). Not setting this env variable
 * causes leaked thread issues to both be logged and to trigger exceptions.
 * The state of this flag will be one of the first test run log entries.
 * 
 * Compile speed info to keep in mind while changing or extending this aspect:
 * 
 * https://wiki.eclipse.org/AspectJBuildSpeed
 */
public final aspect ThreadLeakHunter 
{

    /*------------------- BEGIN: CAPABILTY SECTION ---------------------------*/
    
           /* Search BEGIN to find the start of capability sections. */
    
       // pointcut
    
       // advice based on pointcut
    
       // advice helper methods
    
       // ...
    
       // fields
    
    /*-------------------- END: CAPABILTY SECTION ----------------------------*/
    
    
    
    /*----------- BEGIN: THREAD LEAK DETECTION AND COMMON CODE ---------------*/
    
    // Common code that is used by at least 2 sections of this aspect.
    
    
    /**
     * Finds all places where run() is called on implementors of Runnable.
     * 
     * In this pointcut, using 'this' instead of 'target' reduces the AspectJ
     * compiler runtime by ~25% (on clean builds). 'this' instead of 'target'
     * introduces 10X fewer places where the before() and after() based on
     * this pointcut are added to the code. In this case 'this' does this
     * without impacting correctness. 'target' is used at other places in
     * this aspect because in those contexts 'this' usage negatively impacts
     * correctness. Lesson: Always try 'this' first, and only use 'target' if
     * 'this' doesn't work.
     */
    pointcut runMethod() : this( Runnable ) &&
                           call( public void run() ) &&
                           !within( ThreadLeakHunter );


    /**
     * Finds all places where call() is called on implementors of Callable.
     */
    /*
    pointcut callMethod() : this( Callable ) &&
                            call( public * call() ) &&
                           !within( ThreadLeakHunter );
    */


    /**
     * Saves a reference to each thread used to host the running of a
     * Runnable.run() or Callable.call() impl.
     */
    before() : runMethod() //|| callMethod()
    {
        THREAD_COUNT.get().incrementAndGet();
        
        if ( s_inBetweenTestMethods )
        {
            // Threads started explictly in between test methods almost always
            // are ones used by Shutdownables as part of their shutdown process,
            // and if we put them in the THREADS set they tend to blow up the
            // test run without good cause. While all such threads exit quickly,
            // they sometimes do not exit quickly enough to avoid showing up in
            // the THREADS set too late (i.e. after "let lingering threads exit"
            // exits, or not until just before the next test method starts). To
            // avoid blowing up good test runs with these false flag cases, we
            // log and interrupt them, but we do not put them in THREADS.
            final Thread t = Thread.currentThread();
            LOG.warn( new StringBuilder( 500 ).append(
                                  "RUNNABLE started in between test methods:" )
                                  .append( t.getId() ).append( " | " )
                                  .append( t.getName() )
                                  .append( " | Created during TestCase ")
                                  .append( CREATED_DURING ) ); 
            t.interrupt();
            return;
        }
        
        if ( LOG.isDebugEnabled() )
        {
            LOG.debug( new StringBuilder( 500 ).append ( "ADDING thread " )
                                .append( Thread.currentThread().getId() )
                                .append( ": " )
                                .append( Thread.currentThread().getName() ) );
        }
        
        // Idempotent and faster than !contains() + add():
        THREADS.put( Thread.currentThread(), CREATED_DURING.get() );
    }


    /**
     * Removes the reference to a thread used to host the running of
     * a Runnable.run() or Callable.call() impl after the run() or call() exits.
     */
    after() : runMethod() //|| callMethod()
    {
        if ( ! THREADS.containsKey( Thread.currentThread() ) )
        {
            if ( LOG.isDebugEnabled() )
            {
                LOG.debug( "Thread \"" + Thread.currentThread() +
                           "\" was not previously added to THREADS set." );
            }
            return; // A very small number of threads, possibly even zero, will
                    // not be added to THREADS within before():runMethod().
                    // In these cases this after() should do nothing.
        }
        
        if ( 1 >= THREAD_COUNT.get().intValue() )
        {
            if ( LOG.isDebugEnabled() )
            {
                LOG.debug( new StringBuilder( 500 ).append ( "REMOVING thread " )
                                .append( Thread.currentThread().getId() )
                                .append( ": " )
                                .append( Thread.currentThread().getName() ) );
            }
            
            THREAD_COUNT.remove();
            THREADS.remove( Thread.currentThread() );
        }
        else
        {
            THREAD_COUNT.get().decrementAndGet();
        }
    }
    
    
    /**
     * Finds all places where a JUnit test method is called.
     *
     * Note: It seems that the version of AJDT available for Eclipse 3.7 does
     * not support pointcut patterns for Java annotations. When frontend config
     * is modified to support a newer version of Eclipse, and thus a newer
     * version of AJDT, update this pointcut to also look for JUnit test methods
     * that are indicated by the @Test annotation. This edit will likely update
     * the last execution pointcut to something like
     * "( execution( public void test*() ) || execution( @Test ) )".
     */
    pointcut testMethod( TestCase tc ) : this( tc ) &&
                                         execution( public void test*() );
    
    
    /**
     * Resets some method level globals to the values they should have at the
     * beginning of each test method.
     */
    before( TestCase tc ) : testMethod( tc )
    {
        s_inBetweenTestMethods = false;
        
        final StringBuilder sb = new StringBuilder( 300 );
        s_currentTestCase = sb.append( tc.getClass().getSimpleName() )
                              .append( '.' )
                              .append( tc.getName() ).toString();
        
        sb.delete( 0, sb.length() );
        LOG.info( sb.append( Platform.NEWLINE ).append( Platform.NEWLINE )
                    .append( "BEFORE " ).append( s_currentTestCase ) );
        
        synchronized ( SHUTDOWNABLES )
        {
            if ( ! ( THREADS.isEmpty() && SHUTDOWNABLES.isEmpty() ) )
            {
                // If here then a thread, or the thread a shutdownable should
                // have started on, was put to sleep by the JVM before executing
                // any portion of its run() method. This likely happened just
                // before the last test method finished. Then the thread was
                // woken up and placed in the THREADS or SHUTDOWNABLES sets
                // during the short time/gap between test methods. You can
                // convince yourself of this by looking closely at all of the
                // actions taken by advice afer():testMethod(). This behavior is
                // not a coding error, nor within the control of the TLH,
                // application or test desgin/code.
                
                final String m =
                   "Some test method that ran before the one that is about to.";
                logShutdownablesNotShutDown( m, true );
                logThreadLeaks( m );
                
                // We don't clear these "late comers" from SHUTDOWNABLES or
                // THREADS because if for some reason they've not cleared
                // themselves out by the time the about-to-run test method
                // exits, then there likely is a problem, and we do want an
                // exception thrown is such cases (which will be thrown by the
                // before():testMethod() advice).
            }
        }
        
        sb.delete( 0, sb.length() );
        LOG.info( sb.append( Platform.NEWLINE ).append( Platform.NEWLINE )
                    .append( "STARTING " ).append( s_currentTestCase ) );
    }

    
    /**
     * After each JUnit test method completes, this advice checks if the method
     * did not shut down any of the Shutdownables it created (directly or
     * indirectly), or if any of the Threads it used (d or i) have not yet
     * exited.
     */
    after( final TestCase tc ) : testMethod( tc )
    {
        s_inBetweenTestMethods = true;
        
        LOG.info( new StringBuilder( 300 )
                         .append( Platform.NEWLINE ).append( Platform.NEWLINE )
                         .append( "AFTER " ).append( s_currentTestCase ) );
        try
        {
            handleShutdownables( thisJoinPoint );
            handleJavaTimers();
            handleQuartzSchedulers();
            handleGenericWorkPools();
            
            if ( null != s_systemWorkPool )
            {
                s_systemWorkPool.shutdownNow();
                s_systemWorkPool = null;
            }
            
            handleThreads( thisJoinPoint );
        }
        finally
        {
            clearStaticContainers();
            verifyExitingOnJUnitHostThread( tc );
        }
    }

    
    /** If the Eclipse compiler says this method is not being called locally,
     *  this is NOT true. Adding the "unused" annotation to try and suppress
     *  this warning just triggers another warning, "Unnecessary annotation."
     */   
    private static void handleThreads( final JoinPoint tjp )
    {
        allowLingeringThreadsSomeTimeToExit();
        
        if ( ! THREADS.isEmpty() )
        {
            final String tmn = getTestMethodName( tjp );
            logThreadLeaks( tmn );
                    
            if ( THREAD_PROBLEMS_TRIGGER_EXCEPTIONS )
            {
                throw new IllegalStateException( new StringBuilder( 500 )
                   .append( tmn )
                   .append( " seems to be leaking threads." ).toString() );
            }
        }       
    }
    
   
    private static void allowLingeringThreadsSomeTimeToExit()
    {
        if ( LOG.isDebugEnabled() )
        {
            LOG.debug( "ENTERING lingering threads cleanup sleep." );
        }
        
        int i = 0;
        while ( 100 > i && ! THREADS.isEmpty() )
        {
            for ( Thread t : THREADS.keySet() )
            {
                if( TEST_THREADS_ALLOWED_TO_LEAK.contains( t.getName() ) )
                {
                    THREADS.remove( t ); // Safe because THREADS is a
                                         // concurrent skip list impl.
                }
                else
                {
                    t.interrupt();
                }
            }
            
            ++i;
            try
            {
                Thread.sleep( 100 );
            }
            catch ( final InterruptedException ex )
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException( ex );
            }
        }       
        if ( LOG.isDebugEnabled() )
        {
            LOG.debug( "EXITING lingering threads cleanup sleep." );
        }
    }
    
    
    private static void logThreadLeaks( final String tmn )
    {
        final StringBuilder sb = new StringBuilder( 5000 );
        Thread t = null;
        String tc = null;
        
        for ( Map.Entry< Thread, String > e : THREADS.entrySet() )
        {
            t = e.getKey();
            tc = e.getValue();
            
            if ( 0 < sb.length() )
            {
                sb.delete( 0, sb.length() );
            }
            for ( StackTraceElement ste : e.getKey().getStackTrace() )
            {
                sb.append( ste.toString() ).append( " < " );
            }
            LOG.error( new StringBuilder( 5000 )
                       .append( "LEAKED THREAD in test " )
                       .append( tmn )
                       .append( " | Thread created during TestCase " )
                       .append( tc )
                       .append( " | Id: " )
                       .append( t.getId() )
                       .append( " | Name: " )
                       .append( t.getName() )
                       .append( " | Stack: " )
                       .append( sb ) );
        }
    }
    
    
    private static String getTestMethodName( final JoinPoint tjp )
    {
        final Matcher m = JP_REGEX.matcher( tjp.toString() );
        if ( ! m.find() )
        {
            throw new RuntimeException(
                   "JoinPoint regex no longer matches JP.toString() results." );
        }
        return m.group( 0 );       
    }   
    
    
    private static void clearStaticContainers()
    {
        // Do NOT clear the QUARTZ_SCHEDULERS!
        
        synchronized ( TIMERS )
        {
            TIMERS.clear();
        }
        synchronized ( SHUTDOWNABLES )
        {
            SHUTDOWNABLES.clear();
        }
        synchronized ( WORK_POOLS )
        {
            WORK_POOLS.clear();
        }
        THREADS.clear();
    }
    

    private static void verifyExitingOnJUnitHostThread( final TestCase tc )
    {
        if ( ! JUNIT_HOST_THREAD.equals( Thread.currentThread().getName() ) )
        {
            final IllegalStateException e =
                        new IllegalStateException( new StringBuilder( 200 )
                    .append( "Test method '" )
                    .append( tc.getName() )
                    .append( "' exiting on a thread other than" )
                    .append( " the JUnit host thread (" )
                    .append( JUNIT_HOST_THREAD )
                    .append( "): " )
                    .append( Thread.currentThread().getName() )
                    .toString() );
            Thread.currentThread().setName( JUNIT_HOST_THREAD );
            throw e;
        }
    }
    
    
    private static volatile boolean s_inBetweenTestMethods = false;
    
    /** 
     *  More accurately this is the "most recent test method", since once set
     *  its value does not change until a next test method begins, thus, there's
     *  a small amount of time between two test methods when its value is that
     *  of the last test method to run (i.e. most recent). This value change
     *  policy is a good thing, as it allows some things going on between test
     *  methods to be easily identified WRT method most likely triggered them. 
     */
    private static volatile String s_currentTestCase = null;
    
    private static final Pattern JP_REGEX = Pattern.compile( "\\((.*?)\\)+" );
            
    private static final String JUNIT_HOST_THREAD = Thread.currentThread().getName();
    
    private static final Logger LOG = Logger.getLogger( ThreadLeakHunter.class );

    /*
    static
    {
        LOG.setLevel( Level.DEBUG );
    }
    */

    private static final boolean THREAD_PROBLEMS_TRIGGER_EXCEPTIONS;
    static
    {
        THREAD_PROBLEMS_TRIGGER_EXCEPTIONS =
                   ( null == System.getProperty( "only.log.test.thread.problems" ) );
        LOG.info( "Test run thread problems will trigger exceptions: " + 
                                           THREAD_PROBLEMS_TRIGGER_EXCEPTIONS );
    }
    
    private static final Map<Thread, String> THREADS = 
        new ConcurrentSkipListMap<>(
              new Comparator<Thread>()
              {
                  public int compare( final Thread t1, final Thread t2 )
                  {
                      // Null checks not needed; CSLS doesn't allow null values.
                      
                      final long i1 = t1.getId(), i2 = t2.getId();
                      
                      return ( i1 == i2 ) ? 0 : ( ( i1 < i2 ) ? -1 : 1 ) ;
                  }
              } );
    
    /**
     * We use AtomicInteger only because its increment and decrement syntax and
     * semantics are much simpler and cleaner than those of Integer. Since its
     * value/reference is only accessible by its owning thread, there would be
     * no risk WRT concurrency of using Integer instead--we just don't because
     * Integer use in this context would be uglier.
     */
    private static final ThreadLocal<AtomicInteger> THREAD_COUNT =
        new ThreadLocal<AtomicInteger>()
        {
            @Override
            protected AtomicInteger initialValue()
            {
                return new AtomicInteger( 0 );
            }
        };
        
        
    private static final ThreadLocal<String> CREATED_DURING =
        new ThreadLocal<String>()
        {
            @Override
            protected String initialValue()
            {
                return
                   ( null == s_currentTestCase ) ? "NONE?" : s_currentTestCase;
            }
        };
        
        
    private static final Collection<String> TEST_THREADS_ALLOWED_TO_LEAK;
    static
    {
        final Set<String> s = new HashSet< >();
        s.add( "DeadlockDetector_Test-1" );
        s.add( "DeadlockDetector_Test-2" );
        s.add( "DeadlockListenerImpl_Test-1" );
        s.add( "DeadlockListenerImpl_Test-2" );
        TEST_THREADS_ALLOWED_TO_LEAK = Collections.unmodifiableCollection( s );
    }        
    
    
    /*------------ END: THREAD LEAK DETECTION AND COMMON CODE ----------------*/
    
    
        
    /*--------------- BEGIN: SystemWorkPool LEAK PREVENTION ------------------*/
        
        
    /**
     * Finds all places where SystemWorkPool.getInstance() is called.
     */
    pointcut getSystemWorkPoolMethod() :
                              execution( public static WorkPool getInstance() );
                                
    
    
    /**
     * Collaborates with the after:testMethod() advice to make sure each test
     * methods has its own SystemWorkPool instance.
     */
    WorkPool around() : getSystemWorkPoolMethod()
    {
        if ( s_inBetweenTestMethods )
        {
            LOG.warn( new StringBuilder( 500 ).append(
                    "SYSTEM WORK POOL being accessed between test methods." )
                                  .append( " Returning an empty work pool." ) );
            
            return EMPTY_WORK_POOL;
        }
        
        synchronized ( SYSTEM_WORK_POOL_LOCK )
        {
            if ( null == s_systemWorkPool )
            {
                s_systemWorkPool = WorkPoolFactory.createWorkPool(
                                                        10, "SystemWorkPool" );
            }
        }
        
        return s_systemWorkPool;
    }
    
    
    private static volatile WorkPool s_systemWorkPool = null;
    
    private static final Object SYSTEM_WORK_POOL_LOCK = new Object();
    
    private static final WorkPool EMPTY_WORK_POOL = new WorkPool()
        {
            @Override public Future< ? > submit( Runnable task )
            {
                return null;
            }
            @Override public List< Runnable > shutdownNow()
            {
                return null;
            }
            @Override public void shutdown()
            {
                /* Quite compiler. */
            }
            @Override public boolean isTerminated()
            {
                return true;
            }
            @Override public boolean isFull()
            {
                return false;
            }
            @Override public int getActiveCount()
            {
                return 0;
            }
            @Override public boolean awaitTermination( long timeout, TimeUnit unit )
                                            throws InterruptedException
            {
                return true;
            }
            @Override public boolean isShutdown()
            {
                return true;
            }
        };
        
        
    /*---------------- END: SystemWorkPool LEAK PREVENTION -------------------*/
        
        
    /*--------------- BEGIN: GENERIC WorkPool LEAK PREVENTION ----------------*/
        
        
     /*
      * TODO: There's likely a way to combine the calls to both WPF.create*()
      *       methods into a one around(), enabling just one pointcut and around
      *       to handle both. Learn it and do it.
      */       
        
        
    pointcut workPoolCreate( int maxThrds, String baseName ) :
           args( maxThrds, baseName ) &&
           execution( public static WorkPool createWorkPool( int , String ) );
    
    
    WorkPool around( final int maxThrds, final String baseName ) :
                                            workPoolCreate( maxThrds, baseName )
    {
        final WorkPool wp = proceed( maxThrds, baseName );
        synchronized ( WORK_POOLS )
        {
            WORK_POOLS.add( wp );
        }
        return wp;
    }
    
    
    pointcut boundedWorkPoolCreate(
                                int maxQueued, int maxThrds, String baseName ) :
         args( maxQueued, maxThrds, baseName ) &&
         execution( public static WorkPool createBoundedWorkPool(
                                                       int, int , String ) );
    
    
    WorkPool around( final int maxQueued,
                     final int maxThrds, final String baseName ) :
                          boundedWorkPoolCreate( maxQueued, maxThrds, baseName )
    {
        final WorkPool wp = proceed( maxQueued, maxThrds, baseName );
        synchronized ( WORK_POOLS )
        {
            WORK_POOLS.add( wp );
        }
        return wp;
    }
    
    
    private static void handleGenericWorkPools()
    {
        synchronized ( WORK_POOLS )
        {
            for ( WorkPool wp : WORK_POOLS )
            {
                wp.shutdownNow();
            }
        }
    }
    
    
    private static final Set<WorkPool> WORK_POOLS =
                                                new HashSet<>( 100, (long)1.0 );
    
        
    /*---------------- END: GENERIC WorkPool LEAK PREVENTION -----------------*/
        
        
        
    /*-------------- BEGIN: org.quartz.Scheduler LEAK PREVENTION -------------*/
        
        
    pointcut startScheduler( Scheduler s ) : target( s ) &&
                                             call( public void start() );
    
    
    after( final Scheduler s ) returning() : startScheduler( s )
    {
        if ( s_inBetweenTestMethods )
        {
            LOG.warn( new StringBuilder( 500 ).append(
                            "Quartz Scheduler created between test methods: [" )
                            .append( s.hashCode()  )
                            .append(  "]=" )
                            .append(  s.getClass().getName() ) );
        }
        
        synchronized ( QUARTZ_SCHEDULERS )
        {
            QUARTZ_SCHEDULERS.add( s );
        }
    }
    
    
    private static void handleQuartzSchedulers()
    {
        synchronized ( QUARTZ_SCHEDULERS )
        {
            for ( Scheduler s : QUARTZ_SCHEDULERS )
            {
                try
                {
                    s.clear();
                    s.shutdown();
                }
                catch ( final SchedulerException ex )
                {
                    LOG.warn( ex );
                }
            }
        }
    }
    
    
    private static final Set<Scheduler> QUARTZ_SCHEDULERS =
                                                new HashSet<>( 100, (long)1.0 );
    
        
    /*--------------- END: org.quartz.Scheduler LEAK PREVENTION --------------*/
        
                                                
                                                
    /*----------------- BEGIN: Java Timer LEAK PREVENTION --------------------*/
                                                
                                                
    after() returning( final Timer t ) : call( public Timer.new( .. ) )
    {
        if ( s_inBetweenTestMethods )
        {
            LOG.warn( new StringBuilder( 500 ).append(
                                "Java Timer created between test methods: [" )
                                .append( t.hashCode()  )
                                .append( "]=" )
                                .append( t.getClass().getName() ) );
        }
        
        synchronized ( TIMERS )
        {
            // Idempotent and faster than !contains() + add():
            TIMERS.add( t );
            if( LOG.isDebugEnabled() )
            {
                LOG.debug( "TIMERS.size() -> " + TIMERS.size() );
            }
        }
    }   
    
    
    private static void handleJavaTimers()
    {
        synchronized ( TIMERS )
        {
            for( Timer t : TIMERS )
            {
                if ( LOG.isDebugEnabled() )
                {
                    LOG.debug( "Canceling Timer " + t.toString() );
                }
                t.cancel();
            }
        }
    }
    
    
    private static final Set<Timer> TIMERS = new HashSet<>( 100, (long)1.0 );
                
    
    /*------------------ END: Java Timer LEAK PREVENTION ---------------------*/
                                     
        
        
    /*--------------- BEGIN: Shutdownable LEAK PREVENTION --------------------*/
        
        
    /**
     * Saves a reference to every instance of subclasses of BaseShutdownable.
     */
    after( final BaseShutdownable bs ) : this( bs ) &&
                                         execution( BaseShutdownable+.new(..) )
    {
        if ( s_inBetweenTestMethods )
        {
            LOG.warn( new StringBuilder( 500 ).append(
                        "BaseShutdownable created between test methods: [" )
                        .append( bs.hashCode()  )
                        .append( "]=" )
                        .append( bs.getClass().getName() ) );
        }
        
        if ( LOG.isDebugEnabled() )
        {
            LOG.debug( new StringBuilder( 500 )
                               .append( "new BaseShutdownable() executing: [" )
                               .append( bs.hashCode()  )
                               .append( "]=" )
                               .append( bs.getClass().getName() ) );
        }
        
        synchronized ( SHUTDOWNABLES )
        {
            // Idempotent and faster than !contains() + add():
            SHUTDOWNABLES.add( bs );
        }
    }
    
    
    /** If the Eclipse compiler says this method is not being called locally,
     *  this is NOT true. Adding the "unused" annotation to try and suppress
     *  this warning just triggers another warning, "Unnecessary annotation."
     */   
    private static void handleShutdownables( final JoinPoint jp )
    {
        synchronized ( SHUTDOWNABLES )
        {
            if ( ! SHUTDOWNABLES.isEmpty() )
            {
                final String tmn = getTestMethodName( jp );
                logShutdownablesNotShutDown( tmn, false );
                shutDownShutdownables();
            }
        }
    }
    
    
    private static void logShutdownablesNotShutDown(
                                           final String tmn, boolean forceLog )
    {
        synchronized ( SHUTDOWNABLES )
        {
            if ( SHUTDOWNABLES.isEmpty() )
            {
                return;
            }
            if ( forceLog || LOG.isDebugEnabled() )
            {
                final StringBuilder sb = new StringBuilder( 1000 );
                sb.append( "SHUTDOWNABLES NOT SHUT DOWN | " )
                  .append( tmn )
                  .append( " :" );
                
                for ( BaseShutdownable shtdn : SHUTDOWNABLES )
                {
                    if( ! shtdn.isShutdown() )
                    {
                        sb.append( " [" )
                          .append( shtdn.hashCode() )
                          .append( "]=" )
                          .append( shtdn.getClass().getName() )
                          .append( " :" );
                    }
                }
                LOG.warn( sb ); // Sometimes called when DEBUG is NOT enabled,
                                // so do NOT make a debug() call here.
            }
        }
    }
    
    
    private static void shutDownShutdownables()
    {
        final StringBuilder buf = new StringBuilder( 300 );
        synchronized ( SHUTDOWNABLES )
        {
            for ( BaseShutdownable sb : SHUTDOWNABLES )
            {
                if ( ! sb.isShutdown() )
                {
                    if ( 0 < buf.length() )
                    {
                        buf.delete( 0, buf.length() );
                    }
                    if ( LOG.isDebugEnabled() )
                    {
                        LOG.debug( buf.append( "ASPECT shut down of Shutdowanable: [" )
                                     .append( sb.hashCode() )
                                     .append( "]=" )
                                     .append( sb.getClass().getName() ) );
                    }
                    sb.shutdown();
                }
            }
        }
    }
    
    
    private static final Set<BaseShutdownable> SHUTDOWNABLES =
                                                new HashSet<>( 100, (long)1.0 );
        
    
    /*---------------- END: Shutdownable LEAK PREVENTION ---------------------*/
}
