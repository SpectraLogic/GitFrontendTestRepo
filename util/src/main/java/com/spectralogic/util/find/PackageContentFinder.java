/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.find;


import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.predicate.UnaryPredicate;

/**
 * The package content finder will find content in the package specified via
 * its constructor args.  <b>Note that content will be found in said package as
 * well as any subpackage thereof.</b><br><br>
 *
 * Note that this class works differently depending on whether it runs from the
 * command line from inside Eclipse.  Consequently, any tests that rely on this
 * class will also be particularly prone to test failures only from Eclipse or
 * only from the command line.
 */
public final class PackageContentFinder
{
    /**
     * @param rootPackageNameDeliminatedByPeriod
     * Optional - If specified, will search for content that is in this package
     * (or any subpackage thereof).  If not specified, will search for content
     * that is in <i>randomClassInComponent</i>'s package (or any supackage
     * thereof).<br><br>
     *
     * @param randomClassInComponent
     * Required - Any class that is in the component (e.g. gui or util) that
     * you want to find content in.<br><br>
     *
     * @param directoryContainingJarFiles
     * Optional - If null, will search for content from the directory structure.
     * If non-null, will search for content in jars.  Production package or test
     * package content will be searched, but not both.  If you want test package
     * content (e.g. /util/test/unit/... or /service/test/integrate/...), always
     * pass null.  If you want production package content (e.g. /util/src/... or
     * /service/src/...), pass in the directory containing jar files.
     */
    public PackageContentFinder(
                    String rootPackageNameDeliminatedByPeriod, // intentionally not final
                    final Class<?> randomClassInComponent,
                    final String directoryContainingJarFiles )
    {
        if ( null == randomClassInComponent )
        {
            throw new IllegalArgumentException(
                                 "Config param cannot be null!" ); 
        }
        if ( null == rootPackageNameDeliminatedByPeriod
                        || rootPackageNameDeliminatedByPeriod.isEmpty() )
        {
            rootPackageNameDeliminatedByPeriod =
                randomClassInComponent.getPackage().getName();
        }

        m_packageClasses = new HashSet<>( 16, 1.0f );
        m_packageResources = new HashSet<>( 8, 1.0f );
        findContentInPackage(
                rootPackageNameDeliminatedByPeriod,
                randomClassInComponent,
                directoryContainingJarFiles );
    }


    /**
     * @param filter
     * @return set of classes (outer <font color = red>AND inner</font>)
     */
    public Set< Class<?> > getClasses( final UnaryPredicate< Class<?> > filter )
    {
        final Set< Class<?> > retval = new HashSet<>( m_packageClasses.size(), 1.0f );
        for ( Class<?> cl : m_packageClasses )
        {
            if ( null == filter || filter.test( cl ) )
            {
                retval.add( cl );
            }
        }

        return retval;
    }


    /**
     * @param filter
     * @return set of resource names
     */
    public Set< String > getResources( final UnaryPredicate< String > filter )
    {
        final Set< String > retval = new HashSet<>( m_packageResources.size(), 1.0f );
        for ( String resource : m_packageResources )
        {
            if ( null == filter || filter.test( resource ) )
            {
                retval.add( resource );
            }
        }

        return retval;
    }


    /**
     * @param clazz
     */
    private void packageClassFound( final Class<?> clazz )
    {
        if ( clazz.isSynthetic() )
        {
            return;
        }

        m_packageClasses.add( clazz );

        for ( Class<?> innerClass : clazz.getDeclaredClasses() )
        {
            packageClassFound( innerClass );
        }
    }


    /**
     * @param rootPackageNameDeliminatedByPeriod
     * @param randomClassInComponent
     * @param directoryContainingJarFiles
     * @throws IOException
     */
    private void findContentInPackage(
                    final String rootPackageNameDeliminatedByPeriod,
                    final Class<?> randomClassInComponent,
                    final String directoryContainingJarFiles )
    {
        final Duration duration = new Duration();
        
        /*
         * If a directory containing jar files is provided, search for
         * content in said jars files.
         */
        if ( null != directoryContainingJarFiles )
        {
            final File dir = new File( directoryContainingJarFiles );
            if ( ! dir.exists() )
            {
                throw new IllegalArgumentException( new StringBuilder( 200 )
                    .append( "Directory supplied does not exist! (" ) 
                    .append( directoryContainingJarFiles )
                    .append( ')' ).toString() );
            }
            if ( ! dir.isDirectory() )
            {
                throw new IllegalArgumentException( new StringBuilder( 200 )
                    .append( "Directory supplied is not a directory! (" ) 
                    .append( directoryContainingJarFiles )
                    .append( ')' ).toString() );
            }

            final StringBuilder logMessageSb = new StringBuilder( 200 )
                .append( this.getClass().getSimpleName() )
                .append( " searched for content in jars in " ) 
                .append( directoryContainingJarFiles )
                .append( " (" ); 
            final File [] jars = dir.listFiles();
            if ( null == jars || 0 == jars.length )
            {
                throw new IllegalArgumentException( new StringBuilder( 200 )
                    .append( "Directory supplied does not contain any jars! (" ) 
                    .append( directoryContainingJarFiles )
                    .append( ')' ).toString() );
            }
            for ( File jar : jars )
            {
                if ( ! jar.getAbsolutePath().endsWith( JAR_SUFFIX ) )
                {
                    throw new IllegalArgumentException( new StringBuilder( 200 )
                        .append( "Directory supplied contains non-jar files! (" ) 
                        .append( directoryContainingJarFiles )
                        .append( ')' ).toString() );
                }
                logMessageSb.append( ' ' ).append( jar.getName() ).append( ' ' );
                parsePackageContentFromJar(
                        rootPackageNameDeliminatedByPeriod,
                        jar.getAbsolutePath() );
            }
                
            logMessageSb.append( ')' );
            logMessageSb.append( getPackageContentFoundStringForLoggingPurposes(  
                    duration.getElapsedMillis() ) );
            LOG.info( logMessageSb.toString() );

            return;
        }


        /*
         * If a directory containing jar files is not provided, search for
         * content in directory structure (or jar in the event that the source
         * is not present).  Note that we have to get the URI to create a file
         * so we can ultimately get a file path that does not use '%20' for a
         * space character
         */
        final URI codePath;

        try
        {
            codePath = randomClassInComponent
                        .getProtectionDomain()
                        .getCodeSource()
                        .getLocation()
                        .toURI();
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
        
        File fileForCodePath;
        try
        {
            fileForCodePath = new File(codePath);
        }
        catch ( final IllegalArgumentException ex )
        {
            LOG.debug( "Will parse file from code path assuming it's a jar.", ex );
            String filePart = codePath.toString().substring( 0, codePath.toString().indexOf( '!' ) );
            filePart = filePart.substring( filePart.indexOf( "jar:file:" ) + "jar:file:".length() );
            fileForCodePath = new File( filePart );
        }
        
        if ( fileForCodePath.isFile() && fileForCodePath.getName().endsWith( ".class" ) )
        {
            File classesDir = fileForCodePath;
            while ( null != classesDir )
            {
                if ( classesDir.getName().equals( "classes" ) )
                {
                    fileForCodePath = classesDir;
                    break;
                }
                classesDir = classesDir.getParentFile();
            }
        }
        
        final String codePathString = fileForCodePath.getAbsolutePath();
        if ( fileForCodePath.isDirectory() )
        {
            final StringBuilder sb = new StringBuilder( 200 );
            sb.append( codePathString )
              .append( File.separator )
              .append( rootPackageNameDeliminatedByPeriod.replace( '.', File.separatorChar ) )
              .append( File.separator );

            parsePackageContentFromDirectory(
                            rootPackageNameDeliminatedByPeriod,
                            sb.toString() );

            LOG.debug( new StringBuilder( 200 )
                .append( this.getClass().getSimpleName() )
                .append( " searched for content below " ) 
                .append( codePathString )
                .append( getPackageContentFoundStringForLoggingPurposes(  
                        duration.getElapsedMillis() ) )
                .toString() );
            return;
        }
        if ( codePathString.contains( JAR_SUFFIX ) )
        {
            parsePackageContentFromJar(
                            rootPackageNameDeliminatedByPeriod,
                            codePathString );

            LOG.debug( new StringBuilder( 200 )
                .append( this.getClass().getSimpleName() )
                .append( " searched for content in jar " ) 
                .append( codePathString )
                .append( getPackageContentFoundStringForLoggingPurposes( 
                        duration.getElapsedMillis() ) )
                .toString() );
            return;
        }


        /*
         * If protection domain -> code source did not yield a directory or JAR,
         * something is *really* wrong.
         */
        final StringBuilder sb = new StringBuilder( 200 );
        sb.append( "directoryContainingJarFiles provided was null, " ); 
        sb.append( "indicating that content should be derived from the " ); 
        sb.append( "directory structure; however, " ); 
        sb.append( "the code source / path for the seed class provided " ); 
        sb.append( "is NOT a directory (expected to find root directory " ); 
        sb.append( "for the code)  " ); 
        sb.append( "rootPackage=" ).append( rootPackageNameDeliminatedByPeriod ); 
        sb.append( "; seedClassInPackage=" ).append( randomClassInComponent ); 
        sb.append( "; directoryContainingJarFiles=" ).append(  
                        directoryContainingJarFiles );
        sb.append( "; codePath=" ).append( codePathString ); 
        throw new IllegalArgumentException( sb.toString() );
    }


    /**
     * @param elapsedTime
     * @return String
     */
    private String getPackageContentFoundStringForLoggingPurposes( final long elapsedTime )
    {
        return new StringBuilder( 200 )
            .append( " and found ") 
            .append( m_packageClasses.size() )
            .append( " outer classes, " ) 
            .append( m_packageResources.size() )
            .append( " resources in " ) 
            .append( elapsedTime )
            .append( " ms." ) 
            .toString();
    }


    /**
     * @param rootPackageNameDeliminatedByPeriod
     * @param codePath
     */
    private void parsePackageContentFromJar(
                    final String rootPackageNameDeliminatedByPeriod,
                    final String codePath )
    {
        final JarFile jar;
        final File file = new File( extractJarFilePath( codePath ) );
        
        final String classpath = System.getProperty( "java.class.path" ); 
        final String fileSubstring = file.getName();
        if ( ! classpath.contains( fileSubstring ) )
        {
            LOG.debug( new StringBuilder( 200 )
                .append( fileSubstring )
                .append( " is not in the class path.  " )
                .append( "If you did not mean to search it, please remove it from the search path." )
                .toString() );
        }
        
        try
        {
            jar = new JarFile( file );
        }
        catch ( final IOException ioe )
        {
                LOG.error( new StringBuilder( 200 )
                    .append( this.getClass().getSimpleName() )
                    .append( " - Unable to create jar file from file '" )  
                    .append( file )
                    .append( "'." ), ioe ); 
            return;
        }

        final Enumeration< ? extends JarEntry > jarEntries = jar.entries();
        while ( jarEntries.hasMoreElements() )
        {
            parsePackageContentFromJar(
                rootPackageNameDeliminatedByPeriod, jar, jarEntries.nextElement() );
        }
    }

    /**
     * @param codePath
     * @return String just the path to the first jar file
     */
    private String extractJarFilePath( final String codePath )
    {
        final String filePrefix = "file:"; 
        final StringTokenizer st;
        if ( codePath.contains( filePrefix ) )
        {
            st = new StringTokenizer( codePath.substring( filePrefix.length() ), "!" );
        }
        else
        {
            st = new StringTokenizer( codePath, "!" );
        }
        if ( 0 >= st.countTokens() )
        {
            return null;
        }
        return st.nextToken();
    }

    /**
     * @param rootPackageNameDeliminatedByPeriod
     * @param jar
     * @param jarEntry
     */
    private void parsePackageContentFromJar(
                    final String rootPackageNameDeliminatedByPeriod,
                    final JarFile jar,
                    final JarEntry jarEntry )
    {
        if ( jarEntry.isDirectory() )
        {
            return;
        }
        final boolean jarEntryisJarFile = jarEntry.getName().endsWith( JAR_SUFFIX );
        final File file = new File( jarEntry.getName() );
        if ( !jarEntryisJarFile )
        {
            String contentName;
            if ( isClass( jarEntry.getName() ) )
            {
                contentName =
                    jarEntry.getName().replace( ".class", "" ) 
                    .replace( '/', '.' );

                // jar file may contain classes from other packages
                if ( contentName.startsWith( rootPackageNameDeliminatedByPeriod ) )
                {
                    try
                    {
                        packageClassFound( Class.forName( contentName ) );
                    }
                    catch ( final ClassNotFoundException e )
                    {
                        throw new RuntimeException( new StringBuilder( 100 )
                            .append( "Failed while processing " ) 
                            .append( jar.getName() )
                            .append( " (" )
                            .append( jarEntry.getName() )
                            .append( ")." )
                            .toString(), e );
                    }
                }
            }
            if ( isResource( jarEntry.getName() ) )
            {
                contentName = jarEntry.getName().replace( '/', '.' );

                // jar file may contain classes from other packages
                if ( contentName.startsWith( rootPackageNameDeliminatedByPeriod ) )
                {
                    m_packageResources.add( contentName );
                }
            }
            return;
        }
        if ( !file.exists() && jarEntryisJarFile )
        {
            JarInputStream jis = null;
            try
            {
                jis = new JarInputStream( jar.getInputStream( jarEntry ) );
            }
            catch ( final IOException ioe )
            {
                LOG.error( new StringBuilder( 200 )
                    .append( this.getClass().getSimpleName() )
                    .append( " - Unable to create JarInputStream from jarEntry '" )  
                    .append( jarEntry )
                    .append( "' in jarFile '" ).append( jar ) 
                    .append( "'." ), ioe ); 
                return;
            }

            try
            {
                JarEntry nje = jis.getNextJarEntry();
                while ( null != nje )
                {
                    /*
                     * recurse:
                     */
                    parsePackageContentFromJar(
                        rootPackageNameDeliminatedByPeriod, jar, nje );
                    nje = jis.getNextJarEntry();
                }
            }
            catch ( final IOException ioe )
            {
                LOG.error( new StringBuilder( 200 )
                    .append( this.getClass().getSimpleName() )
                    .append( " - Unable to get jar entry from jar input stream '" )  
                    .append( jis )
                    .append( "'." ), ioe ); 
            }
            finally
            {
                try
                {
                    jis.close();
                }
                catch ( final IOException ioe )
                {
                    LOG.error( new StringBuilder( 200 )
                        .append( this.getClass().getSimpleName() )
                        .append( " - Unable to close jar input stream '" )  
                        .append( jis )
                        .append( "'." ), ioe ); 
                }
            }
            return;
        }
        LOG.error( new StringBuilder( 200 )
            .append( this.getClass().getSimpleName() )
            .append( " - Unable to make sense out of jar entry '" )   
            .append( jarEntry )
            .append( "', fileExists: " ).append( file.exists() )      
            .append( ", isJarFile: " ).append( jarEntryisJarFile ) ); 

    }


    /**
     * @param packageName
     * @param directoryPath
     */
    private void parsePackageContentFromDirectory(
                    final String packageName,
                    final String directoryPath )
    {
        final File root = new File( directoryPath );

        final File [] files = root.listFiles();
        String contentName;
        for ( int i = 0; i < files.length; ++ i )
        {
            if ( ! files[ i ].isDirectory() )
            {
                contentName = new StringBuilder( 300 )
                    .append( packageName )
                    .append( '.' )
                    .append( files[ i ].getName() )
                    .toString();
                if ( isClass( files[ i ].getName() ) )
                {
                    contentName =
                        contentName.substring( 0, contentName.length() - 6 );// removes .class endng

                    try
                    {
                        packageClassFound( Class.forName( contentName ) );

                    }
                    catch ( final ClassNotFoundException e )
                    {
                        throw new RuntimeException( contentName, e );
                    }
                }
                if ( isResource( files[ i ].getName() ) )
                {
                    m_packageResources.add( contentName );
                }
            }
        }

        for ( int i = 0; i < files.length; ++i )
        {
            if ( files[ i ].isDirectory() )
            {
                parsePackageContentFromDirectory(
                                new StringBuilder( 100 )
                                    .append( packageName )
                                    .append( '.' )
                                    .append( files[ i ].getName() )
                                    .toString(),
                                new StringBuilder( 100 )
                                    .append( directoryPath )
                                    .append( files[ i ].getName() )
                                    .append( '/' )
                                    .toString() );
            }
        }
    }


    /**
     * @param name
     * @return boolean
     */
    private boolean isClass( final String name )
    {
        return ( name.endsWith( ".class" ) ); 
    }


    /**
     * @param name
     * @return boolean
     */
    private boolean isResource( final String name )
    {
        return ! name.endsWith( ".class" ); 
    }
    

    private final Set< Class<?> > m_packageClasses;
    private final Set< String > m_packageResources;
    private static final String JAR_SUFFIX = ".jar"; 
    
    private final static Logger LOG = Logger.getLogger( PackageContentFinder.class );
}
