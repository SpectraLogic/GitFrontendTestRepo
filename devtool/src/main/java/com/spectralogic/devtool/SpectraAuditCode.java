/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.devtool;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public final class SpectraAuditCode
{  
    public static void main( String[] args ) // $codepro.audit.disable illegalMainMethod
    {
        new SpectraAuditCode().mainMethod();
    }
    
    
    public void mainMethod() 
    {            
        outLn( NL +  NL + NL + NL + "BEGIN\t\t" + ( new Date() ) + NL + NL );
        //first make sure the rules still work
        s_outLnEnabled = false;
        s_outLnDebugEnabled = false;
        final int allPass = this.getMatchesAgainstAllPassSampleFile();
        final int allFail = this.getMatchesAgainstAllFailSampleFile();
        final int howManyShouldFail = 29;
        s_outLnEnabled = true;
        
        final int nrMatches = this.getMatchesAndRunAllRegExOnAllWritableJavaFiles();

        out( NL +  NL + SEP_LINE + NL + NL + "END\t\t" + ( new Date() ) );
        outLn( "\t\t TOTAL ERRORS = " + ( nrMatches + allPass + allFail - howManyShouldFail ) + NL + NL );
        outLn( "Run against All Pass sample files hould be ZERO and is=" + allPass );
        outLn( "Run against All Fail Sample File =" + allFail + " instead of=" + howManyShouldFail );
        outLn( "Errors in the rest of the files =" + nrMatches + NL + NL + SEP_LINE );
        outFlush();
    }


    private int getMatchesAndRunAllRegExp( final String fileEnding, final boolean mustBeWritable )
    {
        int totalMatches = 0;
        if ( s_excludeStr == null )
        {
            try
            {
                s_excludeStr = FileUtils.readLines( new File ( 
                        "devtool/src/main/resources/" + this.getClass().getSimpleName() + ".excludes.txt" ), 
                        Charset.defaultCharset() );
            }
            catch ( final Exception ex ) 
            {
                throw new RuntimeException( ex );
            }
        }
        // https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html
        // https://regex101.com/r/hQ9xT1/35
        displaySystemInfoOnce();
        final List< String > fileNamelist = getFiles( USERDIR, fileEnding, mustBeWritable );
        outLn( "FILES we will check: " + NL + fileNamelist.toString().replaceAll( ",", NL ) );
        final Map< String, String > regExOrLineByLine = new TreeMap<>();
        final String aToZAndDigits = "[a-zA-Z\\d\"" + "'" + "]"; // regExclude
        final String openParans = "(" + "\\{|\\[|\\(|\\|" + ")"; // regExclude
        final String closedParans = "(" + "\\}|\\]|\\)|\\|)"; // regExclude
        int regExNr = 1;
        // we trim the spaces so reg exp dealing with pre-1st-non-white are not here
        regExOrLineByLine.put( openParans + aToZAndDigits, 
                "Open Paran followed by chars with no space:" + ( regExNr++ ) );
        regExOrLineByLine.put( "\"\\)" + "(?!.*\")", // regExclude 
                "Quotes, No space and Open Paran( not followed by quotes again ):" + ( regExNr++ ) );
        regExOrLineByLine.put( "[^\"]" + openParans + "\"", // regExclude
                "Quotes, No space and Open Paran( not followed by quotes again ):" + ( regExNr++ ) );
        regExOrLineByLine.put( aToZAndDigits + closedParans, 
                "chars followed by Closed Paran no space in between:" + ( regExNr++ ) );
        regExOrLineByLine.put( aToZAndDigits + "\\s{2,}", // regExclude 
                "1 space not 2 After Any Chars:" + ( regExNr++ ) );
        regExOrLineByLine.put( ",\\S+(?!" + REGEX_NEWLINE + ")", // regExclude
                "comma with NO space:" + ( regExNr++ ) );
        regExOrLineByLine.put( ";\\S" + "(?!" + "[^'|^" + REGEX_NEWLINE + "]" + ")", // regExclude
                "semicolon with No space after and other chars:" + ( regExNr++ ) ); 
        regExOrLineByLine.put( "\\s,", "space Before Comma:" + ( regExNr++ ) ); // regExclude
        regExOrLineByLine.put( "(for|if|while|catch)\\s{2,}\\(", // regExclude
                "For, If followed by two spaces:" + ( regExNr++ ) );
        regExOrLineByLine.put( "(for|if|while|catch)\\(", // regExclude
                "For/If/While/catch have no space before the open paran: " + ( regExNr++ ) );
        regExOrLineByLine.put( "(for|if|while|catch)\\s{2,}\\(", // regExclude
                "For/If/While/catch have more thank 1 space before the open paran: " + ( regExNr++ ) );
        regExOrLineByLine.put( "}\\s+" + "(?!" + "[^(inner)]" + ")", // regExclude
                "needed for the full file regex for 2 lines between methods:" + ( regExNr++ ) );
        regExOrLineByLine.put( "[a-zA-Z\\d]\\s{2,}[a-zA-Z\\d]", // regExclude 
                "2 pace between any characters:" + ( regExNr++) );
        regExOrLineByLine.put( openParans + "\\s{2,}\\S+", // regExclude
                "Open Paran followed by more than 1 single space and at least one nonspace:" 
                + ( regExNr++ ) );
        regExOrLineByLine.put( "\\S+\\s{2,4}" + openParans, // regExclude
                "Char, then not more than 1 space Before Open Paran:" + ( regExNr++ ) );
        regExOrLineByLine.put( "(bogie)+", "comments with the word bogie:" + ( regExNr++ ) ); // regExclude
        regExOrLineByLine.put( "=\\w", "Equal with No Space After:" + ( regExNr++ ) );
        regExOrLineByLine.put( "\\)+\\)+", "No space between close params1:" + ( regExNr++ ) ); // regExclude
        regExOrLineByLine.put( "\\(+\\(+", "No space between close params2:" + ( regExNr++ ) ); // regExclude
        regExOrLineByLine.put( "\\)+\\]+", "No space between close params3:" + ( regExNr++ ) ); // regExclude
        regExOrLineByLine.put( "\\[+\\(+", "No space between close params4:" + ( regExNr++ ) ); // regExclude
        regExOrLineByLine.put( "\\]+\\]+", "No space between close params5:" + ( regExNr++ ) ); // regExclude
        regExOrLineByLine.put( "\\[+\\[+", "No space between close params6:" + ( regExNr++ ) ); // regExclude
        regExOrLineByLine.put( "\\S+\\?\\S+" + "(?=\\:)", // regExclude 
                "?: with no space before and after ? :" + ( regExNr++ ) ); 
        regExOrLineByLine.put( "(?=\\?)" + "\\S+\\:\\S+", // regExclude 
                "?: with no space before and after '':'' :" + ( regExNr++ ) );
        regExOrLineByLine.put( "catch\\s{0,3}\\(\\s{0,3}\\w*Exception", // regExclude 
                "catch Exception with No final '':'' :" + ( regExNr++ ) ); 
                
        for ( final String fileName : fileNamelist ) 
        {
            totalMatches+= checkFileLineByLineEachRegex1By1( fileName, regExOrLineByLine, 
                    s_excludeStr );
        }

        final Map< String, String > regExOrFullFile = new HashMap<>();                
        final String common = "}(" + REGEX_NEWLINE + "){1,2}(.*)"; // regExclude
        final String comment = "none or 1 empty line between methods, not the smartest way";
        regExOrFullFile.put( common + "public", comment );
        regExOrFullFile.put( common +  "private", comment );
        regExOrFullFile.put( common +  "static", comment );
        regExOrFullFile.put( common + "protected", comment );
        regExOrFullFile.put( common + "@", comment );
        regExOrFullFile.put( common + "\\/\\*", comment );

        final String common4OrMore = "}(" + REGEX_NEWLINE + "){4,}(.*)"; // regExclude
        regExOrFullFile.put( common4OrMore + "public", comment );
        regExOrFullFile.put( common4OrMore +  "private", comment );
        regExOrFullFile.put( common4OrMore +  "static", comment );
        regExOrFullFile.put( common4OrMore + "protected", comment );
        regExOrFullFile.put( common4OrMore + "@", comment );
        regExOrFullFile.put( common4OrMore + "\\/\\*", comment );

        for ( final String fileName : fileNamelist ) 
        {
            totalMatches+= checkFileRegexAsOneStringOneRegexAtATime( fileName, regExOrFullFile );
        }
        outLn( "Done - TOTAL MATCHES:" + totalMatches );

        return totalMatches;
    }


    private int checkFileLineByLineEachRegex1By1( final String fileName, 
            final Map< String, String >  regExps, List< String > excludeStr )
    {
        int totalMatches = 0;
        int totalFileMatches = 0;
        final Set< String >  regExpsSet = regExps.keySet();
        final File f = new File( fileName );
        outLn( SEP_LINE + NL + "File name: " + f.getName() + "\t==>\t" + f.getAbsoluteFile().getParent() );
        final File file = new File( fileName );
        Scanner scanner = null;

        try 
        {    
            scanner = new Scanner( file );
            int lineNr = 0;
            String previousLine = "";
            while ( scanner.hasNextLine() )
            {
                boolean lineHasMatches = false;
                lineNr++;
                final String line = scanner.nextLine();
                final String anyCharNotSpaceNotAsterisk = "[^\\s+\\*]" ;
                final int nrSpaceBegin = getNrSpacesAndFirstAsteriskBeginingOfString ( line );
                final boolean alignedWithPreviousLine =
                        previousLine != null 
                        && nrSpaceBegin > 1
                        && previousLine.length() > nrSpaceBegin + 1
                        && previousLine.charAt( nrSpaceBegin - 1 ) == ' '
                        && previousLine.charAt( nrSpaceBegin ) != ' ';
                final boolean wrongIndent = 
                        ( nrSpaceBegin / 4 ) * 4 != nrSpaceBegin
                        || line.matches( "^\\s{1,3}" + anyCharNotSpaceNotAsterisk ) //regExclude
                        || line.matches( "^\\s{5,7}" + "[^\\s+\\*]" ) 
                        || line.matches( "^\\s{9,11}" + "[^\\s+\\*]" ) //regExclude
                        || line.matches( "^\\s{13,15}" + "[^\\s+\\*]" ) 
                        || line.matches( "^\\s{17,19}" + "[^\\s+\\*]" ) ; //regExclude
                if ( wrongIndent 
                        && !isAnyExcludeStringPartOfLine( line, excludeStr ) 
                        && !alignedWithPreviousLine ) 
                {
                    totalFileMatches+= 1;
                    lineHasMatches = true;
                    outLn( "Line " + lineNr + ": [ " + line.trim() +  " ]" 
                            + ", is neither indented properly with " + nrSpaceBegin + " spaces,"
                            + " nor aligned with the previous line, which has these chars ["
                            + ( ( previousLine.length() < nrSpaceBegin ) ? "null prev line"
                                : previousLine.substring( 
                                        Math.max( 0, nrSpaceBegin - 1 ),
                                        Math.min( previousLine.length(), nrSpaceBegin + 2 ) ) 
                                        + "]" ) );
                }
                previousLine = line;
                if ( line.length() > 110 ) 
                {
                    totalFileMatches += 1;
                    lineHasMatches = true;
                    outLn( "Line " + lineNr + ": [ " + line.trim() +  " ]" 
                            + ", is longer than 110chars, having" + line.length() );
                }
                final String lineLeftTrim = line.replaceAll( "^\\s+", "" ); //left .trim
                final String lineLeftTrimNoComments = delBetweenDoubleQuotes( lineLeftTrim ); 
                final int idxExcludeRegex = lineLeftTrim.toLowerCase().indexOf( "// regexclude" );
                final String excludeComment = "/*";
                if ( idxExcludeRegex == -1 
                        && lineLeftTrim.startsWith( excludeComment ) == FALSE
                        && lineLeftTrim.startsWith( "*" ) == FALSE )
                {
                    final Iterator< String > iter = regExpsSet.iterator();
                    final StringBuilder lineMultiRegexMatch = new StringBuilder();
                    int regExNr = 1;
                    while ( iter.hasNext() ) 
                    {
                        final Set<String> setMatchesOneRegex = new HashSet<>();
                        final StringBuilder lineOneRegexMatch = new StringBuilder();
                        final String regEx = iter.next();
                        Pattern pattern = Pattern.compile( regEx );
                        Matcher matcher = pattern.matcher( lineLeftTrimNoComments );                        
                        lineOneRegexMatch.append( "\tRegX#:" ).append( regExNr++ ).append( "; " );
                        while ( matcher.find() )
                        {
                            String match = matcher.group();
                            if ( !isMatchSameIndexAsExcludedItem( lineLeftTrimNoComments, match, 
                                    excludeStr ) )
                            {
                                setMatchesOneRegex.add( match );
                                lineOneRegexMatch.append( "Idx: " ).append( line.indexOf( match ) + 1 )
                                .append( "; " );
                            }
                        }
                        if ( !setMatchesOneRegex.isEmpty() )
                        {
                            totalFileMatches+= setMatchesOneRegex.size();
                            lineHasMatches = true;
                            lineOneRegexMatch.append( "Matches:" ).append( setMatchesOneRegex )
                            .append( "; RegExDescr: { " ).append( regExps.get( regEx ) ).append( " }; " )
                            .append( "; RegEx: { " ).append( regEx ).append( " }; " );

                            lineMultiRegexMatch.append( lineOneRegexMatch ).append( NL );
                        }
                        pattern = null;
                        matcher = null;
                    }
                    if ( lineHasMatches )
                    {
                        outLn( "Line " + lineNr + ": [ " + lineLeftTrim.trim() +  " ]"
                                + "; Modified Line:[" + lineLeftTrimNoComments + "]" );
                        outLn( lineMultiRegexMatch.toString() );
                    }
                }
            }

            outLn( "Matches for the file above:  " + totalFileMatches );
            totalMatches = totalMatches + totalFileMatches;
        }
        catch ( final FileNotFoundException ex )
        {
            outFlush();
            throw new RuntimeException( ex );
        }
        finally
        {            
            if ( scanner != null )
            {
                scanner.close();
            }
        }
        return totalMatches;
    }


    public void testGetNrSpacesAndFirstAsteriskBeginingOfString( )
    {
        if ( getNrSpacesAndFirstAsteriskBeginingOfString( " *" ) != 0 )
        {
            throw new RuntimeException( "Should count the * and return 0" );
        }
    }
    
    
    static private int getNrSpacesAndFirstAsteriskBeginingOfString( final String str )
    {
        int count = 0;
        int i = 0;
        for ( ; i < str.length(); i++ )
        {
            if ( str.charAt( i ) == ' ' )
            {
                count++;
            }
            else 
            {
                final int tmpDebug = ( ( str.charAt( i ) == '*' ) ? -1 : 0 ) + count;
                return tmpDebug;
            }
        }
        return count; 
    }
        
    
    public void testRemoveStuffBetwenDoubleQUotes()
    {
        testDel( "xxx+ \"bbb d\" dddd", "xxx+ \"$$$$$\" dddd" ) ;
        testDel( "dd? ee: ff", "dd? ee: f" ) ;
    }


    public void testDel( String strToDeleteBetweenQuotes, String expectAfterDelete )
    {
        final String retStrToDelete = delBetweenDoubleQuotes( strToDeleteBetweenQuotes );
        if ( !retStrToDelete.equals( expectAfterDelete ) )
        {
            throw new RuntimeException( NL + "String to delete=" + strToDeleteBetweenQuotes
                    + NL + "Returned=" + retStrToDelete
                    + NL + "But it was supposed to be=" + expectAfterDelete );
        }
    }


    private String delBetweenDoubleQuotes( String line )
    {
        final char[] lineCharA = line.toCharArray();
        boolean inside = false;
        int countPairs = 0;
        for ( int i = 0; i < lineCharA.length; i++ )
        {
            if ( lineCharA[ i ] == '"' )
            {
                inside = !inside;
                countPairs ++;
            }
            else if ( inside )
            {
                lineCharA[ i ] ='$';
            }
        }
        if ( countPairs % 2 == 0 )
        {
            return new String( lineCharA );
        }

        return line;
    }
    
    
    private boolean isMatchSameIndexAsExcludedItem( String line, String match, List< String > excludeStr )
    {
        final int idxMatch = line.indexOf( match );
        for ( String exclude:excludeStr )
        {
            final int idxExclude = line.indexOf( exclude );
            if ( idxExclude != -1 )
            {
                if ( idxMatch == idxExclude || idxMatch + match.length() == idxExclude + exclude.length()  )
                {
                    outLnDebug( "***Because of[" + exclude + "] we excluded [" + line.trim() + "]" );
                    return true;
                }
            }
        }
        return false;
    }

    
    private boolean isAnyExcludeStringPartOfLine( String line, List< String > excludeStr )
    {
        for ( String exclude:excludeStr )
        {
            final int idxExclude = line.indexOf( exclude );
            if ( idxExclude != -1 )
            {
                return true;
            }
        }
        return false;
    }
    
    
    private int checkFileRegexAsOneStringOneRegexAtATime( final String fileName, 
            final Map< String, String >  regExpsWithComments )    
    {
        int totalMatches = 0;
        outLn( SEP_LINE + NL + "FILE AS 1 STRING :: File name: " + new File( fileName ).getName() );
        final String allFile;
        try
        {
            allFile = IOUtils.toString( new FileInputStream( fileName ) );       
        }
        catch ( final Exception e )
        {
            outFlush();
            throw new RuntimeException( e );
        }

        final Iterator< String > iter = regExpsWithComments.keySet().iterator();
        while ( iter.hasNext() ) 
        {
            final String regExp = iter.next();
            Pattern pattern = Pattern.compile( regExp );
            Matcher matcher = pattern.matcher( allFile );
            final int len = allFile.length();
            final List<String> listMatches = new ArrayList<>();
            final List<String> startEnds = new ArrayList<>();
            while ( matcher.find() )
            {
                String match = matcher.group();
                listMatches.add( match );
                String matchedPart = allFile.substring( matcher.start(), matcher.end() ); 
                String tmp =  allFile.substring( Math.max( matcher.start() - 20, 0 ), matcher.start() )
                        + "<<" + matchedPart + ">>" 
                        + allFile.substring( matcher.end(), Math.min( len, matcher.end() + 50 ) );
                tmp = maskEndOfLine( tmp );
                startEnds.add( "\tMatchedPart " 
                        + NL + "\t\t" + matchedPart 
                        + NL + "\tInsideString " 
                        + NL + "\t\t" + tmp );
            }
            if ( !listMatches.isEmpty() )
            {
                outLn( "REGEX WE SEARCHED FOR BEGIN <<" + maskEndOfLine( regExp ) + ">> REGEX END" + NL 
                        + "NrMatches:" +  listMatches.size() );
                int count = 1;
                for ( String startEnd : startEnds )
                {
                    outLn( ( count++ ) + "-MATCH BEGIN->" + NL + startEnd + NL + "<- MATCH END" + NL );
                }
                totalMatches+= listMatches.size();
            }
            pattern = null;
            matcher = null;
        }
        return totalMatches;
    }


    private String maskEndOfLine( final String in )
    {
        return in.replaceAll( REGEX_CR, "\\\\r" )
                .replaceAll( REGEX_NEWLINE, "\\\\n" )
                .replaceAll( "\\" + "\\" + "u000A", "\\\\n " );
    }


    private List< String > getFiles( final String directoryName, final String endsWith, 
            final boolean mustBeWritable ) 
            {
        final List< String > retFileNameList = new ArrayList<>();
        final File directory = new File( directoryName );
        final File[] fList = directory.listFiles();

        for ( final File file : fList ) 
        {
            final boolean isInBuildDir = file.getPath().contains( "\\build\\" ) 
                    || file.getPath().contains( "/build/" );
            if ( isInBuildDir )
            {
                continue;
            }
            if ( file.isFile() ) 
            {
                final boolean ignoreSampleFile = file.getName().matches( 
                        this.getClass().getSimpleName() + ".+" + "txt" ) && mustBeWritable;
                final boolean fileEndsWith = file.getName().endsWith( endsWith );
                final boolean selectFile = ( mustBeWritable )? file.canWrite() 
                        && fileEndsWith:fileEndsWith;                
                if ( selectFile && !ignoreSampleFile )
                {
                    retFileNameList.add( file.getAbsolutePath() );
                }
            }
            else if ( file.isDirectory() )
            {
                retFileNameList.addAll( getFiles( file.getAbsolutePath(), endsWith, mustBeWritable ) );
            }
        }

        return retFileNameList;
            }


    static private void displaySystemInfoOnce()
    {
        if ( s_displayedSysInfoOnce )
        {
            return;
        }

        try
        {
            final InetAddress addr = InetAddress.getLocalHost();
            s_hostInetName = addr.getHostName();
        }
        catch ( final UnknownHostException ex )
        {
            throw new RuntimeException( ex );
        }

        s_displayedSysInfoOnce = true;
        outLn( SEP_LINE );
        outLn( "Host Inet name: " + s_hostInetName + "; Computer name: " + s_computerName );
        outLn( "OS: " + OSNAME + CSV_SEP + "Arch: " + OSARCH + CSV_SEP + " OsVer: " + OSVER );
        outLn( "JAVA ver: " + JAVAVERSION + CSV_SEP + "Runtime ver: " + JAVARUNTIMEVERSION + CSV_SEP 
                + "VM name: " + JAVAVMNAME + CSV_SEP + "Class ver: " + JAVACLASSVERSION );
        outLn( SEP_LINE );
    }


    private int getMatchesAndRunAllRegExOnAllWritableJavaFiles( )
    {
        return getMatchesAndRunAllRegExp( ".java", true );
    }


    private int getMatchesAgainstAllPassSampleFile( )
    {
        return getMatchesAndRunAllRegExp( this.getClass().getSimpleName() + ".pass.txt", false );
    }


    private int getMatchesAgainstAllFailSampleFile( )
    {
        return getMatchesAndRunAllRegExp( this.getClass().getSimpleName() + ".fail.txt", false );
    }


    static private void outLnDebug( final String str )
    {
            outLnDebug( str, s_outLnDebugEnabled );
    }
    
    static private void outLn( final String str )
    {
            outLn( str, s_outLnEnabled );
    }

    
    static public void outLnDebug( final String str, boolean enabled )
    {
        outLn( str, enabled );
    }
    
    
    static public void outLn( final String str, boolean enabled )
    {
        if ( enabled )
        {
            out( ( new StringBuilder().append( str ).append( NL ) ).toString() );
        }
    }


    static public void out( final String str )
    {        
        s_bufLogOut.append( str );
        s_pauseOutputLineCounter+= StringUtils.countMatches( str, NL );
        
        try
        {
            if ( s_pauseOutputLineCounter > PAUSE_OUTPUT_EVERY_LINES )
            {
                S_LOG.info( s_bufLogOut );
                System.in.read();
                s_pauseOutputLineCounter = 0;
                s_bufLogOut = new StringBuilder( NL );
            }
        }
        catch ( final IOException ex )
        {
            outFlush();
            throw new RuntimeException( ex );
        }
    }

    
    static public void outFlush()
    {
        S_LOG.info( s_bufLogOut );
    }
    
    
    private static final String SEP_LINE = "==========================================================";
    private static final char CSV_SEP = ';';
    private static final String REGEX_NEWLINE = "\\u000A"; //"\n"
    private static final String REGEX_CR = "\\u000D";
    private static final String NL = System.getProperty( "line.separator" );    
    private static final String USERDIR = System.getProperty( "user.dir" ).toLowerCase();
    private static final String OSNAME = System.getProperty( "os.name" );
    private static final String OSARCH = System.getProperty( "os.arch" );
    private static final String OSVER = System.getProperty( "os.version" );
    private static final String JAVAVERSION = System.getProperty( "java.version" );
    private static final String JAVARUNTIMEVERSION = System.getProperty( "java.runtime.version" );
    private static final String JAVAVMNAME = System.getProperty( "java.vm.name" );
    private static final String JAVACLASSVERSION = System.getProperty( "java.class.version" ); 
    private static final boolean FALSE = false;
    private static final Logger S_LOG = Logger.getLogger( SpectraAuditCode.class );
    private static final int PAUSE_OUTPUT_EVERY_LINES = Integer.MAX_VALUE;
    private static List< String > s_excludeStr;
    
    private static boolean s_displayedSysInfoOnce = false;
    private static String s_computerName = "Unknown";
    private static String s_hostInetName = "Unknown";    
    private static boolean s_outLnEnabled = true;
    private static boolean s_outLnDebugEnabled = false;    
    private static int s_pauseOutputLineCounter = 0;
    private static StringBuilder s_bufLogOut = new StringBuilder();
    static 
    {         
        final Map<String, String> env = System.getenv();
        if ( env.containsKey( "COMPUTERNAME" ) )
        {
            s_computerName = env.get( "COMPUTERNAME" );
        }
        else if ( env.containsKey( "HOSTNAME" ) )
        {
            s_computerName = env.get( "HOSTNAME" );
        }
    }
}