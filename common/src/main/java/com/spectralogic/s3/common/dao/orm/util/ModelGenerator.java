/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.orm.util;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import org.reflections.Reflections;


public class ModelGenerator
{
    //Run to regenerate ORM layer - make sure write permissions on ORM files in com.spectralogic.s3.common.dao.orm are set properly
    public static void main( final String[] args )
    {
        final Reflections reflections = new Reflections( "com.spectralogic.s3.common.dao.domain" );
        final Set<Class<? extends DatabasePersistable>> list = reflections.getSubTypesOf( DatabasePersistable.class );
        for (final Class<? extends DatabasePersistable> c : list)
        {
            if ( c.isInterface() )
            {
                PROPERTY_INFO.put( c, new HashSet< PropertyInfo >() );
            }
        }
        
        for ( final Class< ? extends DatabasePersistable > c : PROPERTY_INFO.keySet() )
        {
            for (final Field field : c.getFields())
            {
                try
                {
                    final String property = field.get(null).toString();
                    final Method writer = BeanUtils.getWriter( c, property);
                    final Method reader = BeanUtils.getReader( c, property);
                    if (null != writer && null != reader)
                    {
                        if ( reader.getAnnotation( References.class ) != null )
                        {
                            final Class< ? extends DatabasePersistable > referencedBeanType =
                                    reader.getAnnotation( References.class ).value();
                            final PropertyInfo info = new PropertyInfo(
                                    field.getName(),
                                    reader.getName().substring( 0, reader.getName().length() - 2 ),
                                    referencedBeanType,
                                    field.getDeclaringClass(),
                                    false );
                            PROPERTY_INFO.get( c ).add( info );
                            
                            final PropertyInfo foreignInfo = new PropertyInfo(
                                    field.getName(),
                                    c.getSimpleName().endsWith("y") ?
                                            "get" + c.getSimpleName().substring(0, c.getSimpleName().length() - 1) + "ies" :
                                            "get" + c.getSimpleName() + "s",
                                    c,
                                    field.getDeclaringClass(),
                                    true );
                            PROPERTY_INFO.get( referencedBeanType ).add( foreignInfo );
                        }
                        else
                        {
                            final PropertyInfo info = new PropertyInfo(
                                    field.getName(),
                                    reader.getName(),
                                    reader.getReturnType(),
                                    field.getDeclaringClass(),
                                    false );
                            PROPERTY_INFO.get( c ).add( info );
                        }
                    }
                }
                catch ( final IllegalArgumentException | IllegalAccessException ex )
                {
                    throw new RuntimeException( ex );
                }
            }
        }
        
        final Map< String, String > filesToCreate = new HashMap<>();
        
        for ( final Class< ? extends DatabasePersistable > c : PROPERTY_INFO.keySet() )
        {
             filesToCreate.put( c.getSimpleName() + "RM.java", getRMClassAsString( c ) ) ;    
        }
        
        for ( final String s : filesToCreate.keySet() )
        {
            final List<String> lines = Arrays.asList(filesToCreate.get( s ) );
            Path file = Paths.get("./common/src/main/java/com/spectralogic/s3/common/dao/orm/" + s);
            try
            {
                file = Files.write( file, lines, Charset.forName( "UTF-8" ) );
            }
            catch ( final IOException ex1 )
            {
                throw new RuntimeException( ex1 );
            }
        }
    }
    
    
    private static String getRMClassAsString( final Class< ? extends DatabasePersistable > c )
    {
        String ret =
            "/*******************************************************************************\n"
                + " *\n"
                + " * Copyright C 2021, Spectra Logic Corporation and/or its affiliates.\n"
                + " * All rights reserved.\n"
                + " *\n"
                + " ******************************************************************************/\n"
                + "\n"
                + "/*******************************************************************************\n"
                + " *                    AUTO-GENERATED FILE - DO NOT MODIFY\n"
                + " * This file is a relational model file. It is not generated by the normal build\n"
                + " * process. In order to update this file to match the current DAO layer, please\n"
                + " * run the main method of com.spectralogic.s3.common.dao.orm.util.ModelGenerator\n"
                + " * directly from your IDE. This will regenerate ALL RM objects.\n"
                + " ******************************************************************************/\n";
        ret += "package com.spectralogic.s3.common.dao.orm;\n\n";
        ret += writeImports( c );
        ret += "\n\npublic class ";
        ret += c.getSimpleName() + "RM\n{\n";
        ret += writeConstructors( c );
        for ( final PropertyInfo p : PROPERTY_INFO.get( c ).stream().sorted(new PropertyInfoComparator()).collect(Collectors.toList()) )
        {
            if ( p.isPassThrough() )
            {
                ret += writePassthroughMethod( p );    
            }
            else if ( p.isForeign() )
            {
                ret += writeListMethod( c, p );
            }
            else
            {
                ret += writeBeanReturningMethod( c, p );    
            }
        }
        ret += writeCommonMethods( c );
        ret += writePrivateData( c );
        ret += "}";
        return ret;
    }

    
    private static String writeImports( final Class< ? extends DatabasePersistable > c )
    {
        String ret = "";
        final Set< String > imports = new HashSet<>(); 
        imports.add( c.getName() );
        imports.add( "com.spectralogic.util.bean.lang.Identifiable" );
        imports.add( "com.spectralogic.util.db.query.Require" );
        imports.add( "com.spectralogic.util.db.query.WhereClause" );
        imports.add( "java.util.UUID" );
        imports.add( "com.spectralogic.util.db.service.api.BeansServiceManager" );
        for ( final PropertyInfo p : PROPERTY_INFO.get( c ))
        {
            if ( ( p.isPassThrough() || p.isForeign() )
                    && !p.getPropertyType().isPrimitive()
                    && !p.getPropertyType().getPackage().getName().equals( "java.lang" ) )
            {
                imports.add( p.getPropertyType().getName() );
                if ( p.isForeign() )
                {
                    imports.add( "com.spectralogic.util.db.service.api.RetrieveBeansResult" );
                }
            }
            if ( !p.isPassThrough() )
            {
                imports.add( p.getDeclaringClass().getName() );
            }
        }
        for ( final String i : imports.stream().sorted().collect(Collectors.toList()) )
        {
            ret += "import " + i + ";\n";
        }
        return ret;
    }

    
    private static String writePassthroughMethod( final PropertyInfo p )
    {
        String ret = "";
        ret += "    synchronized public "  + p.getPropertyType().getSimpleName() + " " +
                p.getGetterName() + "()\n";
        ret += "    {\n";
        ret += "        fetchBeanIfNull();\n";
        ret += "        return  m_bean." + p.getGetterName() + "();\n";
        ret += "    }\n\n\n";
        return ret;
    }
    
    
    private static String writeListMethod(
            final Class< ? extends DatabasePersistable > c,
            final PropertyInfo p )
    {
        String ret = "";
        ret += "   synchronized public RetrieveBeansResult< "
                + p.getPropertyType().getSimpleName() + " > "  + p.getGetterName() + "()\n";
        ret += "    {\n";
        ret += "        fetchBeanIfNull();\n";
        ret += "        return m_serviceManager.getRetriever( ";
        ret += p.getPropertyType().getSimpleName() + ".class ).retrieveAll(\n";
        ret += "            Require.beanPropertyEquals( " + p.getDeclaringClass().getSimpleName() + "."
                + p.getAllCapsName() + "," +
                " m_bean.getId() ) );\n";
        ret += "    }\n\n\n";
        return ret;
    }
    
    
    
    private static String writeBeanReturningMethod(
            final Class< ? extends DatabasePersistable > c,
            final PropertyInfo p )
    {
        String ret = "";
        ret += "    synchronized public " + p.getPropertyType().getSimpleName() + "RM "  + p.getGetterName() + "()\n";
        ret += "    {\n";
        ret += "        if ( null != m_bean )\n";
        ret += "        {\n";
        ret += "            return new " + p.getPropertyType().getSimpleName() + "RM( m_bean.";
        ret += p.getGetterName() + "Id(), m_serviceManager );\n";
        ret += "        }\n";
        ret += "        return new " + p.getPropertyType().getSimpleName() + "RM(\n";
        ret += "                Require.exists( " + c.getSimpleName() + ".class,\n";
        ret += "                        " + p.getDeclaringClass().getSimpleName() + "." + p.getAllCapsName() + ",\n";
        ret += "                        m_whereClause ),\n";
        ret += "                m_serviceManager );\n";
        ret += "    }\n\n\n";
        return ret;        
    }
    
    
    private static String writeConstructors( final Class< ? extends DatabasePersistable > c )
    {
        String ret = "    public " + c.getSimpleName() +
                "RM( " + c.getSimpleName() + " bean, final BeansServiceManager serviceManager )\n";
        ret += "    {\n";
        ret += "        m_bean = bean;\n";
        ret += "        m_whereClause = null;\n";
        ret += "        m_serviceManager = serviceManager;\n";
        ret += "    }\n\n\n";    
        ret += "    public " + c.getSimpleName() +
                "RM( final UUID requiredId, final BeansServiceManager serviceManager )\n";
        ret += "    {\n";
        ret += "        this( Require.beanPropertyEquals( Identifiable.ID, requiredId ), serviceManager );\n";
        ret += "    }\n\n\n";
        ret += "    " + c.getSimpleName() +
                "RM(final WhereClause whereClause, final BeansServiceManager serviceManager)\n";
        ret += "    {\n";
        ret += "        m_bean = null;\n";
        ret += "        m_whereClause = whereClause;\n";
        ret += "        m_serviceManager = serviceManager;\n";
        ret += "    }\n\n\n";
        return ret;
    }
    
    private static String writeCommonMethods( final Class< ? extends DatabasePersistable > c )
    {
        String ret = "    private void fetchBeanIfNull()\n";
        ret += "    {\n";
        ret += "        if ( null == m_bean )\n";
        ret += "        {\n";
        ret += "            m_bean = m_serviceManager.getRetriever( " + c.getSimpleName()
                + ".class ).attain( m_whereClause );\n";
        ret += "        }\n";
        ret += "    }\n\n\n";
        ret += "    synchronized public " + c.getSimpleName() + " unwrap()\n";
        ret += "    {\n";
        ret += "        fetchBeanIfNull();\n";
        ret += "        return m_bean;\n";
        ret += "    }\n\n";
        return ret;
    }
    
    
    private static String writePrivateData( final Class< ? extends DatabasePersistable > c )
    {
        String ret = "    //m_whereClause is ignored if m_bean != null\n"; 
        ret += "    private final WhereClause m_whereClause;\n";
        ret += "    private final BeansServiceManager m_serviceManager;\n";
        ret += "    private " + c.getSimpleName() + " m_bean;\n";
        return ret;
    }
    
    
    private static class PropertyInfo
    {
        PropertyInfo(
                final String allCapsName,
                final String getterName,
                final Class< ? > propertyType,
                final Class< ? > declaringClass,
                final boolean foreign)
        {
            m_allCapsName = allCapsName;
            m_getterName = getterName;
            m_propertyType = propertyType;
            m_declaringClass = declaringClass;
            m_foreign = foreign;
        }
        
        
        public String getAllCapsName()
        {
            return m_allCapsName;
        }
        
        
        public String getGetterName()
        {
            return m_getterName;
        }
        
        
        public Class< ? > getPropertyType()
        {
            return m_propertyType;
        }
        
        
        public Class< ? > getDeclaringClass()
        {
            return m_declaringClass;
        }
        
        
        public boolean isForeign()
        {
            return m_foreign;
        }
        
        
        public boolean isPassThrough()
        {
            return !DatabasePersistable.class.isAssignableFrom( m_propertyType );
        }
        
        
        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ( ( m_getterName == null ) ? 0 : m_getterName.hashCode() );
            return result;
        }


        @Override
        public boolean equals( final Object obj )
        {
            if ( this == obj )
            {
                return true;
            }
            if ( obj == null )
            {
                return false;
            }
            if ( ! ( obj instanceof PropertyInfo ) )
            {
                return false;
            }
            final PropertyInfo other = (PropertyInfo)obj;
            if ( m_getterName == null )
            {
                if ( other.m_getterName != null )
                {
                    return false;
                }
            }
            else if ( !m_getterName.equals( other.m_getterName ) )
            {
                return false;
            }
            return true;
        }
        

        private final String m_allCapsName;
        private final String m_getterName;
        private final Class < ? > m_propertyType;
        private final Class < ? > m_declaringClass;
        private final boolean m_foreign;
    }


    private static class PropertyInfoComparator implements Comparator<PropertyInfo> {
        @Override
        public int compare(final PropertyInfo o1, final PropertyInfo o2) {
            return o1.getAllCapsName().compareTo(o2.getAllCapsName());
        }
    }

    
    private static final Map< Class< ? extends DatabasePersistable >, Set< PropertyInfo > > PROPERTY_INFO =
            new HashMap<>();
}