/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.lang;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.spectralogic.util.bean.lang.Optional;

/**
 * Only valid for bean properties that are annotated with {@link References}.  <br><br>
 * 
 * When a bean getter method is annotated with this, indicates that records of this type should receive
 * automatic handling defined via {@link WhenReferenceIsDeleted} when the referenced bean is deleted.  Without
 * this annotation, attempts to delete a referenced bean will result in a validation error to the client.
 */
@Documented
@Retention( RetentionPolicy.RUNTIME )
@Target( ElementType.METHOD )
public @interface CascadeDelete
{
    WhenReferenceIsDeleted value() default WhenReferenceIsDeleted.DEFAULT;
    
    
    public enum WhenReferenceIsDeleted
    {
        /**
         * May only be used on bean properties not marked as {@link Optional}, for which the bean will be 
         * deleted when the reference is deleted (the only possible behavior for non-optional bean 
         * properties).  <br><br>
         * 
         * If the bean property has annotation {@link Optional}, there are multiple possible behaviors that
         * the client could want when the reference is deleted and the client will be required to specify the
         * behavior desired.
         */
        DEFAULT( "CASCADE" ),
        
        /**
         * The referencing bean property value will be set to null when the reference is deleted.  This mode 
         * can only be selected when the bean property also has annotation {@link Optional}.
         */
        SET_NULL( "SET NULL" ),
        
        /**
         * The bean will be deleted when the reference is deleted.
         */
        DELETE_THIS_BEAN( "CASCADE" ),
        ;
        
        private WhenReferenceIsDeleted( final String sql )
        {
            m_sql = sql;
        }
        
        public String getSql()
        {
            return m_sql;
        }
        
        private final String m_sql;
    }
}
