package edu.ucsc.satuning.console;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a field which will represent a command-line option for
 * {@code OptionParser}.
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Option {
    /**
     *
     * @return the names for this option, such as {"-h", "--help"}.
     *      an option must have at least one name, which <strong>must
     *      </strong> start with one or two dashes (i.e., one or two '-'s).
     */
    String[] values();

    /**
     * @return {@code true} if the condition must have a concrete value (i.e., non null)
     */
    boolean required() default true;

    /**
     * these options are used by the tag system to mark options.
     * @return {@code true}, by default. user can override this option.
     */
    boolean savedInTag() default true;
}
